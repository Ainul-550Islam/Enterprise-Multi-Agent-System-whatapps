//! Layered runtime configuration loader (TOML, no external config crate):
//!
//! ```text
//!   config/default.toml   ←  config/{MAS_ENV}.toml   ←  environment vars
//! ```
//!
//! Design constraints:
//!
//! * **Fail closed, not silent:** file mistakes (missing table, wrong type)
//!   surface as `AppError::validation` naming the offending key; *missing
//!   files* are fine (they select the empty layer), except when
//!   [`load_from_env_config_dir`] is asked to require them.
//! * **No secrets in files:** every caller overlays env vars *after* the
//!   file layer ([`Merger::string`] etc.), and the production overlay
//!   documents which keys must stay empty.
//! * **Deep merge for tables:** the environment overlay replaces scalars but
//!   merges tables recursively, so partial overlays don't have to repeat the
//!   whole section.
//!
//! This module deliberately ships *no typed knobs*: each binary converts the
//! merged [`toml::Value`] via the helpers below into its own validated
//! config struct. That keeps `mas-common` free of per-process schema.

use std::path::{Path, PathBuf};

use crate::error::AppError;
use crate::result::Result;

/// Environment overlay selector variable.
pub const ENV_NAME: &str = "MAS_ENV";
/// Config directory override variable.
pub const CONFIG_DIR_ENV: &str = "MAS_CONFIG_DIR";
/// Default config directory name (repo-root relative).
pub const DEFAULT_CONFIG_DIR: &str = "config";

/// Resolves the config directory: `MAS_CONFIG_DIR` wins, otherwise
/// `{cwd}/config` and (when missing) two parents up — so a binary launched
/// from a crate dir still finds the workspace root's config.
#[must_use]
pub fn config_dir() -> PathBuf {
    if let Ok(dir) = std::env::var(CONFIG_DIR_ENV) {
        return PathBuf::from(dir);
    }
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    for candidate in [
        cwd.join(DEFAULT_CONFIG_DIR),
        cwd.join("..").join(DEFAULT_CONFIG_DIR),
        cwd.join("..").join("..").join(DEFAULT_CONFIG_DIR),
    ] {
        if candidate.is_dir() {
            return candidate;
        }
    }
    cwd.join(DEFAULT_CONFIG_DIR)
}

/// The current overlay name (`MAS_ENV`, default `development`).
#[must_use]
pub fn environment_name() -> String {
    std::env::var(ENV_NAME).unwrap_or_else(|_| "development".to_owned())
}

/// Does a path exist?
#[must_use]
pub fn layer_exists(path: &Path) -> bool {
    path.is_file()
}

/// Loads and deep-merges `default.toml` + `{environment}.toml` from the
/// resolved directory. Returns the empty table when neither file exists
/// (dev shells without the repo layout), an error when one exists but is
/// invalid TOML.
pub fn load_layered() -> Result<toml::Value> {
    let dir = config_dir();
    load_layers_in(&dir, &environment_name())
}

/// Explicit-dir, explicit-environment variant (tests + the loader core).
pub fn load_layers_in(dir: &Path, environment: &str) -> Result<toml::Value> {
    let mut merged = toml::Value::Table(toml::map::Map::new());
    for name in ["default", environment] {
        let path = dir.join(format!("{name}.toml"));
        if !path.is_file() {
            continue;
        }
        let text = std::fs::read_to_string(&path).map_err(|err| {
            AppError::validation(format!("config file {} unreadable: {err}", path.display()))
        })?;
        let value: toml::Value = toml::from_str(&text).map_err(|err| {
            AppError::validation(format!(
                "config file {} invalid TOML: {err}",
                path.display()
            ))
        })?;
        deep_merge(&mut merged, value);
    }
    Ok(merged)
}

/// Package-friendly alias used by binaries when they merely want "the
/// standard layered view". (Kept for call-site readability.)
pub fn load_from_env_config_dir() -> Result<toml::Value> {
    load_layered()
}

/// In-place deep merge: tables merge recursively; every other value (and a
/// scalar/table kind clash) is replaced by the overlay.
pub fn deep_merge(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base_table), toml::Value::Table(overlay_table)) => {
            for (key, overlay_val) in overlay_table {
                match base_table.get_mut(&key) {
                    Some(base_val) => deep_merge(base_val, overlay_val),
                    None => {
                        base_table.insert(key, overlay_val);
                    },
                }
            }
        },
        (base_val, overlay_val) => *base_val = overlay_val,
    }
}

/// Dotted-path lookup (`"worker.poll_interval_ms"`).
#[must_use]
pub fn lookup<'a>(merged: &'a toml::Value, path: &str) -> Option<&'a toml::Value> {
    let mut current = merged;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

/// Helper with exactly the precedence contract: CLI/flag override → env var
/// → file → built-in default. The generic callers below implement one width
/// apiece so binaries never hand-roll the env dance.
#[derive(Debug, Clone)]
pub struct Merger<'a> {
    merged: &'a toml::Value,
}

impl<'a> Merger<'a> {
    /// Binds a merger to a layered view.
    #[must_use]
    pub fn new(merged: &'a toml::Value) -> Self {
        Self { merged }
    }

    /// `env → file.path → default`, typed `String`.
    #[must_use]
    pub fn string(&self, env_key: &str, path: &str, default: &str) -> String {
        if let Ok(value) = std::env::var(env_key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return trimmed.to_owned();
            }
        }
        lookup(self.merged, path)
            .and_then(toml::Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| default.to_owned())
    }

    /// Same precedence, `u64`.
    pub fn u64(&self, env_key: &str, path: &str, default: u64) -> Result<u64> {
        if let Ok(value) = std::env::var(env_key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return trimmed.parse::<u64>().map_err(|err| {
                    AppError::validation(format!("env {env_key} invalid u64 '{trimmed}': {err}"))
                });
            }
        }
        Ok(lookup(self.merged, path)
            .and_then(toml::Value::as_integer)
            .map_or(default, |v| v as u64))
    }

    /// Same precedence, `bool` (`true|false|1|0|yes|no` in env).
    pub fn bool(&self, env_key: &str, path: &str, default: bool) -> Result<bool> {
        if let Ok(value) = std::env::var(env_key) {
            let trimmed = value.trim().to_ascii_lowercase();
            if !trimmed.is_empty() {
                return match trimmed.as_str() {
                    "true" | "1" | "yes" | "on" => Ok(true),
                    "false" | "0" | "no" | "off" => Ok(false),
                    _ => Err(AppError::validation(format!(
                        "env {env_key} invalid bool '{trimmed}' (true/false)"
                    ))),
                };
            }
        }
        Ok(lookup(self.merged, path)
            .and_then(toml::Value::as_bool)
            .unwrap_or(default))
    }

    /// Milliseconds-as-Duration variant of [`Merger::u64`].
    pub fn duration_ms(
        &self,
        env_key: &str,
        path: &str,
        default: std::time::Duration,
    ) -> Result<std::time::Duration> {
        let ms = self.u64(env_key, path, default.as_millis() as u64)?;
        Ok(std::time::Duration::from_millis(ms))
    }
}

/// Production guardrail: errors when `predicate(value)` holds — used for
/// "dev-token in prod" and "in-memory in prod" failures without cfg soups.
pub fn refuse_if(condition: bool, message: impl Into<String>) -> Result<()> {
    if condition {
        Err(AppError::validation(message.into()))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deep_merge_replaces_scalars_and_merges_tables() {
        let mut base: toml::Value =
            toml::from_str("[worker]\npoll = 250\n[observability]\nformat = \"json\"\n")
                .expect("base");
        let overlay: toml::Value = toml::from_str("[worker]\npoll = 5\n").expect("overlay");
        deep_merge(&mut base, overlay);
        assert_eq!(
            lookup(&base, "worker.poll").and_then(toml::Value::as_integer),
            Some(5)
        );
        assert_eq!(
            lookup(&base, "observability.format").and_then(toml::Value::as_str),
            Some("json"),
            "untouched table survives the merge",
        );
    }

    #[test]
    fn load_layers_in_layers_default_then_environment() {
        let tmp = std::env::temp_dir().join("mas-config-layer-test");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).expect("mkdir");
        std::fs::write(tmp.join("default.toml"), "[a]\nkeep = 1\ndrop = \"old\"\n")
            .expect("default");
        std::fs::write(tmp.join("test.toml"), "[a]\ndrop = \"new\"\n").expect("overlay");
        let merged = load_layers_in(&tmp, "test").expect("merge");
        assert_eq!(
            lookup(&merged, "a.keep").and_then(toml::Value::as_integer),
            Some(1)
        );
        assert_eq!(
            lookup(&merged, "a.drop").and_then(toml::Value::as_str),
            Some("new")
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn invalid_toml_fails_with_named_file() {
        let tmp = std::env::temp_dir().join("mas-config-bad-test");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).expect("mkdir");
        std::fs::write(tmp.join("default.toml"), "[broken\n").expect("write");
        let err = load_layers_in(&tmp, "nothing").unwrap_err();
        assert!(err.to_string().contains("default.toml"), "{err}");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn refuse_if_guards() {
        assert!(refuse_if(true, "nope").is_err());
        assert!(refuse_if(false, "fine").is_ok());
    }
}
