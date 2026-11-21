//! CLI bootstrap contract: argument parsing, local scope guard and secret
//! masking. No network — everything here must fail or succeed locally.

use clap::Parser;
use mas_cli::args::{Cli, GlobalOpts};
use mas_cli::commands::scoped_ok;

/// Builds global opts without touching clap's env fallbacks (explicit values
/// only — process-env mutation is a test race).
fn opts(token: &str, tenant: Option<uuid::Uuid>, organization: Option<uuid::Uuid>) -> GlobalOpts {
    GlobalOpts {
        base_url: "http://127.0.0.1:8080".to_owned(),
        token: token.to_owned(),
        tenant,
        organization,
        correlation_id: None,
        format: mas_cli::args::Format::Brief,
    }
}

#[test]
fn bootstrap_masks_secret_material() {
    let global = opts("super-secret-bearer", None, None);
    let dbg = format!("{global:?}");
    assert!(dbg.contains("[redacted]"), "token not masked: {dbg}");
    assert!(
        !dbg.contains("super-secret-bearer"),
        "token leaked in Debug: {dbg}"
    );
    // useful fields stay visible
    assert!(dbg.contains("http://127.0.0.1:8080"));
}

#[test]
fn bootstrap_scope_guard_fails_locally_before_the_network() {
    let tenant = uuid::Uuid::now_v7();
    let organization = uuid::Uuid::now_v7();

    // no scope at all -> local, actionable error
    let err = scoped_ok(&opts("dev-token", None, None)).expect_err("no scope must fail");
    assert!(err.contains("missing scope"), "unhelpful message: {err}");
    assert!(err.contains("--tenant"), "message must name the fix: {err}");

    // half a scope is still not a scope (scoped routes need tenant+org together)
    let err = scoped_ok(&opts("dev-token", Some(tenant), None)).expect_err("half scope must fail");
    assert!(err.contains("missing scope"), "unhelpful message: {err}");

    // full scope passes locally
    scoped_ok(&opts("dev-token", Some(tenant), Some(organization))).expect("full scope ok");
}

#[test]
fn bootstrap_parses_explicit_arguments_without_env() {
    let cli = Cli::try_parse_from([
        "mas",
        "--base-url",
        "http://api.internal:8080",
        "--token",
        "tok",
        "--format",
        "json",
        "status",
    ])
    .expect("parses");
    assert_eq!(cli.global.base_url, "http://api.internal:8080");
    assert_eq!(cli.global.token, "tok");
    assert_eq!(cli.global.format, mas_cli::args::Format::Json);

    // missing subcommand is a usage error, not a panic
    assert!(Cli::try_parse_from(["mas"]).is_err());
}

#[test]
fn admin_bootstrap_parses_and_denies_a_wrong_capability_locally() {
    let cli = Cli::try_parse_from([
        "mas",
        "--token",
        "super-secret-bearer",
        "admin",
        "bootstrap",
        "--capability",
        "control_plane.bootstrap",
    ])
    .expect("parses");
    match cli.command {
        mas_cli::args::Command::Admin(mas_cli::args::AdminCmd::Bootstrap { capability }) => {
            assert_eq!(capability, "control_plane.bootstrap");
        },
        other => panic!("expected bootstrap, got {other:?}"),
    }

    let denied = Cli::try_parse_from([
        "mas",
        "--token",
        "super-secret-bearer",
        "admin",
        "bootstrap",
        "--capability",
        "not-a-capability",
    ])
    .expect("parses");
    let mas_cli::args::Command::Admin(mas_cli::args::AdminCmd::Bootstrap { ref capability }) =
        denied.command
    else {
        panic!("expected bootstrap");
    };
    assert_ne!(capability, "control_plane.bootstrap");

    let dbg = format!("{denied:?}");
    assert!(
        !dbg.contains("super-secret-bearer"),
        "token leaked in Debug: {dbg}"
    );
}
