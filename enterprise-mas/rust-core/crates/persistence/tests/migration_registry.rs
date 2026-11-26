//! Repository-level consistency checks for the embedded migration set.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use mas_persistence::migrations::MIGRATIONS;

fn migrations_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations")
}

#[test]
fn every_migration_directory_is_registered_once_and_has_required_sql_files() {
    let root = migrations_dir();
    let mut directories = BTreeMap::<u32, String>::new();

    for entry in fs::read_dir(&root).expect("migrations directory must be readable") {
        let entry = entry.expect("migration directory entry must be readable");
        if !entry
            .file_type()
            .expect("entry type must be readable")
            .is_dir()
        {
            continue;
        }
        let name = entry
            .file_name()
            .into_string()
            .expect("directory name must be UTF-8");
        let (prefix, suffix) = name.split_once('_').expect("migration name needs a prefix");
        assert_eq!(
            prefix.len(),
            4,
            "{name} must have a four-digit version prefix"
        );
        let version: u32 = prefix.parse().expect("migration prefix must be numeric");
        assert!(
            !suffix.is_empty(),
            "{name} must have a non-empty migration name"
        );
        assert!(
            directories.insert(version, name.clone()).is_none(),
            "duplicate version prefix: {prefix}"
        );
        assert!(
            entry.path().join("up.sql").is_file(),
            "{name} is missing up.sql"
        );
        if version >= 42 {
            assert!(
                entry.path().join("down.sql").is_file(),
                "{name} is missing required down.sql"
            );
        }
    }

    let mut registered = BTreeMap::<String, usize>::new();
    let mut versions = BTreeSet::new();
    for migration in MIGRATIONS {
        assert!(
            versions.insert(migration.version),
            "duplicate registered version {}",
            migration.version
        );
        let directory = directories.get(&migration.version).unwrap_or_else(|| {
            panic!(
                "registered migration {} has no directory",
                migration.version
            )
        });
        let expected_name = directory
            .split_once('_')
            .expect("migration directory has prefix")
            .1;
        assert_eq!(
            migration.name, expected_name,
            "registered name differs for {directory}"
        );
        *registered.entry(directory.clone()).or_default() += 1;
    }

    for directory in directories.values() {
        assert_eq!(
            registered.get(directory),
            Some(&1),
            "{directory} must be registered exactly once"
        );
    }
    assert_eq!(
        registered.len(),
        directories.len(),
        "registry and migration directory counts differ"
    );
}
