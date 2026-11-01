//! Cargo anchor for the workspace-root integration suite.
//!
//! The canonical files live at `<workspace>/tests/integration/*.rs` (the
//! directory tree from the repository layout spec); cargo needs a package to
//! hang test targets on, so `mas-api` — the tenant-facing process the suites
//! exercise — includes them here. `state::test_support` becomes public under
//! the `testutils` feature for exactly this wiring.

#![cfg(feature = "testutils")]

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/integration/full_stack.rs"
));
