//! Cargo anchor for the workspace-root contract suite (see
//! `tests/contract/*.rs`; fixtures in `tests/fixtures/`).
//!
//! Contract tests load only public crates — they run unconditionally.

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/contract/api_contract.rs"
));
