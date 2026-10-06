//! PURPOSE: Cargo anchor for the workspace-root security matrix.
//!
//! Owning layer: integration tests, compiled as part of `mas-api`.
//!
//! Production responsibility: give cargo a package to hang the cross-cutting
//! security suite on, so the matrix runs in `cargo test --workspace` rather
//! than in a separate invocation somebody forgets.
//!
//! Security boundary: the suite needs `mas-api`'s test wiring, which is why
//! it lives here and is gated on the `testutils` feature.
//!
//! It must NOT:
//!   • duplicate any assertion — the tests live in
//!     `tests/security/authorization_matrix.rs`;
//!   • enable `testutils` in a production build.

//! Cargo anchor for the workspace-root security matrix (see
//! `tests/security/authorization_matrix.rs`).
//!
//! The suite needs `mas-api`'s test wiring, so it is compiled as part of this
//! crate under the `testutils` feature.

#![cfg(feature = "testutils")]

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/security/authorization_matrix.rs"
));
