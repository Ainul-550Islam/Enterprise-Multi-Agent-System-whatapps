# Module 1 — full Rust ↔ Python AgentBridge source

This directory is an unabridged source overlay for the bridge, its durable task-attempt/correlation propagation, generated protobuf bindings, runtime composition, security boundary, and tests. Paths below this directory are repository-relative; copy/overlay them onto the matching `enterprise-mas/` root to restore the exact files in the audited workspace snapshot.

The code has not been shortened into snippets. Python `Protocol` declarations may use the language-standard `...` method body to declare an interface; those are interface declarations, not omitted file sections. The overlay depends on the rest of the repository's existing crates and Python package, which remain in the audited repository and are intentionally not duplicated here.

Audit warning: this bundle faithfully captures the current implementation; it does not silently claim the unresolved production agent/version resolver, state-reader/idempotency wiring, PostgreSQL broker-claim conflict, or durable result-persistence findings have been fixed. See `SIX_SPEC_AUDIT_REPORT.md` in the parent directory before deployment.

Files and checksums are listed in `manifest.json`.
