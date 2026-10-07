// PURPOSE:
// Generates the Rust `mas.api.v1` client stubs from the canonical proto.
//
// Uses the vendored protoc so the build needs no system protobuf compiler, and
// re-runs when the proto changes so the generated client can never lag behind
// the contract.

//! Compiles the shared v1 proto into the **client** half used to call the
//! Python AI Orchestrator.
//!
//! This crate is the client of `AgentBridgeApi`; `crates/api` is the server of
//! the platform-facing services and builds the server half. Both compile the
//! same authoritative contract file, so Rust and Python cannot drift apart
//! without a build failure.

fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc for this platform");
    std::env::set_var("PROTOC", &protoc);
    println!("cargo:rerun-if-changed=../../crates/api/proto/mas/api/v1/mas.proto");
    tonic_build::configure()
        // We call Python; we never serve this service from Rust.
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &["../../crates/api/proto/mas/api/v1/mas.proto"],
            &["../../crates/api/proto"],
        )
        .expect("compile mas.api.v1 proto for client");
}
