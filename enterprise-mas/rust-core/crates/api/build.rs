//! Compiles the v1 proto into tonic server descriptors using a vendored
//! `protoc` (deploy environments must never depend on a system protoc).

fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc for this platform");
    // Build scripts execute before any dependent crate code; setting this
    // env var is the documented way to point prost-build at a protoc.
    std::env::set_var("PROTOC", &protoc);
    println!("cargo:rerun-if-changed=proto/mas/api/v1/mas.proto");
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/mas/api/v1/mas.proto"], &["proto"])
        .expect("compile mas.api.v1 proto");
}
