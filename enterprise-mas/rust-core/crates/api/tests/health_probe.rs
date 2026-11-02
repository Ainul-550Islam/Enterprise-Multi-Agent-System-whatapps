//! PURPOSE: The container health probe `mas-api --health` must succeed.
//!
//! `deploy/docker/api.Dockerfile` declares
//! `HEALTHCHECK CMD ["/usr/local/bin/mas-api", "--health"]`. If the flag is not
//! handled, the process falls through to the production bootstrap, fails with
//! "production mode requires a database URL", and exits non-zero — so the
//! container is reported unhealthy forever and any `depends_on:
//! service_healthy` gate never opens.
//!
//! This test runs the compiled binary so the flag is exercised the same way the
//! image exercises it.
//!
//! It must NOT start a server, bind a port, or require a database: a health
//! probe that needs the dependency it is probing is not a probe.

use std::process::Command;

#[test]
fn health_flag_exits_successfully_without_configuration() {
    let output = Command::new(env!("CARGO_BIN_EXE_mas-api"))
        .arg("--health")
        // A deliberately empty environment: the probe must not depend on
        // MAS_DATABASE_URL or any other production setting.
        .env_clear()
        .output()
        .expect("mas-api binary should be runnable");

    assert!(
        output.status.success(),
        "--health must exit 0, got {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("ok"),
        "--health should report readiness on stdout, got {stdout:?}"
    );
}
