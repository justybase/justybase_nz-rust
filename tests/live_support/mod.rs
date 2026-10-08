//! Shared configuration and helpers for appliance-backed (LIVE) tests.
//!
//! LIVE tests are `#[ignore]`d, so a plain `cargo test` never needs a Netezza
//! appliance. Running them explicitly (`-- --ignored`, or
//! `scripts/test-live.sh`) makes a missing configuration a hard failure
//! instead of a silent skip. Required environment:
//!
//! * `NZ_DEV_HOST`, `NZ_DEV_USER`, `NZ_DEV_PASSWORD`
//! * `NZ_DEV_DB` or `NZ_DEV_DATABASE`
//! * optional `NZ_DEV_PORT` (default 5480)
//!
//! `NZ_RUN_LIVE_TESTS` is no longer required; the runner still exports it for
//! older tooling. Credentials are never printed.
#![allow(dead_code)]

use nz_rust::NzConnectionConfig;
use std::sync::atomic::{AtomicU32, Ordering};

pub const IGNORE_REASON: &str = "requires live Netezza appliance";

/// Configuration for the live appliance; panics (failing the test) when any
/// required variable is missing, naming the variables but never their values.
pub fn live_config() -> NzConnectionConfig {
    let read = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let host = read("NZ_DEV_HOST");
    let user = read("NZ_DEV_USER");
    let password = read("NZ_DEV_PASSWORD");
    let database = read("NZ_DEV_DB").or_else(|| read("NZ_DEV_DATABASE"));
    let mut missing = Vec::new();
    if host.is_none() {
        missing.push("NZ_DEV_HOST");
    }
    if user.is_none() {
        missing.push("NZ_DEV_USER");
    }
    if password.is_none() {
        missing.push("NZ_DEV_PASSWORD");
    }
    if database.is_none() {
        missing.push("NZ_DEV_DB (or NZ_DEV_DATABASE)");
    }
    assert!(
        missing.is_empty(),
        "LIVE test configuration error: missing {}. LIVE tests run only when \
         requested explicitly; set the variables or run without --ignored.",
        missing.join(", ")
    );
    let port = match read("NZ_DEV_PORT") {
        None => 5480,
        Some(port) => port
            .parse()
            .unwrap_or_else(|_| panic!("LIVE test configuration error: NZ_DEV_PORT is not a port")),
    };
    NzConnectionConfig {
        host: host.unwrap(),
        port,
        database: database.unwrap(),
        user: user.unwrap(),
        password: password.unwrap(),
        ..Default::default()
    }
}

/// A unique, prefixed object name: `RUST_<pid>_<micros>_<counter>`.
/// Persistent objects created by LIVE tests always use this form, so they can
/// never collide with user objects.
pub fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros();
    format!(
        "{}_{}_{}_{}",
        prefix,
        std::process::id(),
        micros,
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}
