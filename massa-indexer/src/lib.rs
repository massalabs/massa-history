//! Massa Indexer V2 — library root.
//!
//! The indexer streams from a massa-node's gRPC into RocksDB and exposes a
//! REST + SSE API. This file is the crate root; every subsystem lives in its
//! own module below.
//!
//! See `../spec.md` for the full specification.

#![deny(rust_2018_idioms)]
#![recursion_limit = "512"]

pub mod config;
pub mod error;
pub mod ids;
pub mod keys;
pub mod model;
pub mod proto;
pub mod schema;
pub mod token;
pub mod codec;
pub mod db;
pub mod ingest;
pub mod grpc;
pub mod legacy;
pub mod metrics;
pub mod peer;
pub mod openapi;
pub mod rest;
pub mod sse;
pub mod server;
pub mod cli;

pub use error::{Error, Result};
