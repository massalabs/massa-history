//! Crate-wide error type.

use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("config: {0}")]
    Config(String),

    #[error("db: {0}")]
    Db(#[from] rocksdb::Error),

    #[error("proto decode: {0}")]
    ProtoDecode(#[from] prost::DecodeError),

    #[error("proto encode: {0}")]
    ProtoEncode(#[from] prost::EncodeError),

    #[error("grpc transport: {0}")]
    GrpcTransport(#[from] Box<tonic::transport::Error>),

    // Boxed so the overall `Error` enum stays small (~32 bytes) — required
    // to keep clippy's `result_large_err` lint happy without sprinkling
    // per-function allows everywhere. `tonic::Status` is ~176 bytes, which
    // alone blows past the 128-byte default threshold.
    #[error("grpc status: {0}")]
    GrpcStatus(#[from] Box<tonic::Status>),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("network mismatch: db={db}, config={cfg}")]
    NetworkMismatch { db: String, cfg: String },

    #[error("not found")]
    NotFound,

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::BadRequest(msg.into())
    }
}

// `thiserror`'s `#[from] Box<T>` only emits `From<Box<T>>`. We want `?`
// to work directly on the unboxed gRPC errors too — boxing is an
// implementation detail (keeps `Error` small enough for clippy's
// `result_large_err` lint).
impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self { Self::GrpcStatus(Box::new(s)) }
}
impl From<tonic::transport::Error> for Error {
    fn from(e: tonic::transport::Error) -> Self { Self::GrpcTransport(Box::new(e)) }
}
