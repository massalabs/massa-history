//! Re-exports of the generated protobuf code.
//!
//! `tonic_build` writes files named after the proto package (e.g.
//! `massa.model.v1.rs`, `massa.api.v1.rs`) into `$OUT_DIR`. We wrap each in a
//! module whose Rust path mirrors the proto package, so user code can write
//! things like `crate::proto::massa::model::v1::Slot`.

#[allow(clippy::all)]
pub mod massa {
    pub mod model {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/massa.model.v1.rs"));
        }
    }
    pub mod api {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/massa.api.v1.rs"));
        }
    }
}

#[allow(clippy::all)]
pub mod google {
    pub mod api {
        include!(concat!(env!("OUT_DIR"), "/google.api.rs"));
    }
}

/// Indexer-to-indexer peer protocol (see `proto/indexer/v1/peer.proto`).
#[allow(clippy::all)]
pub mod indexer {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/massa.indexer.v1.rs"));
    }
}
