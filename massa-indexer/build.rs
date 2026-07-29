//! Build script: compile the subset of `massa-proto` the indexer needs **plus**
//! our own indexer-to-indexer peer protocol (`proto/indexer/v1/peer.proto`).
//!
//! We compile:
//!   - commons/*.proto                  (model types; client-only)
//!   - apis/public.proto                (PublicService - the gRPC service we call; client-only)
//!   - indexer/v1/peer.proto            (our Peer service; client + server)
//!
//! Third-party google protos (annotations / wrappers) come from the sibling
//! `../massa-proto/proto/third_party/` tree.
//!
//! To keep iteration fast the script reruns only when proto files change.
//!
//! The peer protocol is compiled in a separate tonic-build invocation because
//! we need `build_server(true)` for it (the server is hosted in-process).
//! Compiling both .proto groups in one call would force server stubs for the
//! massa protos too, bloating the binary with code we never run.

use std::path::{Path, PathBuf};

fn main() {
    let proto_root = PathBuf::from("../massa-proto/proto");
    if !proto_root.exists() {
        panic!(
            "expected massa-proto at {:?}. Clone massalabs/massa-proto next to this crate.",
            proto_root
        );
    }

    let third_party = proto_root.join("third_party");
    let commons = proto_root.join("commons");
    let apis = proto_root.join("apis");

    // Explicit list of .proto files we want tonic-build to generate code for.
    // Keeping the list explicit (instead of scanning) avoids pulling ABIs we
    // don't use and keeps build time low.
    let files: Vec<PathBuf> = vec![
        commons.join("massa/model/v1/address.proto"),
        commons.join("massa/model/v1/amount.proto"),
        commons.join("massa/model/v1/block.proto"),
        commons.join("massa/model/v1/commons.proto"),
        commons.join("massa/model/v1/datastore.proto"),
        commons.join("massa/model/v1/denunciation.proto"),
        commons.join("massa/model/v1/draw.proto"),
        commons.join("massa/model/v1/endorsement.proto"),
        commons.join("massa/model/v1/execution.proto"),
        commons.join("massa/model/v1/node.proto"),
        commons.join("massa/model/v1/operation.proto"),
        commons.join("massa/model/v1/slot.proto"),
        commons.join("massa/model/v1/staker.proto"),
        commons.join("massa/model/v1/stats.proto"),
        commons.join("massa/model/v1/time.proto"),
        commons.join("massa/model/v1/versioning.proto"),
        apis.join("massa/api/v1/public.proto"),
    ];

    for f in &files {
        if !f.exists() {
            panic!("missing proto file: {:?}", f);
        }
        println!("cargo:rerun-if-changed={}", f.display());
    }
    for dir in [&third_party, &commons, &apis] {
        register_rerun(dir);
    }

    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let out_path = PathBuf::from(&out_dir);

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir(&out_path)
        // We don't need the google.api.http extensions at runtime; compile
        // them but do not treat them specially.
        .compile_protos(
            &files,
            &[commons.clone(), apis.clone(), third_party.clone()],
        )
        .expect("tonic_build failed (massa protos)");

    // --- Indexer storage + peer protocol -----------------------------------
    //
    // `storage.proto` defines the shape of every row we persist in RocksDB
    // and every payload the peer wire ships. Keeping them in one .proto
    // avoids drift (a peer response can be written verbatim to local
    // storage). `peer.proto` defines the transport (RPC method signatures,
    // bitmasks, Health probe).
    //
    // Both live in this crate so they always match the running indexer (no
    // external proto dependency to keep in sync). Compiled with both client
    // AND server stubs: each indexer both calls peers and answers peer calls.
    let peer_root = PathBuf::from("proto");
    let storage_proto = peer_root.join("indexer/v1/storage.proto");
    let peer_proto = peer_root.join("indexer/v1/peer.proto");
    for f in [&storage_proto, &peer_proto] {
        if !f.exists() {
            panic!("missing proto file at {:?}", f);
        }
        println!("cargo:rerun-if-changed={}", f.display());
    }
    register_rerun(&peer_root);

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir(&out_path)
        .compile_protos(&[storage_proto, peer_proto], &[peer_root])
        .expect("tonic_build failed (indexer protos)");
}

fn register_rerun(dir: &Path) {
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if entry.file_type().is_file() {
            println!("cargo:rerun-if-changed={}", entry.path().display());
        }
    }
}
