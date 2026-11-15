//! Generates the gRPC code from `proto/lwwo.proto`.

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=../../proto/lwwo.proto");
    tonic_prost_build::configure().compile_protos(&["../../proto/lwwo.proto"], &["../../proto"])
}
