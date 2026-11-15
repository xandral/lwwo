//! The gRPC code generated from `proto/lwwo.proto`.

#![allow(clippy::all, clippy::pedantic, missing_docs, unreachable_pub)]

tonic::include_proto!("lwwo.v1");

/// The version of the contract between master and workers. Raise it when
/// a change means one side would misread the other: a master and a worker
/// that speak different versions refuse to work together, and say so,
/// instead of failing in a strange way later.
pub(crate) const PROTOCOL: u32 = 1;
