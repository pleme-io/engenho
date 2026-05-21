//! engenho-mcp — typed MCP server for the engenho cluster operator surface.
//!
//! P0 surface: four read-only tools sourced from kikai's on-disk state.
//! Future phases (writer layer with saguão passport + tameshi receipts,
//! native engenho-apiserver tools) extend this without rewriting the
//! existing trait boundaries.
//!
//! The crate exposes three modules:
//!
//! * [`views`] — the wire shapes (Serialize + JsonSchema) the server emits
//! * [`reader`] — the `ClusterReader` trait + production / mock impls
//! * [`server`] — the rmcp `ServerHandler` impl wiring the trait to tools

pub mod reader;
pub mod redaction;
pub mod resource_kind;
pub mod server;
pub mod views;
pub mod writer;

pub use reader::{ClusterReader, ReaderError};
pub use reader::kikai::KikaiClusterReader;
pub use resource_kind::ResourceKind;
pub use server::EngenhoMcp;
pub use writer::{Authority, ClusterWriter, WriterError};
pub use writer::kikai::KikaiClusterWriter;
