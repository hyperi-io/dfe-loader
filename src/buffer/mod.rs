//! Columnar buffer management
//!
//! Two buffer implementations:
//! - `ColumnarBuffer`: Legacy JSON-based buffer (MVP compatibility)
//! - `NativeBuffer`: klickhouse-native format for zero-copy insert (preferred)

pub mod columnar;
pub mod manager;
pub mod native;
pub mod pool;

pub use columnar::ColumnarBuffer;
pub use manager::{BufferManager, FlushBatch};
pub use native::NativeBuffer;
