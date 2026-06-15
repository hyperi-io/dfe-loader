// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Object pool for memory reuse
//!
//! Provides pooled allocation for frequently-allocated types to reduce
//! allocation overhead in the hot path. Uses crossbeam channels for
//! lock-free access.
//!
//! ## Pooled Types
//!
//! - `PooledMap` - `Map<String, Value>` for JSON objects (flattening, transform)
//! - `PooledOffsets` - `Vec<KafkaOffset>` for batch offset tracking
//! - `PooledString` - `String` for key construction and formatting
//!
//! ## Usage
//!
//! ```ignore
//! let pool = ObjectPool::<PooledMap>::new(100);
//! let mut map = pool.get(); // Gets from pool or creates new
//! map.insert("key".into(), Value::Null);
//! // map is returned to pool on drop
//! ```

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};
use serde_json::{Map, Value};

use crate::buffer::KafkaOffset;

/// Trait for types that can be pooled
pub trait Poolable: Default + Send + 'static {
    /// Reset the object to a clean state for reuse
    fn reset(&mut self);

    /// Estimated capacity to pre-allocate (optional optimization)
    fn default_capacity() -> usize {
        0
    }
}

/// A pooled object that returns to the pool on drop
pub struct Pooled<T: Poolable> {
    inner: Option<T>,
    return_tx: Sender<T>,
}

impl<T: Poolable> Pooled<T> {
    fn new(value: T, return_tx: Sender<T>) -> Self {
        Self {
            inner: Some(value),
            return_tx,
        }
    }
}

impl<T: Poolable> Deref for Pooled<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("Pooled object already taken")
    }
}

impl<T: Poolable> DerefMut for Pooled<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().expect("Pooled object already taken")
    }
}

impl<T: Poolable> Drop for Pooled<T> {
    fn drop(&mut self) {
        if let Some(mut obj) = self.inner.take() {
            obj.reset();
            // Try to return to pool, ignore if full
            let _ = self.return_tx.try_send(obj);
        }
    }
}

/// Generic object pool using crossbeam channels
pub struct ObjectPool<T: Poolable> {
    available_rx: Receiver<T>,
    return_tx: Sender<T>,
    capacity: usize,
    /// Stats: objects created (not from pool)
    creates: AtomicUsize,
    /// Stats: objects retrieved from pool
    hits: AtomicUsize,
}

impl<T: Poolable> ObjectPool<T> {
    /// Create a new pool with the given capacity
    pub fn new(capacity: usize) -> Self {
        let (return_tx, available_rx) = bounded(capacity);

        // Pre-populate the pool
        for _ in 0..capacity {
            let obj = T::default();
            let _ = return_tx.try_send(obj);
        }

        Self {
            available_rx,
            return_tx,
            capacity,
            creates: AtomicUsize::new(0),
            hits: AtomicUsize::new(0),
        }
    }

    /// Get an object from the pool, or create a new one if pool is empty
    pub fn get(&self) -> Pooled<T> {
        match self.available_rx.try_recv() {
            Ok(obj) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Pooled::new(obj, self.return_tx.clone())
            }
            Err(TryRecvError::Empty) => {
                self.creates.fetch_add(1, Ordering::Relaxed);
                Pooled::new(T::default(), self.return_tx.clone())
            }
            Err(TryRecvError::Disconnected) => {
                // Pool is shutting down, just create a new object
                self.creates.fetch_add(1, Ordering::Relaxed);
                Pooled::new(T::default(), self.return_tx.clone())
            }
        }
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            capacity: self.capacity,
            available: self.available_rx.len(),
            hits: self.hits.load(Ordering::Relaxed),
            creates: self.creates.load(Ordering::Relaxed),
        }
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Maximum pool size
    pub capacity: usize,
    /// Currently available objects
    pub available: usize,
    /// Objects retrieved from pool (cache hits)
    pub hits: usize,
    /// Objects created fresh (cache misses)
    pub creates: usize,
}

impl PoolStats {
    /// Calculate hit rate as a percentage
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.creates;
        if total == 0 {
            100.0
        } else {
            (self.hits as f64 / total as f64) * 100.0
        }
    }
}

// ============================================================================
// Poolable implementations for common types
// ============================================================================

/// Pooled JSON Map
pub type PooledMap = Map<String, Value>;

impl Poolable for PooledMap {
    fn reset(&mut self) {
        self.clear();
    }

    fn default_capacity() -> usize {
        32 // Typical event has ~20-50 fields after flattening
    }
}

/// Pooled `Vec<KafkaOffset>`
pub type PooledOffsets = Vec<KafkaOffset>;

impl Poolable for PooledOffsets {
    fn reset(&mut self) {
        self.clear();
    }

    fn default_capacity() -> usize {
        1000 // Typical batch size
    }
}

/// Pooled String buffer
pub type PooledString = String;

impl Poolable for PooledString {
    fn reset(&mut self) {
        self.clear();
    }

    fn default_capacity() -> usize {
        256 // Typical flattened key length
    }
}

// ============================================================================
// Pre-configured pool types with sensible defaults
// ============================================================================

/// Pool for JSON Map objects (used in flattening and transform)
pub type MapPool = ObjectPool<PooledMap>;

/// Pool for `KafkaOffset` vectors (used in batch tracking)
pub type OffsetsPool = ObjectPool<PooledOffsets>;

/// Pool for String buffers (used in key construction)
pub type StringPool = ObjectPool<PooledString>;

/// Configuration for buffer pools
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Size of the Map pool
    pub map_pool_size: usize,
    /// Size of the offsets pool
    pub offset_pool_size: usize,
    /// Size of the string pool
    pub string_pool_size: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            map_pool_size: 1000,
            offset_pool_size: 100,
            string_pool_size: 500,
        }
    }
}

/// Combined buffer pools for the pipeline
pub struct BufferPools {
    /// Pool for JSON Map objects
    pub maps: MapPool,
    /// Pool for `KafkaOffset` vectors
    pub offsets: OffsetsPool,
    /// Pool for String buffers
    pub strings: StringPool,
}

impl BufferPools {
    /// Create buffer pools with the given configuration
    pub fn new(config: &PoolConfig) -> Self {
        Self {
            maps: ObjectPool::new(config.map_pool_size),
            offsets: ObjectPool::new(config.offset_pool_size),
            strings: ObjectPool::new(config.string_pool_size),
        }
    }

    /// Get combined statistics
    pub fn stats(&self) -> BufferPoolsStats {
        BufferPoolsStats {
            maps: self.maps.stats(),
            offsets: self.offsets.stats(),
            strings: self.strings.stats(),
        }
    }
}

impl Default for BufferPools {
    fn default() -> Self {
        Self::new(&PoolConfig::default())
    }
}

/// Combined statistics for all buffer pools
#[derive(Debug, Clone)]
pub struct BufferPoolsStats {
    pub maps: PoolStats,
    pub offsets: PoolStats,
    pub strings: PoolStats,
}

// ============================================================================
// Legacy compatibility - keep old BufferPool struct for now
// ============================================================================

/// Legacy buffer pool (deprecated, use `BufferPools` instead)
#[deprecated(note = "Use BufferPools instead")]
pub struct BufferPool;

#[allow(deprecated)]
impl BufferPool {
    /// Create a new buffer pool (legacy)
    pub fn new() -> Self {
        Self
    }
}

#[allow(deprecated)]
impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pooled_map_basic() {
        let pool: ObjectPool<PooledMap> = ObjectPool::new(10);

        // Get a map from pool
        let mut map = pool.get();
        map.insert("key".to_string(), Value::String("value".to_string()));
        assert_eq!(map.len(), 1);

        // Drop returns to pool
        drop(map);

        // Next get should return the same (cleared) map
        let map2 = pool.get();
        assert!(map2.is_empty());

        let stats = pool.stats();
        assert_eq!(stats.hits, 2); // Pre-populated + reuse
    }

    #[test]
    fn test_pooled_offsets() {
        let pool: ObjectPool<PooledOffsets> = ObjectPool::new(5);

        let mut offsets = pool.get();
        offsets.push(KafkaOffset::new("topic", 0, 100));
        offsets.push(KafkaOffset::new("topic", 0, 101));
        assert_eq!(offsets.len(), 2);

        drop(offsets);

        let offsets2 = pool.get();
        assert!(offsets2.is_empty());
    }

    #[test]
    fn test_pooled_string() {
        let pool: ObjectPool<PooledString> = ObjectPool::new(5);

        let mut s = pool.get();
        s.push_str("hello");
        s.push_str(".world");
        assert_eq!(&*s, "hello.world");

        drop(s);

        let s2 = pool.get();
        assert!(s2.is_empty());
    }

    #[test]
    fn test_pool_stats() {
        let pool: ObjectPool<PooledMap> = ObjectPool::new(2);

        // Pool starts pre-populated
        let stats = pool.stats();
        assert_eq!(stats.capacity, 2);
        assert_eq!(stats.available, 2);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.creates, 0);

        // Get 3 objects (2 from pool, 1 fresh)
        let _a = pool.get();
        let _b = pool.get();
        let _c = pool.get();

        let stats = pool.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.creates, 1);
        assert!(stats.hit_rate() > 60.0);
    }

    #[test]
    fn test_buffer_pools_combined() {
        let config = PoolConfig {
            map_pool_size: 10,
            offset_pool_size: 5,
            string_pool_size: 8,
        };
        let pools = BufferPools::new(&config);

        let _map = pools.maps.get();
        let _offsets = pools.offsets.get();
        let _string = pools.strings.get();

        let stats = pools.stats();
        assert_eq!(stats.maps.capacity, 10);
        assert_eq!(stats.offsets.capacity, 5);
        assert_eq!(stats.strings.capacity, 8);
    }

    #[test]
    fn test_pool_overflow() {
        // Pool of size 2, get 4 objects
        let pool: ObjectPool<PooledString> = ObjectPool::new(2);

        let s1 = pool.get();
        let s2 = pool.get();
        let s3 = pool.get(); // Fresh allocation
        let s4 = pool.get(); // Fresh allocation

        let stats = pool.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.creates, 2);

        // Drop all - only 2 will fit back in pool
        drop(s1);
        drop(s2);
        drop(s3);
        drop(s4);

        // Pool should be full (2 items)
        let stats = pool.stats();
        assert_eq!(stats.available, 2);
    }

    #[test]
    fn test_pool_stats_hit_rate_empty_is_100() {
        // With no gets yet, hit_rate is defined as 100%
        let pool: ObjectPool<PooledString> = ObjectPool::new(4);
        let stats = pool.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.creates, 0);
        assert_eq!(stats.hit_rate(), 100.0);
    }

    #[test]
    fn test_pool_stats_hit_rate_all_creates_is_zero() {
        // Zero-capacity pool: every get is a fresh create
        let pool: ObjectPool<PooledString> = ObjectPool::new(0);
        let _a = pool.get();
        let _b = pool.get();
        let _c = pool.get();
        let stats = pool.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.creates, 3);
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn test_pool_stats_hit_rate_mixed() {
        let pool: ObjectPool<PooledString> = ObjectPool::new(2);
        let a = pool.get(); // hit
        let b = pool.get(); // hit
        let c = pool.get(); // create
        drop((a, b, c));
        let stats = pool.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.creates, 1);
        // hit_rate = 2/3 = ~66.67
        assert!(stats.hit_rate() > 66.0);
        assert!(stats.hit_rate() < 67.0);
    }

    #[test]
    fn test_poolable_default_capacity_map() {
        assert_eq!(<PooledMap as Poolable>::default_capacity(), 32);
    }

    #[test]
    fn test_poolable_default_capacity_offsets() {
        assert_eq!(<PooledOffsets as Poolable>::default_capacity(), 1000);
    }

    #[test]
    fn test_poolable_default_capacity_string() {
        assert_eq!(<PooledString as Poolable>::default_capacity(), 256);
    }

    #[test]
    fn test_pool_config_default_values() {
        let cfg = PoolConfig::default();
        assert_eq!(cfg.map_pool_size, 1000);
        assert_eq!(cfg.offset_pool_size, 100);
        assert_eq!(cfg.string_pool_size, 500);
    }

    #[test]
    fn test_buffer_pools_default() {
        // Use the default-impl path of BufferPools
        let pools = BufferPools::default();
        let stats = pools.stats();
        // Default sizes match PoolConfig::default
        assert_eq!(stats.maps.capacity, 1000);
        assert_eq!(stats.offsets.capacity, 100);
        assert_eq!(stats.strings.capacity, 500);
    }

    #[test]
    #[allow(deprecated)]
    fn test_legacy_buffer_pool_still_constructible() {
        // Deprecated, but must still build and default cleanly
        let _ = BufferPool::new();
        let _ = BufferPool;
    }

    #[test]
    fn test_pool_reset_on_drop_preserves_capacity() {
        // Items returned to the pool should be reset (cleared) and reusable
        let pool: ObjectPool<PooledMap> = ObjectPool::new(1);
        {
            let mut m = pool.get();
            m.insert("x".to_string(), Value::String("y".to_string()));
            m.insert("a".to_string(), Value::Number(42.into()));
            assert_eq!(m.len(), 2);
        } // Drop — reset() clears
        let m2 = pool.get();
        assert!(m2.is_empty(), "returned map should be cleared");
    }

    #[test]
    fn test_pool_stats_clone_is_independent() {
        let pool: ObjectPool<PooledString> = ObjectPool::new(2);
        let s1 = pool.stats();
        let _g = pool.get();
        let s2 = pool.stats();
        // First snapshot remains unchanged
        assert_eq!(s1.hits, 0);
        // Second snapshot reflects hit
        assert_eq!(s2.hits, 1);
    }

    #[test]
    fn test_pooled_offsets_default_capacity_roundtrip() {
        let pool: ObjectPool<PooledOffsets> = ObjectPool::new(3);
        let mut offsets = pool.get();
        for i in 0..10 {
            offsets.push(KafkaOffset::new("t", 0, i));
        }
        assert_eq!(offsets.len(), 10);
        drop(offsets);
        let offsets2 = pool.get();
        assert_eq!(offsets2.len(), 0, "reset clears contents");
    }

    #[test]
    fn test_buffer_pools_new_explicit_sizes() {
        let cfg = PoolConfig {
            map_pool_size: 3,
            offset_pool_size: 2,
            string_pool_size: 1,
        };
        let pools = BufferPools::new(&cfg);
        assert_eq!(pools.maps.stats().capacity, 3);
        assert_eq!(pools.offsets.stats().capacity, 2);
        assert_eq!(pools.strings.stats().capacity, 1);
    }
}
