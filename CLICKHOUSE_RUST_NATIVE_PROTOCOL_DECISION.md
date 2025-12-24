# ClickHouse Native Protocol Support for Rust  
**Decision & Architecture Reference (2025)**

## Executive Summary

As of **early 2025**, there is **no officially vendor-supported Rust client** from ClickHouse Inc. that uses the **native ClickHouse TCP binary protocol**.  

However, there **is**:

1. A **vendor-supported C++ client library** that fully implements the native protocol and can be used from Rust via FFI.
2. Several **high-performance, actively maintained Rust community libraries** that implement the native protocol directly and are widely used in production.

This document evaluates all viable options against the criteria:

- Native ClickHouse binary protocol (TCP, port 9000)
- Performance and throughput
- Error handling and helper ergonomics
- Maintenance quality and long-term risk
- Comparability to the **official ClickHouse Go client**

---

## Decision Matrix (TL;DR)

| Option | Native Protocol | Vendor Supported | Performance | Ergonomics | Risk |
|------|-----------------|------------------|-------------|------------|------|
| Official Rust (`clickhouse`) | ❌ HTTP only | ✅ ClickHouse Inc | Medium-High | Excellent | Low |
| C++ Client via FFI | ✅ Native TCP | ✅ ClickHouse Inc | **Very High** | Medium | Medium |
| **Klickhouse (Rust)** | ✅ Native TCP | ❌ Community | **Very High** | Good | Low-Medium |
| **clickhouse-arrow (Rust)** | ✅ Native TCP | ❌ Community | **Excellent++** | Medium | Medium |

---

## 1. Official ClickHouse Rust Client (HTTP / RowBinary)

### Status
- **Maintained by ClickHouse Inc**
- Transport: **HTTP**
- Encoding: **RowBinary**
- Async, Serde-based, very ergonomic

### Key Limitation
> **Does NOT use native TCP protocol**

ClickHouse explicitly documents that the current Rust client uses RowBinary **over HTTP**, not the native TCP protocol, though future native support is planned.

### Verdict
✅ Best officially supported Rust option  
❌ Not suitable if **native protocol is mandatory**

---

## 2. Official ClickHouse C++ Client (Native Protocol)

### Overview
- **Fully vendor-supported**
- Uses **native TCP binary protocol**
- Same protocol used by official Go client

### Rust Integration Model
- C ABI shim
- Rust FFI bindings

### Verdict
🟢 **Gold standard if vendor support + native protocol is non-negotiable**  

---

## 3. Klickhouse (Pure Rust, Native Protocol)

### Overview
- Pure Rust async client
- Native TCP
- LZ4 + TLS support

### Verdict
🟢 **Best Rust-native equivalent to Go client today**

---

## 4. clickhouse-arrow (Rust + Arrow, Native Protocol)

### Overview
- Native TCP
- Apache Arrow integration
- Extreme throughput

### Verdict
🟢 **Best performance ceiling in Rust**

---

## Final Recommendations

- **Vendor support required** → C++ client via FFI  
- **Rust-native & fast** → Klickhouse  
- **Analytics / Arrow pipelines** → clickhouse-arrow  
- **Simplicity** → Official Rust (HTTP)

---

## Strategic Note

Official Rust native TCP support is expected but not yet released.
