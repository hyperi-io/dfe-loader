// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Event enrichment (`GeoIP`, reputation, risk scoring).
//!
//! Reference data -- the GeoIP databases and the blocklists behind reputation
//! -- is fetched, verified and served by `factbook`. What lives here is what
//! the data MEANS to this loader: the field names the destination schema
//! expects, the threat types a blocklist row stands for, and the risk score
//! composed from them.

pub mod geoip;
pub mod reputation;
pub mod risk;
