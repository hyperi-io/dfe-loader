<!--
  Project:      dfe-loader
  File:         docs/performance/README.md
  Purpose:      Index for the performance docs
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Performance

Where the time goes and how to get it back. dfe-loader splits performance work
into build-time optimisations (allocator, LTO, PGO, BOLT -- now CI-automated)
and runtime tuning (batch sizes, concurrent inserts), with profiling tools for
finding the hot spots in between.

- [OVERVIEW.md](OVERVIEW.md) -- the optimisation guide: jemalloc, LTO, PGO and BOLT build optimisations, insert-throughput and batch tuning, and CPU/memory/flame-graph profiling.
