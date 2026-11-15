<!--
  Project:      dfe-loader
  File:         docs/performance/README.md
  Purpose:      Index for the performance docs
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Performance

Where the time goes and how to get it back. dfe-loader splits performance work into build-time optimisations (allocator, LTO, PGO, BOLT), which hyperi-ci applies, and runtime tuning (batch sizes), with profiling tools for finding the hot spots in between.

- [overview.md](overview.md) -- what CI builds, the allocator, batch tuning, and CPU, heap and flame-graph profiling.
- [history.md](history.md) -- why the loader's parser and insert encoding are the ones it uses.
