<!--
  Project:      dfe-loader
  File:         docs/deployment/README.md
  Purpose:      Index for the deployment docs
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Deployment

How dfe-loader gets built into artefacts, and where the tables it writes come from. dfe-docker and dfe-infra pull the published image and chart, and the loader reads its tables' schema from ClickHouse, never from dfe-schemas.

- [publishing.md](publishing.md) -- container image and Helm chart publishing via CI: the generated Dockerfile, `.hyperi-ci.yaml`, the tags and who pins them.
- [schemas.md](schemas.md) -- where a table's schema comes from, and the column directives the loader reads off it.
