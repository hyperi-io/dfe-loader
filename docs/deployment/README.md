<!--
  Project:      dfe-loader
  File:         docs/deployment/README.md
  Purpose:      Index for the deployment docs
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Deployment

How dfe-loader gets built into artefacts and what it depends on at deploy time.
This covers publishing the container image and Helm chart that downstream
projects (dfe-docker, dfe-operator) pull, and the shared `dfe-schemas`
definitions the loader resolves at runtime to drive its table DDL.

- [PUBLISHING.md](PUBLISHING.md) -- container image + Helm chart publishing via CI: Dockerfile, `.hyperi-ci.yaml`, generated tags, and consumer usage.
- [SCHEMAS.md](SCHEMAS.md) -- loader-side reference for the shared schema definitions: submodule setup, resolution order, per-file versioning, and DFE `expr` directives.
