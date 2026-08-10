---
name: Bug report
about: Something isn't working correctly
title: ''
labels: bug
assignees: ''
---

## What happened?

<!-- A clear description of the bug. -->

## What did you expect to happen?

## How to reproduce

<!-- Minimal steps, ideally against the docker-compose-local.yml stack. -->

1.
2.
3.

## Environment

- GTC version/commit:
- PostgreSQL version:
- Sinks in use (Redis Stream / RedisJSON / Meilisearch):
- Relevant env vars / sink YAML (redact credentials):

## Logs and diagnostics

<!--
- GTC logs around the failure (LOG_LEVEL=DEBUG if possible)
- Output of: SELECT * FROM pg_replication_slots;
- Relevant metrics (cdc_wal_lag_bytes, cdc_sink_errors_total, ...)
-->
