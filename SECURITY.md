# Security Policy

## Supported Versions

GTC is pre-1.0; only the latest release (and `main`) receives security
fixes.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub
issues.**

Report them privately via
[GitHub Security Advisories](https://github.com/emoss08/gtc/security/advisories/new)
("Report a vulnerability" on the repository's Security tab).

Please include:

- A description of the issue and its impact
- Steps to reproduce or a proof of concept
- Affected version/commit and configuration (redact credentials)

You should receive an acknowledgment within a few days. Please allow a
reasonable window for a fix before public disclosure.

## Scope notes

Things that are useful to know when assessing GTC deployments:

- GTC needs a PostgreSQL role with the `REPLICATION` attribute. That role can
  read **all row data** flowing through the WAL for published tables — scope
  the publication and network access accordingly.
- GTC writes raw row data to the configured sinks. Anyone with read access to
  the Redis instance, stream keys, or Meilisearch index can read that data;
  secure the sinks like you secure the database.
- Credentials are supplied via environment variables (`DATABASE_URL`,
  `REDIS_URL`, `MEILISEARCH_API_KEY`, ...). Prefer secret managers or
  injected env vars over committing `.env` files.
- The HTTP server (`/health`, `/readiness`, `/metrics`) is unauthenticated
  and intended for private networks; metrics include schema and table names.
