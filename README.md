# CSPI News Worker

Local integration `integration/automatic-source-rss-20261018`, based on `57ccf87`,
is verified with app `79847a0` plus automatic URL onboarding. This records the
integration baseline; activation requires matching release-image evidence.
Matching app migration must precede this worker; the new app UI follows
it. Source-level admission and network hardening are required together. Keep the
live interval override at 60 minutes; 10-minute freshness is still unverified.
App-only rollback can retain this worker. Old-worker rollback requires explicit
pause of new URLs first; do not discard source metadata or collected items.

Independent .NET 10 RSS/Atom collection worker for the CSPI project.

This worker originated in [CSPI PR #40](https://github.com/mathewsun/cspi/pull/40). The automatic-onboarding branch requires the coordinated app migration and changes documented below; no deployment is performed by building this branch.

## Structure

- `Program.cs` — worker host and HTTP client
- `CollectorService.cs` — concurrent PostgreSQL job leasing and feed ingestion
- `sql/001_collector_jobs.sql` — prototype tables and initial job population
- `Dockerfile` — standalone image build
- `docs/distributed-news-collector.md` — design, scaling calculations and production blockers

## Development

```bash
dotnet build Cspi.NewsCollector.csproj
docker build -t cspi-news-worker:dev .
```

Run only against a dedicated test PostgreSQL database after applying the SQL setup script. Set `ConnectionStrings__DefaultConnection` to the database connection string, and configure `Collector__Concurrency` and `Collector__IntervalMinutes`.

**Do not start against production.** Before enabling ingestion, address SSRF/redirect/DNS security, per-host throttling, lease renewal, retries, rights/robots, feed discovery, publication integration and test coverage. The current prototype stores collected items in a separate staging table, not the CSPI Publications table.

## Polling objective: every enabled source within 10 minutes

Target: each unique active source should be checked successfully at least once in a rolling 10-minute window, subject to publisher access and transient failures. A scheduled attempt does not equal a successful refresh.

- 1,591 source-country records imply **up to** 1,591 checks/10 minutes, or 2.65 checks/second; actual unique sites may be fewer.
- Start benchmarking with `Collector__Concurrency=16` and `Collector__IntervalMinutes=10` in a staging environment.
- Estimate concurrency as `source_count * average_duration_seconds / 600`, then add capacity for long-tail latency, discovery, retries and domain throttling.
- Measure `fresh_last_10m / total_jobs`, `never_successful`, `due_jobs`, p95 duration and oldest success age using `sql/002_ten_minute_monitoring.sql`.
- The current worker schedules the next attempt 10 minutes **after completion**, which does not guarantee a strict 10-minute start-to-start interval. This must be corrected before claiming SLO compliance.
- Horizontal scaling uses PostgreSQL leases; before running multiple instances in production, implement robust lease renewal and ensure jobs are periodically synchronized for newly added sources.

The collector is not yet production-ready. The monitoring query is a diagnostic, not evidence that the 10-minute goal has been achieved.

## Automatic URL onboarding (coordinated app change)

New operator-submitted URLs are immediately confirmed/enabled by the app and scheduled atomically, without manual RSS or an extra enable action. Source-level nullable confirmation/collection fields distinguish these records from legacy country-gated sources. This worker requires app migration `20261016160000_AddSourceCollectionState` before startup. A once-per-minute synchronization creates missing jobs ONLY for explicitly verified/enabled source-level records; it never activates old disabled/pending/rejected sources. RSS/Atom discovery and periodic ingestion keep their results in the existing job table.

`PublicFeedHttp.cs` implements HTTPS-only, connection-time DNS/IP pinning, normal TLS validation, bounded validated redirects, no environment proxy, bounded body and request deadlines including streaming. Whole jobs have a 60-second maximum budget inside the 90-second lease; source eligibility is checked again before committing items. Timeouts now enter per-source backoff rather than leaving due jobs in a retry loop. Valid empty feeds are accepted. This addresses the relevant older prototype gaps; per-host politeness/conditional requests, publisher access policy and broader operational SLO work remain separate.

Defaults: Collector:RequestTimeoutSeconds=12, Collector:JobTimeoutSeconds=60 (may only be shortened). Existing concurrency/interval configuration is retained. No database changes or deployment occur automatically from this repository's startup; use the app's additive migration and the existing staging schema setup.

Cross-repository integration checks are in the app's tests/AutomaticRssChecks, with a controlled HTTP fixture and fresh localhost PostgreSQL. See the app's docs/automatic-source-rss.md for semantics, migration/publication ordering and rollback limitations. The old sql/001_collector_jobs.sql is an initial setup script, not a periodic backfill; do not use it to bulk-enable historical sources.

.NET streaming timeout rationale: https://learn.microsoft.com/en-us/dotnet/api/system.net.http.httpcompletionoption
