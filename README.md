# CSPI News Worker

Independent .NET 10 RSS/Atom collection worker for the CSPI project.

This is a **development prototype**, migrated from [CSPI PR #40](https://github.com/mathewsun/cspi/pull/40). It is not production-ready and is not running on the server.

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
