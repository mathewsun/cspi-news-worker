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
