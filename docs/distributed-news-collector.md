# Distributed CSPI news collector — experimental foundation

**Do not deploy this branch to production yet.** The new worker is deliberately not added to compose.yaml and does not run automatically. The existing web app is unchanged.

## Intended topology

- Dedicated .NET 10 Worker container(s), independently scaled from the web application.
- Shared PostgreSQL `news_collection_jobs` with row leasing via `FOR UPDATE SKIP LOCKED`.
- One job per Source, even if the same source covers several countries.
- Configurable worker concurrency (default 16, maximum 64) and revisit interval (default 10 minutes).
- First pass supports explicitly linked RSS/Atom feeds only, not arbitrary HTML scraping.
- Items are written to `news_collected_items` with unique (source_id, item_url).
- Per-source timings, consecutive failures, last success and error are stored.

## Estimated load, not benchmark results

1591 sources / 300 s = 5.3 source checks/s; /600 s = 2.7/s.
At mean request duration 3 s, ideal concurrency ~16 (5-minute target), before discovery requests, retries, CPU, database and network variability.
10000 sources require 33.3 checks/s for 5 minutes; 100000 require 333.3/s.
Actual throughput must be measured on the deployment network with the real source mix.

## Missing production requirements

1. **SSRF safety**: current DNS validation is preliminary. Pin resolved public IPs at connection time, block redirect chains and private IPv4/IPv6 ranges, and prevent DNS rebinding. Restrict outbound networking.
2. **Feed discovery**: HTML link parsing is incomplete and many publishers have no feed. Add explicit per-source feed URLs and review discovery results.
3. **Scheduling**: SQL setup script is manual, not an EF migration; newly added sources need periodic upsert into the job table.
4. **Leases**: 90-second leases require renewal or timeouts below lease duration; avoid duplicate processing after lease expiry. Implement graceful cancellation.
5. **Error policy**: classify HTTP status and parsing errors, backoff with jitter, only mark Unavailable after repeated confirmed failures; respect 429 and Retry-After.
6. **Publication integration**: current prototype writes a separate staging table, not the CSPI Publications table. Add vetted promotion pipeline and source-country mapping.
7. **Politeness/compliance**: robots.txt, per-host throttling, publisher usage rights, identification and contact details, conditional GET/ETag, bandwidth limits.
8. **Operations**: metrics, queue age, p95 duration, source coverage, dead-letter handling, admin dashboard, tests, staging load tests, resource caps.
9. **Security**: database credentials via secrets, no hardcoded credentials, safe logs, read-only network constraints.
10. **Build verification**: this branch has not been compiled or integration tested.

## Prototype commands (development only)

Apply `Cspi.NewsCollector/sql/001_collector_jobs.sql` manually to a **test database**. Then run the standalone worker with `ConnectionStrings__DefaultConnection` set. Do not start against production until the missing requirements are resolved.

The worker does not change `NewsSourceCoverages.VerificationState` or `CollectionEnabled` on failure. Failures are only recorded in the job table.
