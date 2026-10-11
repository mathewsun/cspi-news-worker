-- Collector operational view: measure coverage and whether 10-minute target is met.
-- This counts UNIQUE source jobs, not country-source links.
-- A source with no successful check is NOT counted as fresh.
SELECT
    count(*) AS total_jobs,
    count(*) FILTER (WHERE last_success_at >= now() - interval '10 minutes') AS fresh_last_10m,
    count(*) FILTER (WHERE last_success_at IS NULL) AS never_successful,
    count(*) FILTER (WHERE last_success_at < now() - interval '10 minutes') AS stale_last_10m,
    count(*) FILTER (WHERE next_attempt_at <= now()) AS due_jobs,
    count(*) FILTER (WHERE lease_until > now()) AS active_leases,
    round(avg(last_duration_ms)::numeric, 1) AS mean_duration_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY last_duration_ms) AS p95_duration_ms,
    max(now() - last_success_at) AS oldest_success_age
FROM news_collection_jobs;

-- Oldest stale jobs first: useful to identify sites consistently missing the target.
SELECT j.source_id, s."Name", s."Url", j.last_success_at,
       j.next_attempt_at, j.consecutive_failures, j.last_error,
       j.last_duration_ms
FROM news_collection_jobs j
JOIN "Sources" s ON s."Id" = j.source_id
WHERE j.last_success_at IS NULL OR j.last_success_at < now() - interval '10 minutes'
ORDER BY j.last_success_at NULLS FIRST, j.next_attempt_at
LIMIT 100;
