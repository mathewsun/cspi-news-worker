-- Apply once to the existing CSPI PostgreSQL database before starting workers.
-- No changes to existing source/publication tables.
CREATE TABLE IF NOT EXISTS news_collection_jobs (
    source_id integer PRIMARY KEY REFERENCES "Sources"("Id") ON DELETE CASCADE,
    feed_url text,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz,
    lease_owner text,
    consecutive_failures integer NOT NULL DEFAULT 0,
    last_success_at timestamptz,
    last_error text,
    last_duration_ms integer
);
CREATE INDEX IF NOT EXISTS ix_news_collection_jobs_due
    ON news_collection_jobs (next_attempt_at) WHERE lease_until IS NULL;
CREATE TABLE IF NOT EXISTS news_collected_items (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id integer NOT NULL REFERENCES "Sources"("Id") ON DELETE CASCADE,
    item_url text NOT NULL,
    title text NOT NULL,
    published_at timestamptz,
    collected_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(source_id,item_url)
);
CREATE INDEX IF NOT EXISTS ix_news_collected_items_collected
    ON news_collected_items (collected_at DESC);
-- Existing verified and enabled sources only; one job per unique source,
-- regardless of how many countries reference it.
INSERT INTO news_collection_jobs (source_id)
SELECT DISTINCT n."SourceId"
FROM "NewsSourceCoverages" n
JOIN "Sources" s ON s."Id" = n."SourceId"
WHERE n."CollectionEnabled" AND n."VerificationState" = 'Verified'
  AND s."Url" IS NOT NULL
ON CONFLICT DO NOTHING;
