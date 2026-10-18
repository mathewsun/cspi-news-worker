using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Npgsql;

namespace Cspi.NewsCollector;

/// <summary>
/// Independent worker. PostgreSQL row leasing allows multiple replicas without
/// scheduling the same source simultaneously. RSS/Atom only in this first iteration.
/// </summary>
public sealed class CollectorService(
    IConfiguration config, IHttpClientFactory clients, ILogger<CollectorService> logger) : BackgroundService
{
    private readonly string owner = $"{Environment.MachineName}-{Guid.NewGuid():N}";
    private readonly string connection = config.GetConnectionString("DefaultConnection")
        ?? throw new InvalidOperationException("ConnectionStrings:DefaultConnection is required");
    private readonly int concurrency = Math.Clamp(config.GetValue("Collector:Concurrency", 16), 1, 64);
    private readonly int intervalMinutes = Math.Clamp(config.GetValue("Collector:IntervalMinutes", 10), 5, 60);
    private readonly int jobSeconds=Math.Clamp(config.GetValue("Collector:JobTimeoutSeconds",60),1,60);
    private readonly TimeSpan requestTimeout=TimeSpan.FromSeconds(Math.Clamp(config.GetValue("Collector:RequestTimeoutSeconds",12),1,12));
    public const string Eligibility = """
        EXISTS (SELECT 1 FROM "Sources" s WHERE s."Id"=j.source_id AND s."Status" NOT IN (4,5)
            AND coalesce(to_jsonb(s)->>'CollectionKind','Rss')='Rss'
            AND lower(coalesce(substring(lower(s."Url") from '^https?://(?:www[.]|m[.])?([^/:?#]+)'),'')) NOT IN ('youtube.com','youtu.be','t.me','instagram.com','facebook.com','tiktok.com','x.com','twitter.com')
            AND ((s."CollectionEnabled" IS TRUE AND s."VerificationState"='Verified'
                    AND NOT EXISTS (SELECT 1 FROM "NewsSourceCoverages" c WHERE c."SourceId"=s."Id"
                        AND c."VerificationState" IN ('VerificationFailed','Unavailable','Suspended')))
                OR (s."CollectionEnabled" IS NULL AND EXISTS (SELECT 1 FROM "NewsSourceCoverages" c
                    WHERE c."SourceId"=s."Id" AND c."CollectionEnabled" AND c."VerificationState"='Verified'))))
        """;
    private static readonly string ClaimSql = $$"""
        WITH due AS (
          SELECT j.source_id FROM news_collection_jobs j
          WHERE j.next_attempt_at <= now() AND (j.lease_until IS NULL OR j.lease_until < now())
            AND {{Eligibility}}
          ORDER BY j.next_attempt_at,j.source_id FOR UPDATE SKIP LOCKED LIMIT 1
        )
        UPDATE news_collection_jobs j SET lease_until=now()+interval '90 seconds',lease_owner=@owner
        FROM due WHERE j.source_id=due.source_id
        RETURNING j.source_id,j.feed_url,(SELECT s."Url" FROM "Sources" s WHERE s."Id"=j.source_id)
        """;
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Collector starting owner={Owner} concurrency={Concurrency} interval={Interval}m",
            owner, concurrency, intervalMinutes);
        await SynchronizeAsync(stoppingToken);
        var workers = Enumerable.Range(0, concurrency).Select(_ => Loop(stoppingToken)).Append(SyncLoop(stoppingToken));
        await Task.WhenAll(workers);
    }

    private async Task Loop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if(!await RunOnceAsync(ct)) await Task.Delay(TimeSpan.FromSeconds(3),ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "Collector loop error");
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }

    public async Task<bool> RunOnceAsync(CancellationToken ct=default)
    {
        var job=await Claim(ct);if(job==null)return false;
        await Process(job.Value,ct);return true;
    }
    public async Task SynchronizeAsync(CancellationToken ct=default)
    {
        await using var db=new NpgsqlConnection(connection);await db.OpenAsync(ct);
        await using var cmd=new NpgsqlCommand("""
            INSERT INTO news_collection_jobs (source_id)
            SELECT s."Id" FROM "Sources" s
            WHERE s."CollectionEnabled" IS TRUE AND s."VerificationState"='Verified' AND s."Status" NOT IN (4,5)
              AND coalesce(to_jsonb(s)->>'CollectionKind','Rss')='Rss'
              AND lower(coalesce(substring(lower(s."Url") from '^https?://(?:www[.]|m[.])?([^/:?#]+)'),'')) NOT IN ('youtube.com','youtu.be','t.me','instagram.com','facebook.com','tiktok.com','x.com','twitter.com')
              AND NOT EXISTS (SELECT 1 FROM "NewsSourceCoverages" c WHERE c."SourceId"=s."Id"
                AND c."VerificationState" IN ('VerificationFailed','Unavailable','Suspended'))
            ON CONFLICT (source_id) DO NOTHING
            """,db);
        await cmd.ExecuteNonQueryAsync(ct);
    }
    private async Task SyncLoop(CancellationToken ct)
    {
        while(!ct.IsCancellationRequested)
        {
            try {await Task.Delay(TimeSpan.FromMinutes(1),ct);await SynchronizeAsync(ct);}
            catch(OperationCanceledException) when(ct.IsCancellationRequested){break;}
            catch(Exception ex){logger.LogError(ex,"Automatic source queue synchronization failed");}
        }
    }
    private async Task<(int Id, string? Feed, string? Homepage)?> Claim(CancellationToken ct)
    {
        await using var db = new NpgsqlConnection(connection);
        await db.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(ClaimSql, db);
        cmd.Parameters.AddWithValue("owner", owner);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        return (reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2));
    }

    private async Task Process((int Id, string? Feed, string? Homepage) job, CancellationToken ct)
    {
        var watch = Stopwatch.StartNew();
        using var budget=CancellationTokenSource.CreateLinkedTokenSource(ct);budget.CancelAfter(TimeSpan.FromSeconds(jobSeconds));
        var token=budget.Token;
        try
        {
            var homepage = job.Homepage ?? throw new InvalidDataException("Source URL missing");
            var feed = job.Feed ?? await Discover(homepage, token);
            var items = await DownloadFeed(feed, token);
            await using var db = new NpgsqlConnection(connection);
            await db.OpenAsync(token);
            await using var tx = await db.BeginTransactionAsync(token);
            // Fence before storing items: disabling or expiry cannot commit stale results.
            await using(var fence=new NpgsqlCommand($"SELECT 1 FROM news_collection_jobs j WHERE source_id=@id AND lease_owner=@owner AND lease_until>now() AND ({Eligibility}) FOR UPDATE OF j",db,tx))
            {
                fence.Parameters.AddWithValue("id",job.Id);fence.Parameters.AddWithValue("owner",owner);
                if(await fence.ExecuteScalarAsync(token)==null)return;
            }
            var added = 0;
            foreach (var item in items.Take(100))
            {
                await using var cmd = new NpgsqlCommand("""
                    INSERT INTO news_collected_items (source_id,item_url,title,published_at)
                    VALUES (@id,@url,@title,@date)
                    ON CONFLICT (source_id,item_url) DO NOTHING;
                    """, db, tx);
                cmd.Parameters.AddWithValue("id", job.Id);
                cmd.Parameters.AddWithValue("url", item.Url);
                cmd.Parameters.AddWithValue("title", item.Title);
                cmd.Parameters.AddWithValue("date", (object?)item.PublishedAt ?? DBNull.Value);
                added += await cmd.ExecuteNonQueryAsync(token);
            }
            await using (var cmd = new NpgsqlCommand("""
                UPDATE news_collection_jobs SET feed_url=@feed,
                last_success_at=now(), consecutive_failures=0,last_error=NULL,
                last_duration_ms=@duration, next_attempt_at=now()+(@minutes * interval '1 minute'),
                lease_until=NULL,lease_owner=NULL
                WHERE source_id=@id AND lease_owner=@owner;
                """, db, tx))
            {
                cmd.Parameters.AddWithValue("feed", feed);
                cmd.Parameters.AddWithValue("duration", (int)watch.ElapsedMilliseconds);
                cmd.Parameters.AddWithValue("minutes", intervalMinutes);
                cmd.Parameters.AddWithValue("id", job.Id);
                cmd.Parameters.AddWithValue("owner", owner);
                await cmd.ExecuteNonQueryAsync(token);
            }
            await tx.CommitAsync(token);
            logger.LogInformation("Source={Source} newItems={Count} durationMs={Duration}",
                job.Id, added, watch.ElapsedMilliseconds);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "Source={Source} failed", job.Id);
            using var failureBudget=new CancellationTokenSource(TimeSpan.FromSeconds(10));
            ct=failureBudget.Token;
            await using var db = new NpgsqlConnection(connection);
            await db.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand("""
                UPDATE news_collection_jobs SET
                  consecutive_failures=consecutive_failures+1,
                  last_error=@error, last_duration_ms=@duration,
                  next_attempt_at=now() + (
                    CASE
                      WHEN @missingFeed THEN
                        CASE WHEN consecutive_failures + 1 >= 3 THEN 1440 ELSE 60 END
                      ELSE
                        CASE
                          WHEN consecutive_failures + 1 >= 6 THEN 360
                          WHEN consecutive_failures + 1 >= 3 THEN 60
                          ELSE 15
                        END
                    END * interval '1 minute'),
                  lease_until=NULL, lease_owner=NULL
                WHERE source_id=@id AND lease_owner=@owner;
                """, db);
            cmd.Parameters.AddWithValue("id", job.Id);
            cmd.Parameters.AddWithValue("owner", owner);
            cmd.Parameters.AddWithValue("error", ex.Message[..Math.Min(ex.Message.Length, 500)]);
            cmd.Parameters.AddWithValue("missingFeed", ex is InvalidDataException && ex.Message.StartsWith("No valid RSS/Atom feed", StringComparison.Ordinal));
            cmd.Parameters.AddWithValue("duration", (int)watch.ElapsedMilliseconds);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        finally
        {
            using var cleanup=new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await using var db=new NpgsqlConnection(connection);await db.OpenAsync(cleanup.Token);
            await using var release=new NpgsqlCommand("UPDATE news_collection_jobs SET lease_until=NULL,lease_owner=NULL WHERE source_id=@id AND lease_owner=@owner",db);
            release.Parameters.AddWithValue("id",job.Id);release.Parameters.AddWithValue("owner",owner);
            await release.ExecuteNonQueryAsync(cleanup.Token);
        }
    }

    private async Task<string> Discover(string homepage, CancellationToken ct)
    {
        PublicFeedNetwork.ValidateUri(new Uri(homepage));
        using var http = new PublicFeedHttp(clients.CreateClient("feeds"),requestTimeout);
        try {await DownloadFeed(homepage,ct);return homepage;}
        catch(OperationCanceledException) when(ct.IsCancellationRequested){throw;}
        catch(System.Security.SecurityException){throw;}
        catch(InvalidDataException ex) when(ex.Message=="Source DNS resolution failed"){throw;}
        catch(Exception ex){logger.LogDebug(ex,"Source URL is not a feed");}
        var candidates = new List<string>();
        var root = new Uri(new Uri(homepage).GetLeftPart(UriPartial.Authority));

        // HTML attribute order is not significant. Inspect each link element
        // independently rather than assuming rel/type precede href.
        async Task InspectPage(string page)
        {
            var response=await http.ReadAsync(page,1_000_000,ct);
            page=response.Url.AbsoluteUri;
            var html=System.Text.Encoding.UTF8.GetString(response.Body);
            foreach (Match tag in Regex.Matches(html, @"<link\b[^>]*>", RegexOptions.IgnoreCase,TimeSpan.FromSeconds(1)))
            {
                ct.ThrowIfCancellationRequested();
                var attrs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (Match attr in Regex.Matches(tag.Value,
                    """([a-zA-Z][\w:-]*)\s*=\s*(?:"([^"]*)"|'([^']*)')""",RegexOptions.None,TimeSpan.FromSeconds(1)))
                    attrs[attr.Groups[1].Value] = WebUtility.HtmlDecode(
                        attr.Groups[2].Success ? attr.Groups[2].Value : attr.Groups[3].Value);

                if (!attrs.TryGetValue("href", out var href) ||
                    !attrs.TryGetValue("rel", out var rel) ||
                    !rel.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                        .Contains("alternate", StringComparer.OrdinalIgnoreCase) ||
                    !attrs.TryGetValue("type", out var type) ||
                    !(type.Contains("rss+xml", StringComparison.OrdinalIgnoreCase) ||
                      type.Contains("atom+xml", StringComparison.OrdinalIgnoreCase)))
                    continue;

                if (Uri.TryCreate(new Uri(page), href, out var resolved))
                    candidates.Add(resolved.AbsoluteUri);
            }
        }

        foreach (var page in new[] { homepage, new Uri(root, "/news").AbsoluteUri,
                                      new Uri(root, "/feeds").AbsoluteUri })
        {
            try { await InspectPage(page); }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch(System.Security.SecurityException){throw;}
            catch (Exception ex) { logger.LogDebug(ex, "RSS discovery page failed: {Page}", page); }
        }

        foreach (var path in new[] {
            "/rss/", "/rss", "/feed/", "/feeds/", "/rss.xml", "/feed.xml",
            "/atom.xml", "/index.xml", "/rss.php", "/feeds/posts/default",
            "/news/rss.xml", "/news/rss", "/world/rss"
        })
            candidates.Add(new Uri(root, path).AbsoluteUri);

        foreach (var candidate in candidates.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            try
            {
                await DownloadFeed(candidate,ct);return candidate;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch(System.Security.SecurityException){throw;}
            catch (Exception ex) { logger.LogDebug(ex, "RSS candidate rejected: {Candidate}", candidate); }
        }

        throw new InvalidDataException("No valid RSS/Atom feed found on homepage or conventional paths");
    }

    private async Task<List<(string Url, string Title, DateTime? PublishedAt)>> DownloadFeed(string feed, CancellationToken ct)
    {
        using var http=new PublicFeedHttp(clients.CreateClient("feeds"),requestTimeout);
        var response=await http.ReadAsync(feed,2_000_000,ct);
        using var stream=new MemoryStream(response.Body);
        using var reader=System.Xml.XmlReader.Create(stream,new System.Xml.XmlReaderSettings
            {Async=true,DtdProcessing=System.Xml.DtdProcessing.Prohibit,XmlResolver=null,MaxCharactersInDocument=2_000_000});
        var xml=await XDocument.LoadAsync(reader,LoadOptions.None,ct);
        if(xml.Root?.Name.LocalName is not ("rss" or "feed" or "RDF"))throw new InvalidDataException("Not an RSS/Atom document");
        var result = new List<(string, string, DateTime?)>();
        foreach (var node in xml.Descendants().Where(x => x.Name.LocalName is "item" or "entry").Take(100))
        {
            var title = node.Elements().FirstOrDefault(x => x.Name.LocalName == "title")?.Value?.Trim();
            var link = node.Elements().FirstOrDefault(x => x.Name.LocalName == "link");
            var url = link?.Attribute("href")?.Value ?? link?.Value;
            if (string.IsNullOrWhiteSpace(title) || string.IsNullOrWhiteSpace(url) ||
                !Uri.TryCreate(url, UriKind.Absolute, out var uri) || uri.Scheme != "https") continue;
            var date = node.Elements().FirstOrDefault(x => x.Name.LocalName is "pubDate" or "published" or "updated")?.Value;
            DateTime? published = DateTimeOffset.TryParse(date, out var parsed) ? parsed.UtcDateTime : null;
            result.Add((uri.AbsoluteUri, title[..Math.Min(title.Length, 1000)], published));
        }
        return result;
    }

}
