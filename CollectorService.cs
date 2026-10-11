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
    private const string ClaimSql = """
        WITH due AS (
          SELECT j.source_id FROM news_collection_jobs j
          WHERE j.next_attempt_at <= now()
            AND (j.lease_until IS NULL OR j.lease_until < now())
            AND EXISTS (
              SELECT 1 FROM "NewsSourceCoverages" c
              WHERE c."SourceId"=j.source_id AND c."CollectionEnabled"
                AND c."VerificationState"='Verified'
            )
          ORDER BY j.next_attempt_at, j.source_id
          FOR UPDATE SKIP LOCKED LIMIT 1
        )
        UPDATE news_collection_jobs j
        SET lease_until=now()+interval '90 seconds', lease_owner=@owner
        FROM due WHERE j.source_id=due.source_id
        RETURNING j.source_id, j.feed_url,
          (SELECT s."Url" FROM "Sources" s WHERE s."Id"=j.source_id);
        """;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Collector starting owner={Owner} concurrency={Concurrency} interval={Interval}m",
            owner, concurrency, intervalMinutes);
        var workers = Enumerable.Range(0, concurrency).Select(_ => Loop(stoppingToken));
        await Task.WhenAll(workers);
    }

    private async Task Loop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var job = await Claim(ct);
                if (job is null) { await Task.Delay(TimeSpan.FromSeconds(3), ct); continue; }
                await Process(job.Value, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "Collector loop error");
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
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
        try
        {
            var homepage = job.Homepage ?? throw new InvalidDataException("Source URL missing");
            var feed = job.Feed ?? await Discover(homepage, ct);
            var items = await DownloadFeed(feed, ct);
            await using var db = new NpgsqlConnection(connection);
            await db.OpenAsync(ct);
            await using var tx = await db.BeginTransactionAsync(ct);
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
                added += await cmd.ExecuteNonQueryAsync(ct);
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
                await cmd.ExecuteNonQueryAsync(ct);
            }
            await tx.CommitAsync(ct);
            logger.LogInformation("Source={Source} newItems={Count} durationMs={Duration}",
                job.Id, added, watch.ElapsedMilliseconds);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Source={Source} failed", job.Id);
            await using var db = new NpgsqlConnection(connection);
            await db.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand("""
                UPDATE news_collection_jobs SET
                  consecutive_failures=consecutive_failures+1,
                  last_error=@error, last_duration_ms=@duration,
                  next_attempt_at=now()+interval '15 minutes',
                  lease_until=NULL, lease_owner=NULL
                WHERE source_id=@id AND lease_owner=@owner;
                """, db);
            cmd.Parameters.AddWithValue("id", job.Id);
            cmd.Parameters.AddWithValue("owner", owner);
            cmd.Parameters.AddWithValue("error", ex.Message[..Math.Min(ex.Message.Length, 500)]);
            cmd.Parameters.AddWithValue("duration", (int)watch.ElapsedMilliseconds);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    private async Task<string> Discover(string homepage, CancellationToken ct)
    {
        await ValidatePublicUrl(homepage, ct);
        var http = clients.CreateClient("feeds");
        using var response = await http.GetAsync(homepage, HttpCompletionOption.ResponseHeadersRead, ct);
        response.EnsureSuccessStatusCode();
        var html = await response.Content.ReadAsStringAsync(ct);
        if (html.Length > 1_000_000) html = html[..1_000_000];
        var match = Regex.Match(html,
            """<link\\b(?=[^>]*rel=["']alternate["'])(?=[^>]*type=["']application/(?:rss|atom)\\+xml["'])[^>]*href=["']([^"']+)["']""",
            RegexOptions.IgnoreCase);
        if (!match.Success) throw new InvalidDataException("No RSS/Atom link found on homepage");
        var resolved = new Uri(new Uri(homepage), WebUtility.HtmlDecode(match.Groups[1].Value)).AbsoluteUri;
        await ValidatePublicUrl(resolved, ct);
        return resolved;
    }

    private async Task<List<(string Url, string Title, DateTime? PublishedAt)>> DownloadFeed(string feed, CancellationToken ct)
    {
        await ValidatePublicUrl(feed, ct);
        var http = clients.CreateClient("feeds");
        using var response = await http.GetAsync(feed, HttpCompletionOption.ResponseHeadersRead, ct);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(ct);
        using var limited = new LimitedStream(stream, 2_000_000);
        var xml = await XDocument.LoadAsync(limited, LoadOptions.None, ct);
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

    private static async Task ValidatePublicUrl(string value, CancellationToken ct)
    {
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri) || uri.Scheme != "https" ||
            uri.UserInfo.Length > 0) throw new InvalidDataException("Only public HTTPS feeds are supported");
        var addresses = await Dns.GetHostAddressesAsync(uri.DnsSafeHost, ct);
        if (addresses.Length == 0 || addresses.Any(ip =>
            IPAddress.IsLoopback(ip) || ip.IsIPv6LinkLocal || ip.IsIPv6SiteLocal ||
            ip.IsIPv6Multicast || ip.Equals(IPAddress.Any) || ip.Equals(IPAddress.IPv6Any) ||
            (ip.AddressFamily == AddressFamily.InterNetwork &&
              (ip.GetAddressBytes()[0] is 0 or 10 or 127 ||
               (ip.GetAddressBytes()[0] == 169 && ip.GetAddressBytes()[1] == 254) ||
               (ip.GetAddressBytes()[0] == 172 && ip.GetAddressBytes()[1] is >= 16 and <= 31) ||
               (ip.GetAddressBytes()[0] == 192 && ip.GetAddressBytes()[1] == 168)))))
            throw new InvalidDataException("Private or non-public host is not allowed");
    }

    private sealed class LimitedStream(Stream inner, long maxBytes) : Stream
    {
        private long read;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => read; set => throw new NotSupportedException(); }
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            var n = await inner.ReadAsync(buffer[..(int)Math.Min(buffer.Length, maxBytes + 1 - read)], ct);
            read += n;
            if (read > maxBytes) throw new InvalidDataException("Feed exceeds size limit");
            return n;
        }
        public override int Read(byte[] buffer, int offset, int count) =>
            ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();
        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
