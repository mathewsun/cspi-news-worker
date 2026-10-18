using System.Net;
using System.Net.Sockets;

namespace Cspi.NewsCollector;

public static class PublicFeedNetwork
{
    public static void ValidateUri(Uri uri)
    {
        var host=uri.IdnHost.ToLowerInvariant();if(host.StartsWith("www."))host=host[4..];if(host.StartsWith("m."))host=host[2..];
        if(host is "youtube.com" or "youtu.be" or "t.me" or "instagram.com" or "facebook.com" or "tiktok.com" or "x.com" or "twitter.com")
            throw new System.Security.SecurityException("Social platform URLs require the social collector");
        if(uri.Scheme!="https" || uri.UserInfo.Length!=0 || uri.Host.Length==0 || uri.Host.Contains('%')
            || uri.IsLoopback || (!uri.Host.Contains('.') && uri.HostNameType==UriHostNameType.Dns)
            || new[]{".local",".localhost",".internal"}.Any(s=>uri.Host.EndsWith(s,StringComparison.OrdinalIgnoreCase)))
            throw new System.Security.SecurityException("Only public HTTPS URLs without credentials are allowed");
        if(IPAddress.TryParse(uri.Host.Trim('[',']'),out var ip) && !IsPublic(ip))
            throw new System.Security.SecurityException("Non-public network address is not allowed");
    }

    public static bool IsPublic(IPAddress ip)
    {
        if(ip.IsIPv4MappedToIPv6)ip=ip.MapToIPv4();
        var b=ip.GetAddressBytes();
        if(b.Length==16) return b[0]>=0x20&&b[0]<=0x3f && !(b[0]==0x20&&b[1]==2)
            && !(b[0]==0x20&&b[1]==1&&(b[2]<2||(b[2]==0x0d&&b[3]==0xb8)));
        return !(ip.Equals(IPAddress.Parse("168.63.129.16")) || b[0] is 0 or 10 or 127 || b[0]>=224 || (b[0]==100&&b[1]>=64&&b[1]<=127)
            || (b[0]==169&&b[1]==254)||(b[0]==172&&b[1]>=16&&b[1]<=31)
            || (b[0]==192&&(b[1]==168||(b[1]==0&&(b[2]==0||b[2]==2))||(b[1]==88&&b[2]==99)))
            || (b[0]==198&&(b[1] is 18 or 19 || (b[1]==51&&b[2]==100)))
            || (b[0]==203&&b[1]==0&&b[2]==113));
    }

    public static async ValueTask<Stream> ConnectAsync(string host,int port,CancellationToken ct,
        Func<string,CancellationToken,Task<IPAddress[]>>? resolve=null,
        Func<IPAddress,int,CancellationToken,ValueTask<Stream>>? connect=null)
    {
        var addresses=await (resolve??((name,token)=>Dns.GetHostAddressesAsync(name,token)))(host,ct);
        if(addresses.Length==0 || addresses.Any(ip=>!IsPublic(ip)))
            throw new System.Security.SecurityException("DNS returned a non-public address");
        Exception? last=null;
        foreach(var ip in addresses)
        {
            try
            {
                if(connect!=null)return await connect(ip,port,ct);
                var socket=new Socket(ip.AddressFamily,SocketType.Stream,ProtocolType.Tcp){NoDelay=true};
                try { await socket.ConnectAsync(new IPEndPoint(ip,port),ct);return new NetworkStream(socket,ownsSocket:true); }
                catch {socket.Dispose();throw;}
            }
            catch(SocketException ex){last=ex;}
        }
        throw last??new IOException("No public endpoint could be connected");
    }

    public static SocketsHttpHandler CreateHandler()=>new()
    {
        AllowAutoRedirect=false,UseProxy=false,UseCookies=false,
        AutomaticDecompression=DecompressionMethods.None,
        ConnectTimeout=TimeSpan.FromSeconds(8),PooledConnectionLifetime=TimeSpan.FromMinutes(5),
        ConnectCallback=(context,ct)=>ConnectAsync(context.DnsEndPoint.Host,context.DnsEndPoint.Port,ct)
        // TLS/SNI uses the original hostname and normal certificate validation, above pinned socket.
    };
}

public sealed record FeedResponse(Uri Url,byte[] Body);

public sealed class PublicFeedHttp(HttpClient client,TimeSpan? timeout=null) : IDisposable
{
    public void Dispose()=>client.Dispose();
    public async Task<FeedResponse> ReadAsync(string value,int limit,CancellationToken ct)
    {
        if(!Uri.TryCreate(value,UriKind.Absolute,out var uri))throw new InvalidDataException("Invalid feed URL");
        using var requestTimeout=CancellationTokenSource.CreateLinkedTokenSource(ct);
        requestTimeout.CancelAfter(timeout??TimeSpan.FromSeconds(12));
        var token=requestTimeout.Token;
        for(var redirect=0;redirect<=5;redirect++)
        {
            PublicFeedNetwork.ValidateUri(uri);
            using var request=new HttpRequestMessage(HttpMethod.Get,uri){Version=HttpVersion.Version11,VersionPolicy=HttpVersionPolicy.RequestVersionExact};
            using var response=await SendAsync(request,token);
            if((int)response.StatusCode is 301 or 302 or 303 or 307 or 308)
            {
                if(redirect==5 || response.Headers.Location==null)throw new InvalidDataException("Redirect limit exceeded or missing target");
                uri=new Uri(uri,response.Headers.Location);continue; // Validate every target before requesting it.
            }
            response.EnsureSuccessStatusCode();
            if(response.Content.Headers.ContentLength>limit)throw new InvalidDataException("Response exceeds size limit");
            await using var stream=await response.Content.ReadAsStreamAsync(token);
            using var body=new MemoryStream();var buffer=new byte[8192];int read;
            while((read=await stream.ReadAsync(buffer.AsMemory(0,Math.Min(buffer.Length,limit+1-(int)body.Length)),token))>0)
            {
                body.Write(buffer,0,read);
                if(body.Length>limit)throw new InvalidDataException("Response exceeds size limit");
            }
            return new(uri,body.ToArray());
        }
        throw new InvalidDataException("Redirect limit exceeded");
    }

    private async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken ct)
    {
        try {return await client.SendAsync(request,HttpCompletionOption.ResponseHeadersRead,ct);}
        catch(HttpRequestException ex) when(ex.GetBaseException() is System.Security.SecurityException)
        {throw new System.Security.SecurityException("DNS endpoint rejected as non-public",ex);}
        catch(HttpRequestException ex) when(ex.GetBaseException() is SocketException socket
            && socket.SocketErrorCode is SocketError.HostNotFound or SocketError.NoData or SocketError.TryAgain)
        {throw new InvalidDataException("Source DNS resolution failed",ex);}
    }
}
