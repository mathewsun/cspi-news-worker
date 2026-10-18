using Cspi.NewsCollector;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHttpClient("feeds", c =>
{
    c.Timeout = Timeout.InfiniteTimeSpan; // Per-request budget includes streamed body in PublicFeedHttp.
    c.DefaultRequestHeaders.UserAgent.ParseAdd("CSPI-NewsCollector/0.1 (+contact: admin@cspi-global.ru)");
}).ConfigurePrimaryHttpMessageHandler(PublicFeedNetwork.CreateHandler);
builder.Services.AddHostedService<CollectorService>();
builder.Logging.AddConsole();
await builder.Build().RunAsync();
