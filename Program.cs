using System.Net;
using Foundry.Models;
using Foundry.Models.Configuration;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { Args = args, ContentRootPath = AppContext.BaseDirectory });
var foundry = builder.Configuration.GetSection(FoundryOptions.SectionName).Get<FoundryOptions>()
    ?? throw new InvalidOperationException("Missing Foundry configuration");
foundry.EnsureSchemas<Packets, EnrichedPackets>();

// Isp:Ranges maps CIDR ranges to ISP names; the most specific match wins.
var ranges = builder.Configuration.GetSection("Isp:Ranges").Get<Dictionary<string, string>>() ?? [];
var networks = ranges.Select(r => (Network: IPNetwork.Parse(r.Key), Isp: r.Value)).OrderByDescending(r => r.Network.PrefixLength).ToList();

builder.Services.AddHostedService(sp => new TransformWorker<Packets, EnrichedPackets>(
    foundry, builder.Configuration, sp.GetRequiredService<ILogger<TransformWorker<Packets, EnrichedPackets>>>(), Enrich));
builder.Build().Run();

EnrichedPackets Enrich(Packets p) => new()
{
    Guid = p.Guid,
    Data = p.Data,
    TimeSent = p.TimeSent,
    HostIp = p.HostIp,
    TargetIp = p.TargetIp,
    ISP = networks.FirstOrDefault(n => n.Network.Contains(p.HostIp)).Isp ?? "unknown",
};
