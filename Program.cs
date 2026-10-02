using System.Net;
using Foundry.Common.Models;
using Foundry.Common.Transformers;

FoundryTransform.Run<Packets, EnrichedPackets>(args, config =>
{
    // Isp:Ranges maps CIDR ranges to ISP names; the most specific match wins.
    var ranges = config.GetSection("Isp:Ranges").Get<Dictionary<string, string>>() ?? [];
    var networks = ranges.Select(r => (Network: IPNetwork.Parse(r.Key), Isp: r.Value)).OrderByDescending(r => r.Network.PrefixLength).ToList();

    return p => new EnrichedPackets
    {
        Guid = p.Guid,
        Data = p.Data,
        TimeSent = p.TimeSent,
        HostIp = p.HostIp,
        TargetIp = p.TargetIp,
        ISP = networks.FirstOrDefault(n => n.Network.Contains(p.HostIp)).Isp ?? "unknown",
    };
});
