using System.Text.Json.Serialization;

namespace IspEnricher.Models.Out;

/// <summary>The ISP of a packet's host_ip, keyed by its guid.</summary>
public sealed class IspEnrichment
{
    [JsonPropertyName("guid")]
    public Guid Guid { get; set; }

    [JsonPropertyName("ISP")]
    public string ISP { get; set; } = "";
}
