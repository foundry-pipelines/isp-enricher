using Confluent.Kafka;
using IspEnricher.Models.In;
using IspEnricher.Models.Out;
using System.Text.Json;
using System.Net;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Isp>();
builder.Build().Run();

// Packets -> IspEnrichment: the ISP of host_ip only, keyed by guid (the Join processor puts packets and enrichments together).
// Reads Foundry:Input and writes Foundry:Output; a message it can't handle is logged and skipped.
sealed class Isp(IConfiguration config, IHostEnvironment env, ILogger<Isp> log) : BackgroundService
{
    const string TypeHeader = "foundry-type"; // tells apart the records of a topic carrying several types

    readonly List<(IPNetwork Network, string Isp)> networks = (config.GetSection("Isp:Ranges").Get<Dictionary<string, string>>() ?? [])
        .Select(r => (IPNetwork.Parse(r.Key), r.Value)).OrderByDescending(r => r.Item1.PrefixLength).ToList();

    protected override Task ExecuteAsync(CancellationToken ct) => Task.Run(() => Run(ct), ct);

    async Task Run(CancellationToken ct)
    {
        var input = config.GetSection("Foundry:Input");
        var output = config.GetSection("Foundry:Output");
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig(Kafka(input))
        {
            GroupId = $"{config["Foundry:Pipeline"] ?? env.EnvironmentName}.{config["Foundry:Processor"] ?? "Isp"}", // locally the environment (prod, load); deployed, the pipeline sets both
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoOffsetStore = false, // stored only after the produce succeeds
        }).Build();
        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig(Kafka(output)) { EnableIdempotence = true }).Build();
        consumer.Subscribe(input["Topic"]);
        log.LogInformation("{Input} -> {Output}", input["Topic"], output["Topic"]);

        while (!ct.IsCancellationRequested)
        {
            ConsumeResult<byte[], byte[]> result;
            try
            {
                result = consumer.Consume(ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException e) when (!e.Error.IsFatal)
            {
                log.LogWarning("{Reason}", e.Error.Reason); // e.g. the topic doesn't exist yet
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
                continue;
            }

            IspEnrichment value;
            try
            {
                value = Process(Read<Packets>(result.Message.Value));
            }
            catch (FormatException e)
            {
                log.LogWarning(e, "Skipping bad message at {Offset}", result.TopicPartitionOffset);
                consumer.StoreOffset(result);
                continue;
            }
            var headers = new Headers { { TypeHeader, System.Text.Encoding.UTF8.GetBytes(nameof(IspEnrichment)) } };
            await producer.ProduceAsync(output["Topic"], new Message<byte[], byte[]> { Key = result.Message.Key, Value = JsonSerializer.SerializeToUtf8Bytes(value), Headers = headers }, ct);
            consumer.StoreOffset(result);
        }
        consumer.Close();
    }

    // A host_ip that isn't an address throws FormatException: skipped.
    IspEnrichment Process(Packets p)
    {
        var host = IPAddress.TryParse(p.HostIp, out var ip) ? ip : throw new FormatException($"host_ip {p.HostIp}: not an IP address");
        return new()
        {
            Guid = p.Guid,
            ISP = networks.FirstOrDefault(n => n.Network.Contains(host)).Isp ?? "unknown",
        };
    }

    // Bad JSON throws FormatException: skipped.
    static T Read<T>(byte[] value)
    {
        try
        {
            return JsonSerializer.Deserialize<T>(value) ?? throw new FormatException("JSON null");
        }
        catch (JsonException e)
        {
            throw new FormatException(e.Message, e);
        }
    }

    // ConnectionSettings from the profile; credentials from Secrets:<SecretRef>:Username/Password (env Secrets__<ref>__Username).
    ClientConfig Kafka(IConfigurationSection dataSet)
    {
        var settings = dataSet.GetSection("ConnectionSettings");
        var client = new ClientConfig { BootstrapServers = settings["Brokers"] };
        if (settings["SecurityProtocol"] is { Length: > 0 } protocol)
            client.Set("security.protocol", protocol);
        if (settings["SaslMechanism"] is { Length: > 0 } mechanism)
            client.Set("sasl.mechanism", mechanism);
        if (settings["SecretRef"] is { Length: > 0 } secret)
        {
            client.SaslUsername = config[$"Secrets:{secret}:Username"];
            client.SaslPassword = config[$"Secrets:{secret}:Password"];
        }
        return client;
    }
}
