using Confluent.Kafka;
using Foundry.Models;
using Foundry.Models.Configuration;
using Foundry.Models.Serialization;

/// <summary>Consumes Input, applies the transform and produces to Output. Offsets are stored only after the produce succeeds.</summary>
sealed class TransformWorker<TIn, TOut>(FoundryOptions foundry, IConfiguration config, ILogger<TransformWorker<TIn, TOut>> log, Func<TIn, TOut> transform)
    : BackgroundService where TIn : IFoundryModel where TOut : IFoundryModel
{
    protected override Task ExecuteAsync(CancellationToken ct) => Task.Run(() => RunAsync(ct), ct);

    async Task RunAsync(CancellationToken ct)
    {
        var consumerConfig = new ConsumerConfig(Client(foundry.Input))
        {
            GroupId = $"{foundry.Pipeline}.{foundry.Transform}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoOffsetStore = false,
        };
        using var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig(Client(foundry.Output)) { EnableIdempotence = true }).Build();

        consumer.Subscribe(foundry.Input.Topic);
        log.LogInformation("{Transform}: {Input} -> {Output}", foundry.Transform, foundry.Input.Topic, foundry.Output.Topic);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<byte[], byte[]> result;
                try
                {
                    result = consumer.Consume(ct);
                }
                catch (ConsumeException e) when (!e.Error.IsFatal)
                {
                    // E.g. the topic doesn't exist yet: keep polling.
                    log.LogWarning("{Reason}", e.Error.Reason);
                    await Task.Delay(TimeSpan.FromSeconds(5), ct);
                    continue;
                }
                byte[] value;
                try
                {
                    value = FoundrySerializer.Serialize(transform(FoundrySerializer.Deserialize<TIn>(result.Message.Value)));
                }
                catch (FormatException e)
                {
                    log.LogWarning(e, "Skipping bad message at {Offset}", result.TopicPartitionOffset);
                    consumer.StoreOffset(result);
                    continue;
                }
                await producer.ProduceAsync(foundry.Output.Topic, new Message<byte[], byte[]> { Key = result.Message.Key, Value = value }, ct);
                consumer.StoreOffset(result);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        finally
        {
            consumer.Close();
        }
    }

    // Credentials come from configuration under Secrets:<SecretRef>, e.g. env Secrets__kafka-internal-creds__Username.
    ClientConfig Client(DataSetOptions dataSet)
    {
        var settings = dataSet.ConnectionSettings;
        var client = new ClientConfig { BootstrapServers = settings.Brokers };
        if (settings.SecurityProtocol is { Length: > 0 } protocol)
            client.Set("security.protocol", protocol);
        if (settings.SaslMechanism is { Length: > 0 } mechanism)
            client.Set("sasl.mechanism", mechanism);
        if (settings.SecretRef is { Length: > 0 } secret)
        {
            client.SaslUsername = config[$"Secrets:{secret}:Username"];
            client.SaslPassword = config[$"Secrets:{secret}:Password"];
        }
        return client;
    }
}
