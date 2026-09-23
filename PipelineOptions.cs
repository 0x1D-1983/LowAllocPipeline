namespace LowAllocPipeline;

public sealed class PipelineOptions
{
    public string BootstrapServers { get; init; } = "localhost:9092";
    public string GroupId { get; init; } = "low-alloc-consumer";
    public string Topic { get; init; } = "events";
    public int ChannelCapacity { get; init; } = 10_000;   // bounded -> backpressure
    public int BatchSize { get; init; } = 500;
    public TimeSpan BatchLinger { get; init; } = TimeSpan.FromMilliseconds(50);
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

    /// <summary>
    /// When false, the pipeline does not create a Kafka consumer. Messages are
    /// supplied via <see cref="ILowAllocPipeline.EnqueueAsync"/> (benchmarks).
    /// </summary>
    public bool EnableKafka { get; init; } = true;
}