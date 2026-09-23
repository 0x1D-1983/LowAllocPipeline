using Confluent.Kafka;

namespace LowAllocPipeline;

/// <summary>
/// Shared surface for the hand-rolled Channel pipeline and the TPL Dataflow
/// pipeline. Production code calls <see cref="Start"/> (Kafka poll + process).
/// Benchmarks call <see cref="StartProcessing"/> and feed payloads in-process
/// via <see cref="EnqueueAsync"/> so we measure the pipeline, not the broker.
/// </summary>
public interface ILowAllocPipeline : IAsyncDisposable
{
    void Start();

    /// <summary>
    /// Starts downstream processing without connecting to Kafka.
    /// </summary>
    void StartProcessing();

    ValueTask EnqueueAsync(
        ReadOnlyMemory<byte> payload,
        TopicPartitionOffset tpo,
        CancellationToken ct = default);

    void CompleteIngest();

    Task Completion { get; }
}
