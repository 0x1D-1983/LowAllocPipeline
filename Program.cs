// ============================================================================
// Low-Allocation Kafka Consumer Pipeline (C# / .NET 8+)
//
// Pattern:
//   Kafka poll loop (dedicated thread) -> Channel<T> (bounded, backpressure)
//   -> batch aggregator -> parallel processing -> batch commit to Kafka
//
// Goals:
//   - Avoid per-message heap allocations on the hot path (pooled buffers,
//     struct-based messages, no LINQ, no boxing).
//   - Decouple I/O (Kafka poll/commit) from CPU-bound processing via a
//     bounded channel, so a slow consumer applies backpressure instead of
//     unbounded memory growth.
//   - Commit offsets in batches, not per-message, to cut broker round trips
//     and avoid an allocation-per-commit.
//
// NuGet: Confluent.Kafka
// ============================================================================

namespace LowAllocPipeline;

public static class Program
{
    public static async Task Main()
    {
        var pipeline = new KafkaLowAllocPipeline(new PipelineOptions
        {
            BootstrapServers = "localhost:9092",
            GroupId = "orders-processor",
            Topic = "orders",
            ChannelCapacity = 20_000,
            BatchSize = 1_000,
            BatchLinger = TimeSpan.FromMilliseconds(25),
        });

        pipeline.Start();

        // Keep running until shutdown signal (Ctrl+C, K8s SIGTERM, etc.)
        var shutdown = new TaskCompletionSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; shutdown.TrySetResult(); };
        await shutdown.Task;

        await pipeline.DisposeAsync();
    }
}