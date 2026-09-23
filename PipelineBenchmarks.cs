using System.Buffers;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

namespace LowAllocPipeline;

/// <summary>
/// In-process comparison of the hand-rolled Channel pipeline vs the TPL
/// Dataflow pipeline. Kafka I/O is excluded: payloads are pre-generated and
/// fed through <see cref="ILowAllocPipeline.EnqueueAsync"/>.
///
/// Run with: <c>dotnet run -c Release</c>
/// </summary>
[RankColumn]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[MemoryDiagnoser]
[WarmupCount(8)]
[MinIterationCount(15)]
[MinIterationTime(250)]
public class PipelineBenchmarks
{
    public const int MessageCount = 50_000;

    // IterationSetup forces InvocationCount=1, so one 50k feed is only
    // ~30–55ms. Replay the same payloads so a single invoke stays ≥250ms
    // and TPL's Post vs SendAsync backpressure path is hit every time.
    public const int Passes = 8;
    public const int TotalMessages = MessageCount * Passes;

    private byte[][] _payloads = null!;
    private TopicPartitionOffset[] _tpos = null!;

    private KafkaLowAllocPipeline? _handRolled;
    private KafkaLowAllocPipelineTpl? _tpl;

    [GlobalSetup]
    public void GlobalSetup()
    {
        WarmThreadPool();

        _payloads = new byte[MessageCount][];
        _tpos = new TopicPartitionOffset[MessageCount];
        var partition = new TopicPartition("orders", 0);

        for (var i = 0; i < MessageCount; i++)
        {
            _payloads[i] = SerializeOrder(i);
            _tpos[i] = new TopicPartitionOffset(partition, i);
        }

        var pool = new DefaultObjectPool<OrderEvent>(new OrderEventPooledPolicy());
        var sample = new OrderEventValidator(pool).Validate(_payloads[0], _tpos[0]);
        if (sample.Outcome != ValidationOutcome.Valid)
            throw new InvalidOperationException($"Benchmark payload is not valid: {sample.Reason}");
    }

    [IterationSetup(Target = nameof(HandRolledChannel))]
    public void SetupHandRolled()
    {
        _handRolled = new KafkaLowAllocPipeline(CreateOptions());
        _handRolled.StartProcessing();
    }

    [IterationCleanup(Target = nameof(HandRolledChannel))]
    public void CleanupHandRolled()
    {
        DisposePipeline(_handRolled);
        _handRolled = null;
    }

    [IterationSetup(Target = nameof(TplDataflow))]
    public void SetupTpl()
    {
        _tpl = new KafkaLowAllocPipelineTpl(CreateOptions());
        _tpl.StartProcessing();
    }

    [IterationCleanup(Target = nameof(TplDataflow))]
    public void CleanupTpl()
    {
        DisposePipeline(_tpl);
        _tpl = null;
    }

    /// <summary>
    /// Hand-rolled bounded <see cref="System.Threading.Channels.Channel{T}"/>
    /// + manual batch/linger/commit loop.
    /// </summary>
    [Benchmark(Baseline = true, OperationsPerInvoke = TotalMessages)]
    public Task HandRolledChannel() => FeedAndCompleteAsync(_handRolled!);

    /// <summary>
    /// Same stages expressed as a TPL Dataflow block graph
    /// (BufferBlock → TransformBlock → BatchBlock → ActionBlock).
    /// </summary>
    [Benchmark(OperationsPerInvoke = TotalMessages)]
    public Task TplDataflow() => FeedAndCompleteAsync(_tpl!);

    private async Task FeedAndCompleteAsync(ILowAllocPipeline pipeline)
    {
        for (var pass = 0; pass < Passes; pass++)
        {
            for (var i = 0; i < MessageCount; i++)
                await pipeline.EnqueueAsync(_payloads[i], _tpos[i]);
        }

        pipeline.CompleteIngest();
        await pipeline.Completion;
    }

    private static PipelineOptions CreateOptions() => new()
    {
        EnableKafka = false,
        Topic = "orders",
        ChannelCapacity = 20_000,
        BatchSize = 1_000,
        // Do not flush partial batches mid-run; CompleteIngest drains the tail.
        // Otherwise linger would dominate and hide the pipeline difference.
        BatchLinger = Timeout.InfiniteTimeSpan,
        MaxDegreeOfParallelism = Environment.ProcessorCount,
    };

    // TPL Dataflow schedules on the ThreadPool. The default min-thread
    // hill-climbing injects workers slowly and produces a fast/slow
    // bimodal (Post succeeds vs SendAsync waits). Pin enough workers
    // up front so every iteration sees the same scheduler.
    private static void WarmThreadPool()
    {
        var workers = Math.Max(Environment.ProcessorCount * 4, 16);
        ThreadPool.GetMinThreads(out _, out var iocp);
        ThreadPool.SetMinThreads(workers, iocp);
    }

    private static void DisposePipeline(ILowAllocPipeline? pipeline)
    {
        if (pipeline is null)
            return;

        pipeline.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private static byte[] SerializeOrder(int i)
    {
        var buffer = new ArrayBufferWriter<byte>(160);
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("orderId", Guid.NewGuid());
            writer.WriteString("customerId", $"cust-{i % 256:D3}");
            writer.WriteNumber("amount", 10m + (i % 100));
            writer.WriteNumber("timestampMs", 1_700_000_000_000L + i);
            writer.WriteEndObject();
        }

        return buffer.WrittenSpan.ToArray();
    }
}
