using System.Buffers;
using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

namespace LowAllocPipeline;

public sealed class KafkaLowAllocPipeline : IAsyncDisposable
{
    private readonly PipelineOptions _opts;
    private readonly IConsumer<Ignore, byte[]> _consumer;
    private readonly Channel<PooledMessage> _channel;
    private readonly ArrayPool<byte> _pool = ArrayPool<byte>.Shared;
    private readonly ObjectPool<OrderEvent> _orderEventPool =
        new DefaultObjectPool<OrderEvent>(new OrderEventPooledPolicy(), maximumRetained: 4096);
    private readonly OrderEventValidator _validator;
    private readonly CancellationTokenSource _cts = new();
    private Task? _pollTask;
    private Task? _processTask;
    
    public KafkaLowAllocPipeline(PipelineOptions opts)
    {
        _opts = opts;
 
        // BoundedChannelFullMode.Wait => the poll loop blocks (awaits) when the
        // channel is full. This is the backpressure mechanism: if downstream
        // processing can't keep up, we stop pulling more messages off the
        // Kafka socket buffer rather than buffering unbounded memory in-process.
        _channel = Channel.CreateBounded<PooledMessage>(new BoundedChannelOptions(opts.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
        });
 
        var config = new ConsumerConfig
        {
            BootstrapServers = opts.BootstrapServers,
            GroupId = opts.GroupId,
            EnableAutoCommit = false,          // we commit manually, in batches
            AutoOffsetReset = AutoOffsetReset.Latest,
            FetchMinBytes = 1,
            // Tune these for throughput vs latency tradeoffs per workload:
            // FetchWaitMaxMs, QueuedMinMessages, etc.
        };
 
        _consumer = new ConsumerBuilder<Ignore, byte[]>(config).Build();
        _validator = new OrderEventValidator(_orderEventPool);
    }

    public void Start()
    {
        _consumer.Subscribe(_opts.Topic);
 
        // Poll loop runs on its own long-running task/thread. Kafka's .Consume()
        // is blocking, so isolating it avoids starving the thread pool.
        _pollTask = Task.Factory.StartNew(
            () => PollLoop(_cts.Token),
            TaskCreationOptions.LongRunning);
 
        _processTask = Task.Run(() => ProcessLoop(_cts.Token));
    }
 
    /// <summary>
    /// Poll loop: read from Kafka, copy payload into a rented buffer,
    /// write to the channel. No LINQ, no intermediate collections.
    /// </summary>
    /// <param name="ct"></param>
    private void PollLoop(CancellationToken ct)
    {
        var writer = _channel.Writer;
 
        while (!ct.IsCancellationRequested)
        {
            ConsumeResult<Ignore, byte[]>? result;
            try
            {
                result = _consumer.Consume(ct);
            }
            catch (ConsumeException ex)
            {
                // Log and continue; a real implementation should classify
                // fatal vs transient errors here.
                Console.Error.WriteLine($"Consume error: {ex.Error.Reason}");
                continue;
            }
            catch (OperationCanceledException)
            {
                break;
            }
 
            if (result?.Message is null)
                continue;
 
            var payload = result.Message.Value;
            var rented = _pool.Rent(payload.Length);
            Buffer.BlockCopy(payload, 0, rented, 0, payload.Length);
 
            var pooledMsg = new PooledMessage(rented, payload.Length, result.TopicPartitionOffset);
 
            // TryWrite first (cheap, non-blocking); fall back to the async
            // path only when the channel is actually full. Avoids a Task
            // allocation on the common case where there's room.
            if (!writer.TryWrite(pooledMsg))
            {
                // Blocks (asynchronously, via ValueTask) until space frees up.
                // This is where Kafka-consumer backpressure actually applies.
                var vt = writer.WriteAsync(pooledMsg, ct);
                if (!vt.IsCompletedSuccessfully)
                    vt.AsTask().GetAwaiter().GetResult();
            }
        }
 
        writer.Complete();
    }
 
    /// <summary>
    /// Process loop: batch messages, process in parallel, commit as a
    /// batch, return rented buffers to the pool.
    /// </summary>
    /// <param name="ct"></param>
    /// <returns></returns>
    private async Task ProcessLoop(CancellationToken ct)
    {
        var reader = _channel.Reader;
 
        // Reused across batches to avoid re-allocating a List every iteration.
        var batch = new List<PooledMessage>(_opts.BatchSize);
 
        while (!ct.IsCancellationRequested)
        {
            // Block until at least one message is available (or the channel completes).
            if (!await reader.WaitToReadAsync(ct))
                break;
 
            // Drain whatever's immediately available, without waiting.
            while (batch.Count < _opts.BatchSize && reader.TryRead(out var msg))
                batch.Add(msg);
 
            // Batch isn't full yet: give it up to BatchLinger more time to fill,
            // continuing to drain as messages trickle in, rather than flushing
            // a 1-message batch immediately. This is the actual linger.ms-style
            // behavior — the earlier `batch.Count == 0` check never engaged it
            // for a partially-filled batch.
            if (batch.Count < _opts.BatchSize)
            {
                using var lingerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                lingerCts.CancelAfter(_opts.BatchLinger);
 
                try
                {
                    while (batch.Count < _opts.BatchSize)
                    {
                        if (!await reader.WaitToReadAsync(lingerCts.Token))
                            break; // channel completed while lingering
 
                        while (batch.Count < _opts.BatchSize && reader.TryRead(out var msg))
                            batch.Add(msg);
                    }
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    // Linger window elapsed — fall through and flush the partial batch.
                }
            }
 
            if (batch.Count > 0)
            {
                await ProcessAndCommitBatchAsync(batch, ct);
                batch.Clear(); // only clear AFTER a successful flush, not at loop top
            }
        }
 
        // Drain any remainder after the channel completes.
        while (reader.TryRead(out var msg))
            batch.Add(msg);
        if (batch.Count > 0)
            await ProcessAndCommitBatchAsync(batch, ct);
    }
 
    private async Task ProcessAndCommitBatchAsync(List<PooledMessage> batch, CancellationToken ct)
    {
        try
        {
            // Validate + parse each raw message into a typed, pooled OrderEvent.
            // This runs synchronously per-message (Utf8JsonReader is a ref struct,
            // so it can't cross an await/Task boundary) — cheap enough that
            // Parallel.For rather than Parallel.ForEachAsync is the right tool here.
            var validated = new ValidatedMessage[batch.Count];
            Parallel.For(0, batch.Count,
                new ParallelOptions { MaxDegreeOfParallelism = _opts.MaxDegreeOfParallelism, CancellationToken = ct },
                i => validated[i] = _validator.Validate(batch[i].Span, batch[i].Tpo));
 
            // Split into valid vs rejected without LINQ (avoid the allocation
            // of iterator/closure state on the hot path).
            var validCount = 0;
            foreach (var v in validated)
                if (v.Outcome == ValidationOutcome.Valid) validCount++;
 
            if (validCount < validated.Length)
                await DeadLetterRejectedAsync(validated, ct); // async I/O to a DLQ topic/table
 
            // Process only the schema-valid, typed messages downstream.
            await Parallel.ForEachAsync(
                validated,
                new ParallelOptions { MaxDegreeOfParallelism = _opts.MaxDegreeOfParallelism, CancellationToken = ct },
                async (v, token) =>
                {
                    if (v.Outcome != ValidationOutcome.Valid || v.Typed is null)
                        return;
 
                    try
                    {
                        await HandleOrderEventAsync(v.Typed, token);
                    }
                    finally
                    {
                        _orderEventPool.Return(v.Typed); // return to pool once fully processed
                    }
                });
 
            // Batch commit: one broker round trip for the whole batch,
            // committing the highest offset per partition.
            var highestPerPartition = new Dictionary<TopicPartition, TopicPartitionOffset>();
            foreach (var msg in batch)
            {
                var tp = msg.Tpo.TopicPartition;
                if (!highestPerPartition.TryGetValue(tp, out var existing) ||
                    msg.Tpo.Offset > existing.Offset)
                {
                    highestPerPartition[tp] = msg.Tpo;
                }
            }
 
            var offsetsToCommit = new List<TopicPartitionOffset>(highestPerPartition.Count);
            foreach (var kvp in highestPerPartition)
                offsetsToCommit.Add(new TopicPartitionOffset(kvp.Key, kvp.Value.Offset + 1));
 
            _consumer.Commit(offsetsToCommit);
        }
        finally
        {
            // Always return rented buffers, even on failure, or we leak
            // pooled memory back to GC-managed allocation over time.
            foreach (var msg in batch)
                _pool.Return(msg.RentedBuffer);
        }
    }
 
    // Real per-message processing now operates on the typed, validated
    // OrderEvent rather than raw bytes — e.g. batched COPY into TimescaleDB,
    // metric emission, downstream Kafka publish, etc.
    private static ValueTask HandleOrderEventAsync(OrderEvent order, CancellationToken ct)
    {
        return ValueTask.CompletedTask;
    }
 
    // Schema-invalid or malformed messages go here instead of silently
    // dropping or crashing the batch. Typical real implementation: publish
    // to a Kafka DLQ topic or write to a TimescaleDB "rejected_events" table,
    // tagged with v.Reason for observability (feed into Grafana as a rate panel).
    private static Task DeadLetterRejectedAsync(ValidatedMessage[] validated, CancellationToken ct)
    {
        foreach (var v in validated)
        {
            if (v.Outcome == ValidationOutcome.Valid) continue;
            Console.Error.WriteLine($"[DLQ] {v.Tpo} outcome={v.Outcome} reason={v.Reason}");
        }
        return Task.CompletedTask;
    }
 
    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
 
        if (_pollTask is not null)
            await SafeAwait(_pollTask);
        if (_processTask is not null)
            await SafeAwait(_processTask);
 
        _consumer.Close();
        _consumer.Dispose();
        _cts.Dispose();
 
        static async Task SafeAwait(Task t)
        {
            try { await t; } catch (OperationCanceledException) { }
        }
    }
}