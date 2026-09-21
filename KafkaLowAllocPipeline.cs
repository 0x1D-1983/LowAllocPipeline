using System.Buffers;
using System.Threading.Channels;
using Confluent.Kafka;

namespace LowAllocPipeline;

// ---------------------------------------------------------------------------
// 3. The pipeline itself
// ---------------------------------------------------------------------------
public sealed class KafkaLowAllocPipeline : IAsyncDisposable
{
    private readonly PipelineOptions _opts;
    private readonly IConsumer<Ignore, byte[]> _consumer;
    private readonly Channel<PooledMessage> _channel;
    private readonly ArrayPool<byte> _pool = ArrayPool<byte>.Shared;
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

    // -----------------------------------------------------------------------
    // 4. Poll loop: read from Kafka, copy payload into a rented buffer,
    //    write to the channel. No LINQ, no intermediate collections.
    // -----------------------------------------------------------------------
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

    // -----------------------------------------------------------------------
    // 5. Process loop: batch messages, process in parallel, commit as a
    //    batch, return rented buffers to the pool.
    // -----------------------------------------------------------------------
    private async Task ProcessLoop(CancellationToken ct)
    {
        var reader = _channel.Reader;

        // Reused across batches to avoid re-allocating a List every iteration.
        var batch = new List<PooledMessage>(_opts.BatchSize);
        using var timer = new PeriodicTimer(_opts.BatchLinger);

        var readTask = reader.WaitToReadAsync(ct).AsTask();

        while (!ct.IsCancellationRequested)
        {
            batch.Clear();

            // Drain up to BatchSize messages without awaiting, as long as
            // they're immediately available.
            while (batch.Count < _opts.BatchSize && reader.TryRead(out var msg))
                batch.Add(msg);

            if (batch.Count == 0)
            {
                // Nothing ready — wait for either new data or the linger
                // timeout, whichever comes first, so we don't hold partial
                // batches indefinitely under low load.
                var completed = await Task.WhenAny(
                    reader.WaitToReadAsync(ct).AsTask(),
                    timer.WaitForNextTickAsync(ct).AsTask());

                if (completed.IsFaulted || (reader.Completion.IsCompleted && reader.Count == 0))
                    break;

                continue;
            }

            await ProcessAndCommitBatchAsync(batch, ct);
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
            // Parallelize CPU-bound processing across the batch.
            await Parallel.ForEachAsync(
                batch,
                new ParallelOptions { MaxDegreeOfParallelism = _opts.MaxDegreeOfParallelism, CancellationToken = ct },
                async (msg, token) =>
                {
                    await HandleMessageAsync(msg.Span, token);
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

    // Replace with real per-message processing (deserialize, write to
    // TimescaleDB via batched COPY, publish metrics, etc.)
    private static ValueTask HandleMessageAsync(ReadOnlySpan<byte> payload, CancellationToken ct)
    {
        // Example: parse payload with System.Text.Json.Utf8JsonReader directly
        // over the span, or a binary struct layout via MemoryMarshal — both
        // avoid allocating an intermediate string or object graph.
        return ValueTask.CompletedTask;
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