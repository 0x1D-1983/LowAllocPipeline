using System.Buffers;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

namespace LowAllocPipeline;


// ---------------------------------------------------------------------------
// Typed domain model + validation, sitting between raw bytes and
//     business logic. Two deliberate choices here:
//
//     - The type is a mutable, pooled CLASS, not a fresh record per message.
//       We rent an instance, populate its fields in place, and return it to
//       the pool once the batch is processed and committed. This turns
//       "one allocation per message" into "N allocations for a pool that
//       fills once and is then reused forever."
//
//     - Validation happens INSIDE the single parse pass (Utf8JsonReader
//       directly over the pooled byte span), not as a second pass over an
//       already-materialized object. A structurally invalid or
//       schema-violating message is rejected before we've populated
//       anything, and the pooled object is returned unused.
// ---------------------------------------------------------------------------




// ---------------------------------------------------------------------------
// The pipeline itself, expressed as a TPL Dataflow block graph:
//
//      _ingestBlock (BufferBlock<PooledMessage>)
//            |  bounded capacity = backpressure to the poll loop
//            v
//      _validateBlock (TransformBlock, parallel)   parses + validates,
//            |                                      returns byte buffer to pool
//            v
//      _batchBlock (BatchBlock<ValidatedMessage>)  groups into arrays of
//            |                                      BatchSize; a Timer calls
//            |                                      TriggerBatch() for linger
//            v
//      _commitBlock (ActionBlock, MaxDOP = 1)      processes batch, commits
//                                                    offsets in order
//
//    Why Dataflow over the hand-rolled Channel<T> + manual loop version:
//      - Backpressure and completion propagation (PropagateCompletion) are
//        handled by the framework instead of hand-written wait/drain logic.
//      - BatchBlock automatically flushes a final partial batch on
//        completion — no manual "drain the remainder" code needed.
//      - Each stage declares its own MaxDegreeOfParallelism independently
//        (validation is parallel; commit is serialized to 1) rather than
//        threading Parallel.For/ForEachAsync calls through one big method.
//    Trade-off: less direct control over exactly when backpressure kicks in
//    across the chain, and a small amount of scheduling overhead per block
//    hand-off versus the tighter hand-rolled loop.
// ---------------------------------------------------------------------------
public sealed class KafkaLowAllocPipelineTpl : IAsyncDisposable
{
    private readonly PipelineOptions _opts;
    private readonly IConsumer<Ignore, byte[]> _consumer;
    private readonly ArrayPool<byte> _pool = ArrayPool<byte>.Shared;
    private readonly ObjectPool<OrderEvent> _orderEventPool =
        new DefaultObjectPool<OrderEvent>(new OrderEventPooledPolicy(), maximumRetained: 4096);
    private readonly OrderEventValidator _validator;
    private readonly CancellationTokenSource _cts = new();

    private readonly BufferBlock<PooledMessage> _ingestBlock;
    private readonly TransformBlock<PooledMessage, ValidatedMessage> _validateBlock;
    private readonly BatchBlock<ValidatedMessage> _batchBlock;
    private readonly ActionBlock<ValidatedMessage[]> _commitBlock;
    private readonly Timer _lingerTimer;

    private Task? _pollTask;

    public KafkaLowAllocPipelineTpl(PipelineOptions opts)
    {
        _opts = opts;

        var config = new ConsumerConfig
        {
            BootstrapServers = opts.BootstrapServers,
            GroupId = opts.GroupId,
            EnableAutoCommit = false,          // we commit manually, in batches
            AutoOffsetReset = AutoOffsetReset.Latest,
            FetchMinBytes = 1,
        };

        _consumer = new ConsumerBuilder<Ignore, byte[]>(config).Build();
        _validator = new OrderEventValidator(_orderEventPool);

        // --- Stage 1: ingest. BoundedCapacity is the backpressure knob —
        // SendAsync from the poll loop blocks once this fills, same role
        // BoundedChannelFullMode.Wait played before. ---
        _ingestBlock = new BufferBlock<PooledMessage>(new DataflowBlockOptions
        {
            BoundedCapacity = opts.ChannelCapacity,
            CancellationToken = _cts.Token,
        });

        // --- Stage 2: validate + parse (parallel). Returns the rented byte
        // buffer to the pool as soon as parsing is done — it's not needed
        // past this stage, so no reason to carry it through batching/commit. ---
        _validateBlock = new TransformBlock<PooledMessage, ValidatedMessage>(
            msg =>
            {
                var result = _validator.Validate(msg.Span, msg.Tpo);
                _pool.Return(msg.RentedBuffer);
                return result;
            },
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = opts.ChannelCapacity,
                MaxDegreeOfParallelism = opts.MaxDegreeOfParallelism,
                CancellationToken = _cts.Token,
            });

        // --- Stage 3: batch. BatchBlock groups into arrays of exactly
        // BatchSize; a Timer forces out whatever's pending (TriggerBatch)
        // so a partial batch doesn't wait indefinitely under low load. On
        // completion, BatchBlock automatically emits one final partial
        // batch for whatever's left, so no manual drain step is needed. ---
        _batchBlock = new BatchBlock<ValidatedMessage>(opts.BatchSize, new GroupingDataflowBlockOptions
        {
            BoundedCapacity = opts.ChannelCapacity,
            CancellationToken = _cts.Token,
        });

        // --- Stage 4: process + commit. MaxDegreeOfParallelism = 1 so
        // batches are committed in the order they were formed — parallel
        // commits could race and commit a lower offset after a higher one.
        // BoundedCapacity is small: a handful of in-flight batches is
        // plenty of pipelining without letting memory grow unbounded if
        // Kafka commits stall. ---
        _commitBlock = new ActionBlock<ValidatedMessage[]>(
            batch => ProcessAndCommitBatchAsync(batch, _cts.Token),
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = 4,
                CancellationToken = _cts.Token,
            });

        var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };
        _ingestBlock.LinkTo(_validateBlock, linkOptions);
        _validateBlock.LinkTo(_batchBlock, linkOptions);
        _batchBlock.LinkTo(_commitBlock, linkOptions);

        // Linger: force the current partial batch out periodically instead
        // of waiting for BatchSize items to accumulate. TriggerBatch() is a
        // no-op if the block currently has zero buffered items.
        _lingerTimer = new Timer(
            _ => _batchBlock.TriggerBatch(),
            state: null,
            dueTime: opts.BatchLinger,
            period: opts.BatchLinger);
    }

    public void Start()
    {
        _consumer.Subscribe(_opts.Topic);

        // Poll loop runs on its own long-running task/thread. Kafka's .Consume()
        // is blocking, so isolating it avoids starving the thread pool.
        _pollTask = Task.Factory.StartNew(
            () => PollLoop(_cts.Token),
            TaskCreationOptions.LongRunning);
    }

    // -----------------------------------------------------------------------
    // Poll loop: read from Kafka, copy payload into a rented buffer,
    // post into the ingest block. No LINQ, no intermediate collections.
    // -----------------------------------------------------------------------
    private void PollLoop(CancellationToken ct)
    {
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

            // Post() first (cheap, non-blocking); fall back to the async
            // SendAsync only when the block is actually full. This is where
            // backpressure applies: SendAsync blocks until the ingest
            // block's BoundedCapacity frees up, which only happens once
            // downstream stages drain it.
            if (!_ingestBlock.Post(pooledMsg))
            {
                var vt = _ingestBlock.SendAsync(pooledMsg, ct);
                if (!vt.IsCompletedSuccessfully)
                    vt.GetAwaiter().GetResult();
            }
        }

        // Signals completion down the whole chain (validate -> batch -> commit)
        // because every link was created with PropagateCompletion = true.
        _ingestBlock.Complete();
    }

    // -----------------------------------------------------------------------
    // Batch commit: process the typed, validated messages in a batch,
    // dead-letter rejects, commit offsets once per batch.
    // -----------------------------------------------------------------------
    private async Task ProcessAndCommitBatchAsync(ValidatedMessage[] batch, CancellationToken ct)
    {
        // Split into valid vs rejected without LINQ (avoid the allocation
        // of iterator/closure state on the hot path).
        var validCount = 0;
        foreach (var v in batch)
            if (v.Outcome == ValidationOutcome.Valid) validCount++;

        if (validCount < batch.Length)
            await DeadLetterRejectedAsync(batch, ct); // async I/O to a DLQ topic/table

        // Process only the schema-valid, typed messages downstream.
        await Parallel.ForEachAsync(
            batch,
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
        foreach (var v in batch)
        {
            var tp = v.Tpo.TopicPartition;
            if (!highestPerPartition.TryGetValue(tp, out var existing) ||
                v.Tpo.Offset > existing.Offset)
            {
                highestPerPartition[tp] = v.Tpo;
            }
        }

        var offsetsToCommit = new List<TopicPartitionOffset>(highestPerPartition.Count);
        foreach (var kvp in highestPerPartition)
            offsetsToCommit.Add(new TopicPartitionOffset(kvp.Key, kvp.Value.Offset + 1));

        _consumer.Commit(offsetsToCommit);
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
    private static Task DeadLetterRejectedAsync(ValidatedMessage[] batch, CancellationToken ct)
    {
        foreach (var v in batch)
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

        // Wait for the whole chain to drain and complete. Completion was
        // signaled by _ingestBlock.Complete() at the end of PollLoop and
        // propagates automatically through validate -> batch -> commit.
        await SafeAwait(_commitBlock.Completion);

        await _lingerTimer.DisposeAsync();
        _consumer.Close();
        _consumer.Dispose();
        _cts.Dispose();

        static async Task SafeAwait(Task t)
        {
            try { await t; } catch (OperationCanceledException) { }
        }
    }
}

