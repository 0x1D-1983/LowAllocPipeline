# LowAllocPipeline Benchmark

Two .NET Kafka consumer pipelines that process `OrderEvent` messages with as little allocation as possible. Same stages, two implementations, so you can compare a hand-rolled `Channel<T>` loop with a TPL Dataflow block graph.

```
poll / enqueue → copy into ArrayPool → validate + parse → batch → handle → commit offsets
```

Invalid or malformed messages are dead-lettered; valid ones are handled as typed, pooled `OrderEvent` instances. Offsets are committed once per batch (highest offset per partition).

Requires **.NET 10** (`net10.0`).

## Implementations

Both types implement `ILowAllocPipeline`.

| | Hand-rolled | TPL Dataflow |
|---|---|---|
| Type | `KafkaLowAllocPipeline` | `KafkaLowAllocPipelineTpl` |
| Ingest | Bounded `Channel<PooledMessage>` (`FullMode = Wait`) | `BufferBlock<PooledMessage>` |
| Validate | `Parallel.For` over the current batch | `TransformBlock` (`MaxDegreeOfParallelism` from options) |
| Batch | Manual list + linger window | `BatchBlock` + `Timer.TriggerBatch()` |
| Commit | Same process loop | `ActionBlock` with `MaxDegreeOfParallelism = 1` |

Production code calls `Start()`, which subscribes to Kafka and runs a long-running poll loop. Benchmarks skip the broker: `EnableKafka = false`, then `StartProcessing()` + `EnqueueAsync()`.

**Hand-rolled** gives tighter control over backpressure and linger. **TPL** gets completion propagation and a final partial-batch flush for free, at the cost of extra scheduling between blocks.

## Allocation strategy

- **`PooledMessage`** is a struct envelope. Payload bytes come from `ArrayPool<byte>.Shared`, not a new `byte[]` per message.
- **`OrderEvent`** is a mutable class rented from `ObjectPool<OrderEvent>` and reset on return — not a new record per message.
- **`OrderEventValidator`** parses and validates in one pass with `Utf8JsonReader` over the pooled span. No `JsonDocument` / `JsonNode` tree.
- Batching and commit use reused lists where the hand-rolled loop can; TPL `BatchBlock` emits arrays of `BatchSize`.

## Configuration

`PipelineOptions` defaults:

| Option | Default | Role |
|---|---|---|
| `BootstrapServers` | `localhost:9092` | Kafka brokers |
| `GroupId` | `low-alloc-consumer` | Consumer group |
| `Topic` | `events` | Source topic |
| `ChannelCapacity` | `10_000` | Bounded ingest → backpressure |
| `BatchSize` | `500` | Messages per commit |
| `BatchLinger` | `50ms` | Flush a partial batch under low load |
| `MaxDegreeOfParallelism` | `Environment.ProcessorCount` | Validate / handle parallelism |
| `EnableKafka` | `true` | `false` for in-process feed (benchmarks) |

## Running

```bash
dotnet build -c Release
```

**Smoke** — feed 400k in-process messages through both pipelines (no Kafka):

```bash
dotnet run -c Release -- --smoke
```

**Benchmarks** — BenchmarkDotNet, Release only:

```bash
dotnet run -c Release
dotnet run -c Release -- --job short    # quicker, less precise
```

Results land in `BenchmarkDotNet.Artifacts/`.

**Against a real broker** — construct either pipeline with default options (`EnableKafka` stays `true`) and call `Start()`. `HandleOrderEventAsync` and `DeadLetterRejectedAsync` are stubs; wire them to your store / DLQ before using this as a service.

## Benchmarks

`PipelineBenchmarks` measures the pipeline, not Kafka:

- 50,000 pre-serialized valid `OrderEvent` payloads, replayed 8 times (400,000 messages per invoke)
- Capacity 20,000, batch size 1,000, linger disabled (completion flushes the tail)
- Construction is outside the timed region (`[IterationSetup]`)
- `OperationsPerInvoke` is 400,000, so mean time and `Allocated` are **per message**

Sample run (Apple M5 Pro, .NET 10.0.11):

| Method            | Mean     | Error     | StdDev    | Ratio | RatioSD | Rank | Gen0   | Gen1   | Allocated | Alloc Ratio |
|------------------ |---------:|----------:|----------:|------:|--------:|-----:|-------:|-------:|----------:|------------:|
| HandRolledChannel | 1.318 us | 0.0063 us | 0.0059 us |  1.00 |    0.01 |    1 | 0.0325 | 0.0100 |     273 B |        1.00 |
| TplDataflow       | 1.870 us | 0.0370 us | 0.0908 us |  1.42 |    0.07 |    2 | 0.0425 | 0.0075 |     356 B |        1.30 |

On that machine the Channel pipeline was about 1.4× faster and allocated ~30% less per message. Treat the numbers as a starting point; re-run on your hardware.

## Layout

| File | Purpose |
|---|---|
| `KafkaLowAllocPipeline.cs` | Channel + manual batch/commit loop |
| `KafkaLowAllocPipelineTpl.cs` | Dataflow graph (`BufferBlock` → `TransformBlock` → `BatchBlock` → `ActionBlock`) |
| `ILowAllocPipeline.cs` | Shared start / enqueue / complete surface |
| `OrderEvent.cs` / `OrderEventPooledPolicy.cs` | Pooled domain type |
| `OrderEventValidator.cs` | One-pass UTF-8 JSON validate + parse |
| `PooledMessage.cs` / `ValidatedMessage.cs` | In-flight envelopes |
| `PipelineOptions.cs` | Knobs above |
| `PipelineBenchmarks.cs` | In-process BenchmarkDotNet suite |
| `PipelineSmoke.cs` | `--smoke` correctness check |
