using LowAllocPipeline;

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