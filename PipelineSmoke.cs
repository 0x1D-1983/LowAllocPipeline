namespace LowAllocPipeline;

/// <summary>
/// Fast correctness check that both pipelines accept a full in-process
/// workload and complete. Run with: <c>dotnet run -c Release -- --smoke</c>
/// </summary>
internal static class PipelineSmoke
{
    public static void Run()
    {
        var bench = new PipelineBenchmarks();
        bench.GlobalSetup();

        bench.SetupHandRolled();
        bench.HandRolledChannel().GetAwaiter().GetResult();
        bench.CleanupHandRolled();
        Console.WriteLine($"hand-rolled: processed {PipelineBenchmarks.TotalMessages} messages");

        bench.SetupTpl();
        bench.TplDataflow().GetAwaiter().GetResult();
        bench.CleanupTpl();
        Console.WriteLine($"tpl-dataflow: processed {PipelineBenchmarks.TotalMessages} messages");
    }
}
