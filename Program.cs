using BenchmarkDotNet.Running;
using LowAllocPipeline;

public class Program
{
    public static void Main(string[] args)
    {
        if (args is ["--smoke"])
        {
            PipelineSmoke.Run();
            return;
        }

        BenchmarkSwitcher
            .FromAssembly(typeof(Program).Assembly)
            .Run(args);
    }
}