using Kaminari.Sazanami.Platform;

namespace Sazanami.Tests;

/// <summary><see cref="CpuTopology"/>: physical cores counted from a sysfs tree (hardware threads of a core once), and the bounds of the machine's count.</summary>
public sealed class CpuTopologyTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("sazanami-cpus").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    /// <summary>CPU <paramref name="cpu"/> with its sibling list in the file a kernel of that age writes (none: offline).</summary>
    private void Cpu(int cpu, string? siblings, string file = "core_cpus_list")
    {
        string topology = Path.Combine(_root, $"cpu{cpu}", "topology");
        Directory.CreateDirectory(topology);
        if (siblings is not null) File.WriteAllText(Path.Combine(topology, file), siblings + "\n");
    }

    [Fact]
    public void HardwareThreadsOfACore_CountOnce()
    {
        // 4 cores of 2 threads, numbered as Linux numbers them (thread 0 of every core first), and an offline CPU.
        for (int cpu = 0; cpu < 8; cpu++) Cpu(cpu, $"{cpu % 4},{cpu % 4 + 4}");
        Cpu(8, null);
        Directory.CreateDirectory(Path.Combine(_root, "cpufreq"));
        Directory.CreateDirectory(Path.Combine(_root, "cpuidle"));
        Assert.Equal(4, CpuTopology.LinuxCores(_root));
    }

    [Fact]
    public void WithoutHardwareThreads_EveryCpuIsACore_AndOlderKernelsListThreadSiblings()
    {
        for (int cpu = 0; cpu < 6; cpu++) Cpu(cpu, $"{cpu}", "thread_siblings_list");
        Assert.Equal(6, CpuTopology.LinuxCores(_root));
    }

    [Fact]
    public void NoTopology_IsNotTold()
    {
        Assert.Null(CpuTopology.LinuxCores(Path.Combine(_root, "missing")));
        Cpu(0, null);
        Assert.Null(CpuTopology.LinuxCores(_root));
    }

    [Fact]
    public void TheMachinesCount_IsBetweenOneAndTheLogicalProcessors()
    {
        Assert.InRange(CpuTopology.PhysicalCores, 1, Environment.ProcessorCount);
        if (OperatingSystem.IsLinux() && CpuTopology.LinuxCores("/sys/devices/system/cpu") is int cores)
            Assert.Equal(Math.Min(cores, Environment.ProcessorCount), CpuTopology.PhysicalCores);
    }
}
