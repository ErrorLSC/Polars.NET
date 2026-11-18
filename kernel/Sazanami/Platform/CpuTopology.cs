using System.Runtime.InteropServices;

namespace Kaminari.Sazanami.Platform;

/// <summary>
/// The processors the process runs on, as kernels size their parallelism: memory-bound work (gathers, scans) gains
/// nothing from a core's second hardware thread, so it takes as many threads as there are physical cores.
/// </summary>
internal static unsafe partial class CpuTopology
{
    /// <summary>
    /// The physical cores (hardware threads of one core counted once): from sysfs on Linux, <c>hw.physicalcpu</c> on
    /// macOS, the processor cores Windows lists. Never more than <see cref="Environment.ProcessorCount"/> (which a
    /// container's CPU limit or the process's affinity lowers), at least 1; <see cref="Environment.ProcessorCount"/>
    /// where the system does not tell.
    /// </summary>
    public static int PhysicalCores { get; } = Math.Clamp(Detect() ?? Environment.ProcessorCount, 1, Environment.ProcessorCount);

    private static int? Detect()
    {
        try
        {
            if (OperatingSystem.IsLinux()) return LinuxCores("/sys/devices/system/cpu");
            if (OperatingSystem.IsMacOS()) return MacCores();
            if (OperatingSystem.IsWindows()) return WindowsCores();
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException or DllNotFoundException or EntryPointNotFoundException)
        {
            // Not told: the logical count.
        }
        return null;
    }

    /// <summary>
    /// The cores under a sysfs CPU directory: the distinct sibling lists of the CPUs that have a topology (an offline
    /// CPU has none); null when none does.
    /// </summary>
    internal static int? LinuxCores(string cpuRoot)
    {
        if (!Directory.Exists(cpuRoot)) return null;
        HashSet<string> cores = [];
        foreach (string cpu in Directory.EnumerateDirectories(cpuRoot, "cpu*"))
        {
            string name = Path.GetFileName(cpu);
            if (name.Length == 3 || name.AsSpan(3).ContainsAnyExceptInRange('0', '9')) continue; // cpufreq, cpuidle
            string topology = Path.Combine(cpu, "topology");
            string? siblings = Read(Path.Combine(topology, "core_cpus_list")) ?? Read(Path.Combine(topology, "thread_siblings_list"));
            if (siblings is not null) cores.Add(siblings);
        }
        return cores.Count > 0 ? cores.Count : null;

        static string? Read(string path) => File.Exists(path) ? File.ReadAllText(path).Trim() : null;
    }

    private static int? MacCores()
    {
        int cores = 0;
        nuint size = sizeof(int);
        return sysctlbyname("hw.physicalcpu", &cores, &size, null, 0) == 0 && cores > 0 ? cores : null;
    }

    private static int? WindowsCores()
    {
        const int RelationProcessorCore = 0;
        uint length = 0;
        GetLogicalProcessorInformationEx(RelationProcessorCore, null, &length); // the length needed
        if (length == 0) return null;
        byte* records = (byte*)NativeMemory.Alloc(length);
        try
        {
            if (!GetLogicalProcessorInformationEx(RelationProcessorCore, records, &length)) return null;
            // SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX: Relationship (4 bytes), Size (4 bytes), ...: one record per core.
            int cores = 0;
            for (uint at = 0; at < length; at += *(uint*)(records + at + 4)) cores++;
            return cores > 0 ? cores : null;
        }
        finally
        {
            NativeMemory.Free(records);
        }
    }

    [LibraryImport("libc", StringMarshalling = StringMarshalling.Utf8)]
    private static partial int sysctlbyname(string name, void* value, nuint* size, void* newValue, nuint newSize);

    [LibraryImport("kernel32", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool GetLogicalProcessorInformationEx(int relationship, byte* buffer, uint* length);
}
