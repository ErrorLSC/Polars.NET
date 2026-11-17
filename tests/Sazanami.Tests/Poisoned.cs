using System.Runtime.CompilerServices;
using Kaminari.Sazanami.Buffers;

namespace Sazanami.Tests;

internal static class Poisoned
{
    /// <summary>Native blocks come filled with 0xCC in these tests: code counting on zeroed memory fails here.</summary>
    [ModuleInitializer]
    internal static void Init() => NativeMemoryPool.Poison = true;
}
