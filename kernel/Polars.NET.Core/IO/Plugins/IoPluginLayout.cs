using System.Runtime.InteropServices;
using Apache.Arrow.C;
using CArrowArrayStream = Kaminari.Sazanami.Arrow.Interop.CArrowArrayStream;
using Polars.NET.Core.Native;

namespace Polars.NET.Core.IO.Plugins;

/// <summary>The bridge's mirrors of the contract types against the engine's layouts (pl_io_plugin_layout).</summary>
internal static unsafe class IoPluginLayout
{
    /// <summary>Every value that differs, as "name: engine X, bridge Y" (empty when all agree).</summary>
    public static List<string> Mismatches()
    {
        nint text = NativeBindings.pl_io_plugin_layout();
        Dictionary<string, long> engine;
        try
        {
            engine = Marshal.PtrToStringUTF8(text)!
                .Split('\n', StringSplitOptions.RemoveEmptyEntries)
                .Select(static line => line.Split('='))
                .ToDictionary(static p => p[0], static p => long.Parse(p[1], System.Globalization.CultureInfo.InvariantCulture));
        }
        finally
        {
            NativeBindings.pl_free_string(text);
        }

        Dictionary<string, long> bridge = new()
        {
            ["FfiStr.size"] = sizeof(FfiStr),
            ["FfiBuf.size"] = sizeof(FfiBuf),
            ["FfiSlice.size"] = sizeof(FfiSlice),
            ["FfiVec.size"] = sizeof(FfiVec),
            ["FfiError.size"] = sizeof(FfiError),
            ["FfiError.kind"] = Offset<FfiError>(nameof(FfiError.Kind)),
            ["FfiError.msg"] = Offset<FfiError>(nameof(FfiError.Message)),
            ["ArrowSchema.size"] = sizeof(CArrowSchema),
            ["ArrowArrayStream.size"] = sizeof(CArrowArrayStream),
            ["OptionU64.size"] = sizeof(FfiOptionU64),
            ["OptionU64.some"] = Offset<FfiOptionU64>(nameof(FfiOptionU64.Value)),
            ["OptionStr.size"] = sizeof(FfiOptionStr),
            ["OptionStr.some"] = Offset<FfiOptionStr>(nameof(FfiOptionStr.Value)),
            ["OptionStrSlice.size"] = sizeof(FfiOptionSlice),
            ["OptionStrSlice.some"] = Offset<FfiOptionSlice>(nameof(FfiOptionSlice.Value)),
            ["OptionBuf.size"] = sizeof(FfiOptionBuf),
            ["OptionBuf.some"] = Offset<FfiOptionBuf>(nameof(FfiOptionBuf.Value)),
            ["Capabilities.size"] = sizeof(FfiCapabilities),
            ["Capabilities.flags"] = Offset<FfiCapabilities>(nameof(FfiCapabilities.Flags)),
            ["Capabilities.max_concurrent_scans"] = Offset<FfiCapabilities>(nameof(FfiCapabilities.MaxConcurrentScans)),
            ["Capabilities.preferred_remote_pre_init"] = Offset<FfiCapabilities>(nameof(FfiCapabilities.PreferredRemotePreInit)),
            ["BeginReadArgs.size"] = sizeof(FfiBeginReadArgs),
            ["BeginReadArgs.columns"] = Offset<FfiBeginReadArgs>(nameof(FfiBeginReadArgs.Columns)),
            ["BeginReadArgs.verbose"] = Offset<FfiBeginReadArgs>(nameof(FfiBeginReadArgs.Verbose)),
            ["ResolveArgs.size"] = sizeof(FfiResolveArgs),
            ["ResolveArgs.projection"] = Offset<FfiResolveArgs>(nameof(FfiResolveArgs.Projection)),
            ["ResolveArgs.limit"] = Offset<FfiResolveArgs>(nameof(FfiResolveArgs.Limit)),
            ["ResolveArgs.filters"] = Offset<FfiResolveArgs>(nameof(FfiResolveArgs.Filters)),
            ["ResolveArgs.filter_columns"] = Offset<FfiResolveArgs>(nameof(FfiResolveArgs.FilterColumns)),
            ["ResolveArgs.filter_drop_columns_idx"] = Offset<FfiResolveArgs>(nameof(FfiResolveArgs.FilterDropColumnsIdx)),
            ["ResolveArgs.existing_version_key"] = Offset<FfiResolveArgs>(nameof(FfiResolveArgs.ExistingVersionKey)),
            ["ResolveArgs.target_n_sources"] = Offset<FfiResolveArgs>(nameof(FfiResolveArgs.TargetNSources)),
            ["ResolveArgs.verbose"] = Offset<FfiResolveArgs>(nameof(FfiResolveArgs.Verbose)),
            ["ResolvedScan.size"] = sizeof(FfiResolvedScan),
            ["ResolvedScan.schema"] = Offset<FfiResolvedScan>(nameof(FfiResolvedScan.Schema)),
            ["ResolvedScan.sources"] = Offset<FfiResolvedScan>(nameof(FfiResolvedScan.Sources)),
            ["ResolvedScan.reader_package"] = Offset<FfiResolvedScan>(nameof(FfiResolvedScan.ReaderPackage)),
            ["ResolvedScan.reader"] = Offset<FfiResolvedScan>(nameof(FfiResolvedScan.Reader)),
            ["ResolvedScan.applied_filters"] = Offset<FfiResolvedScan>(nameof(FfiResolvedScan.AppliedFilters)),
            ["ResolvedScan.row_estimate"] = Offset<FfiResolvedScan>(nameof(FfiResolvedScan.RowEstimate)),
            ["OptionResolvedScan.size"] = sizeof(FfiOptionResolvedScan),
            ["OptionResolvedScan.some"] = Offset<FfiOptionResolvedScan>(nameof(FfiOptionResolvedScan.Value)),
            ["Resolved.size"] = sizeof(FfiResolved),
            ["Resolved.scan"] = Offset<FfiResolved>(nameof(FfiResolved.Scan)),
            ["Resolved.version_key"] = Offset<FfiResolved>(nameof(FfiResolved.VersionKey)),
            ["FileReaderPlugin.size"] = sizeof(FileReaderContract),
            ["ResolverPlugin.size"] = sizeof(ResolverContract),
        };
        Result<FfiResultHandle>("ResultHandle", nameof(FfiResultHandle.Ok));
        Result<FfiResultBuf>("ResultBuf", nameof(FfiResultBuf.Ok));
        Result<FfiResultU64>("ResultU64", nameof(FfiResultU64.Ok));
        Result<FfiResultUnit>("ResultUnit", null);
        Result<FfiResultCapabilities>("ResultCapabilities", nameof(FfiResultCapabilities.Ok));
        Result<FfiResultSchema>("ResultSchema", nameof(FfiResultSchema.Ok));
        Result<FfiResultStream>("ResultStream", nameof(FfiResultStream.Ok));
        Result<FfiResultBufVec>("ResultBufVec", nameof(FfiResultBufVec.Ok));
        Result<FfiResultResolved>("ResultResolved", nameof(FfiResultResolved.Ok));
        foreach (string option in (string[])["OptionU64", "OptionStr", "OptionStrSlice", "OptionBuf"])
        {
            bridge[$"{option}.tag_some"] = FfiTag.Some;
            bridge[$"{option}.tag_none"] = FfiTag.None;
        }

        List<string> mismatches = [];
        foreach (var (name, value) in bridge)
        {
            if (!engine.TryGetValue(name, out long expected)) mismatches.Add($"{name}: not reported by the engine");
            else if (expected != value) mismatches.Add($"{name}: engine {expected}, bridge {value}");
        }
        return mismatches;

        void Result<T>(string name, string? ok) where T : unmanaged
        {
            bridge[$"{name}.size"] = sizeof(T);
            // A unit payload sits where the union starts: the error's offset.
            bridge[$"{name}.ok"] = ok is null ? Offset<T>("Err") : Offset<T>(ok);
            bridge[$"{name}.err"] = Offset<T>("Err");
            bridge[$"{name}.tag_ok"] = FfiTag.Ok;
            bridge[$"{name}.tag_err"] = FfiTag.Err;
        }

        static long Offset<T>(string field) => Marshal.OffsetOf<T>(field).ToInt64();
    }
}
