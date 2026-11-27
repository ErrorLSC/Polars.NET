using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.C;
using Kaminari.Sazanami.Arrow.Interop;
using CArrowArrayStream = Kaminari.Sazanami.Arrow.Interop.CArrowArrayStream;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Native;

namespace Polars.NET.Core.IO.Plugins;

/// <summary>
/// The engine side of the managed scans: <see cref="ScanResolver"/>s and <see cref="ScanReaderBuilder"/>s exposed
/// through Polars' I/O plugin contracts <c>polars.io_plugin.lazyframe_resolver.v1</c> and
/// <c>polars.io_plugin.file_reader.v1</c>, under the plugin package <see cref="Package"/>. The engine calls the
/// entries on its own threads; no exception crosses: each becomes the contract's error, the original exception
/// relayed to whoever collects the query.
/// </summary>
public static unsafe class IoPluginBridge
{
    /// <summary>The plugin package the engine names our scans by (explain shows <c>seseragi: …</c>).</summary>
    public const string Package = "seseragi";

    /// <summary><see cref="Package"/> as the engine is handed it.</summary>
    private static ReadOnlySpan<byte> PackageUtf8 => "seseragi"u8;

    private static readonly object RegisterLock = new();
    private static bool _registered;

    /// <summary>A LazyFrame over the resolver: the engine resolves it while optimizing each query.</summary>
    public static LazyFrameHandle Scan(ScanResolver resolver)
    {
        ArgumentNullException.ThrowIfNull(resolver);
        EnsureRegistered();
        nint handle;
        try
        {
            handle = resolver.AddReference();
        }
        catch
        {
            resolver.ReleaseUnused();
            throw;
        }
        ReadOnlySpan<byte> package = PackageUtf8;
        LazyFrameHandle lf;
        // The engine owns the reference from here on (it releases it itself if the frame cannot be made).
        fixed (byte* p = package) lf = NativeBindings.pl_lazy_frame_scan_io_plugin(p, (nuint)package.Length, (void*)handle);
        return ErrorHelper.Check(lf);
    }

    /// <summary>Hands the engine our contract structs (once per process; they live as long).</summary>
    internal static void EnsureRegistered()
    {
        if (Volatile.Read(ref _registered)) return;
        lock (RegisterLock)
        {
            if (_registered) return;
            var reader = (FileReaderContract*)NativeMemory.AllocZeroed((nuint)sizeof(FileReaderContract));
            *reader = new FileReaderContract
            {
                BuilderDeserialize = &BuilderDeserialize,
                BuilderSerialize = &BuilderSerialize,
                BuilderClone = &BuilderClone,
                BuilderRelease = &BuilderRelease,
                BuilderName = &BuilderName,
                BuilderFingerprint = &BuilderFingerprint,
                BuilderExplain = &BuilderExplain,
                BuilderCapabilities = &BuilderCapabilities,
                ScanNew = &ScanNew,
                ScanRelease = &ScanRelease,
                ReaderNew = &ReaderNew,
                ReaderRelease = &ReaderRelease,
                ReaderSchema = &ReaderSchema,
                ReaderNRows = &ReaderNRows,
                ReaderBeginRead = &ReaderBeginRead,
                ReaderCancel = &ReaderCancel,
                ReaderTakeError = &ReaderTakeError,
            };
            var resolver = (ResolverContract*)NativeMemory.AllocZeroed((nuint)sizeof(ResolverContract));
            *resolver = new ResolverContract
            {
                ResolverDeserialize = &ResolverDeserialize,
                ResolverSerialize = &ResolverSerialize,
                ResolverClone = &ResolverClone,
                ResolverRelease = &ResolverRelease,
                ResolverName = &ResolverName,
                ResolverFingerprint = &ResolverFingerprint,
                ResolverSchema = &ResolverSchema,
                ResolverResolve = &ResolverResolve,
            };
            Register("polars.io_plugin.file_reader.v1"u8, reader);
            Register("polars.io_plugin.lazyframe_resolver.v1"u8, resolver);
            Volatile.Write(ref _registered, true);
        }

        static void Register(ReadOnlySpan<byte> id, void* contract)
        {
            ReadOnlySpan<byte> package = PackageUtf8;
            fixed (byte* p = package)
            fixed (byte* i = id)
                NativeBindings.pl_io_plugin_register(p, (nuint)package.Length, i, (nuint)id.Length, contract);
            ErrorHelper.CheckVoid();
        }
    }

    // ============================================================================================ errors

    /// <summary>
    /// The contract error for an exception: a compute error whose message carries the relay token, so that the
    /// engine's error, once it reaches the caller, turns back into this exception (with its type).
    /// </summary>
    private static FfiError ToError(Exception exception)
    {
        Exception relay = ManagedErrorRelay.Capture(exception);
        return new() { Kind = FfiErrorKind.Compute, Message = FfiBuf.From($"{exception.Message} {relay.Message}") };
    }

    private static FfiError NotSerializable(string what) => new()
    {
        Kind = FfiErrorKind.NotImplemented,
        Message = FfiBuf.From($"A query plan that scans a .NET source ({what}) cannot be serialized: its scans read through this process."),
    };

    /// <summary>The schema as an Arrow struct schema (one with no columns too, which Apache.Arrow's export refuses).</summary>
    private static void ExportStruct(Schema schema, CArrowSchema* destination)
    {
        if (schema.FieldsList is { Count: > 0 })
        {
            CArrowSchemaExporter.ExportSchema(schema, destination);
            return;
        }
        // A struct of no fields, written by hand: its strings are static data, its release only marks it released.
        var raw = (RawArrowSchema*)destination;
        *raw = default;
        raw->Format = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference("+s\0"u8));
        raw->Name = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference("\0"u8));
        raw->Release = &ReleaseEmptyStruct;
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ReleaseEmptyStruct(RawArrowSchema* schema) => schema->Release = null;

    /// <summary>The C data interface's ArrowSchema with writable members (Apache.Arrow keeps its release internal).</summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct RawArrowSchema
    {
        public byte* Format;
        public byte* Name;
        public byte* Metadata;
        public long Flags;
        public long ChildCount;
        public RawArrowSchema** Children;
        public RawArrowSchema* Dictionary;
        public delegate* unmanaged[Cdecl]<RawArrowSchema*, void> Release;
        public void* PrivateData;
    }

    private static T Target<T>(nint handle) where T : class => (T)GCHandle.FromIntPtr(handle).Target!;

    private static Schema Select(Schema schema, ReadOnlySpan<FfiStr> columns)
    {
        var names = new string[columns.Length];
        for (int i = 0; i < names.Length; i++) names[i] = columns[i].Text;
        return Select(schema, names);
    }

    /// <summary>The named columns of a source's schema, in this order.</summary>
    private static Schema Select(Schema schema, IReadOnlyList<string> columns)
    {
        if (columns.Count == schema.FieldsList.Count && columns.Select((c, i) => schema.FieldsList[i].Name == c).All(static same => same)) return schema;
        List<Field> fields = [];
        foreach (string name in columns)
        {
            Field field = schema.GetFieldByName(name)
                ?? throw new InvalidOperationException($"The source has no column \"{name}\" (its columns: {string.Join(", ", schema.FieldsList.Select(static f => f.Name))}).");
            fields.Add(field);
        }
        return new Schema(fields, schema.Metadata);
    }

    // ===================================================================================== file_reader.v1

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultHandle BuilderDeserialize(FfiSlice bytes) => new() { Tag = FfiTag.Err, Err = NotSerializable("a reader") };

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultBuf BuilderSerialize(nint builder) => new() { Tag = FfiTag.Err, Err = NotSerializable("a reader") };

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static nint BuilderClone(nint builder)
    {
        try
        {
            return Target<ScanReaderBuilder>(builder).AddReference();
        }
        catch
        {
            // Cannot fail by contract: a live handle is cloned. Nothing sensible remains but to keep the handle.
            return builder;
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void BuilderRelease(nint builder)
    {
        try
        {
            Target<ScanReaderBuilder>(builder).RemoveReference();
        }
        catch
        {
            // A release has no way to report; what it holds is released as far as it got.
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiBuf BuilderName(nint builder)
    {
        try
        {
            return FfiBuf.From(Target<ScanReaderBuilder>(builder).Name);
        }
        catch
        {
            return default;
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static ulong BuilderFingerprint(nint builder)
    {
        try
        {
            return Target<ScanReaderBuilder>(builder).Fingerprint;
        }
        catch
        {
            return 0;
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultBufVec BuilderExplain(nint builder)
    {
        try
        {
            var pairs = Target<ScanReaderBuilder>(builder).Explain;
            return new() { Tag = FfiTag.Ok, Ok = FfiVec.From([.. pairs.SelectMany(static p => (string[])[p.Key, p.Value])]) };
        }
        catch (Exception e)
        {
            return new() { Tag = FfiTag.Err, Err = ToError(e) };
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultCapabilities BuilderCapabilities(nint builder)
    {
        try
        {
            ScanCapabilities c = Target<ScanReaderBuilder>(builder).Capabilities;
            return new()
            {
                Tag = FfiTag.Ok,
                Ok = new()
                {
                    Flags = (c.Unordered ? FfiCapabilities.Unordered : 0) | (c.RemoteSources ? FfiCapabilities.SourcesRemote : 0),
                    MaxConcurrentScans = FfiOptionU64.From(c.MaxConcurrentReaders is { } m ? (ulong)Math.Max(1, m) : null),
                    PreferredRemotePreInit = FfiOptionU64.From(c.RemotePreInit is { } p ? (ulong)Math.Max(0, p) : null),
                },
            };
        }
        catch (Exception e)
        {
            return new() { Tag = FfiTag.Err, Err = ToError(e) };
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultHandle ScanNew(nint builder)
    {
        try
        {
            ScanReaderBuilder target = Target<ScanReaderBuilder>(builder);
            var scan = new ScanSlot(target.BeginScan(), [.. target.Schema.FieldsList.Select(static f => f.Name)]);
            return new() { Tag = FfiTag.Ok, Ok = GCHandle.ToIntPtr(GCHandle.Alloc(scan)) };
        }
        catch (Exception e)
        {
            return new() { Tag = FfiTag.Err, Err = ToError(e) };
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ScanRelease(nint scan)
    {
        try
        {
            var handle = GCHandle.FromIntPtr(scan);
            var slot = (ScanSlot)handle.Target!;
            handle.Free();
            slot.Release();
        }
        catch
        {
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultHandle ReaderNew(nint scan, FfiStr source)
    {
        try
        {
            ScanSlot slot = Target<ScanSlot>(scan);
            var reader = new ReaderSlot(slot, source.Text);
            return new() { Tag = FfiTag.Ok, Ok = GCHandle.ToIntPtr(GCHandle.Alloc(reader)) };
        }
        catch (Exception e)
        {
            return new() { Tag = FfiTag.Err, Err = ToError(e) };
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ReaderRelease(nint reader)
    {
        try
        {
            var handle = GCHandle.FromIntPtr(reader);
            var slot = (ReaderSlot)handle.Target!;
            handle.Free();
            slot.Release();
        }
        catch
        {
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultSchema ReaderSchema(nint reader)
    {
        try
        {
            ReaderSlot slot = Target<ReaderSlot>(reader);
            Schema schema = Select(slot.Reader.Schema, slot.Columns);
            FfiResultSchema result = new() { Tag = FfiTag.Ok };
            ExportStruct(schema, &result.Ok);
            return result;
        }
        catch (Exception e)
        {
            return new() { Tag = FfiTag.Err, Err = ToError(e) };
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultU64 ReaderNRows(nint reader)
    {
        try
        {
            long rows = Target<ReaderSlot>(reader).Reader.CountRows();
            return new() { Tag = FfiTag.Ok, Ok = checked((ulong)rows) };
        }
        catch (Exception e)
        {
            return new() { Tag = FfiTag.Err, Err = ToError(e) };
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultStream ReaderBeginRead(nint reader, FfiBeginReadArgs* args)
    {
        try
        {
            ReaderSlot slot = Target<ReaderSlot>(reader);
            ReadOnlySpan<FfiStr> columns = args->Columns.Items<FfiStr>();
            Schema schema = Select(slot.Reader.Schema, columns);
            var names = new string[schema.FieldsList.Count];
            for (int i = 0; i < names.Length; i++) names[i] = schema.FieldsList[i].Name;
            var exporter = new ArrowStreamExporter(slot.Read(names), schema) { SelectByName = true };
            FfiResultStream result = new() { Tag = FfiTag.Ok };
            exporter.Export((CArrowArrayStream*)&result.Ok);
            return result;
        }
        catch (Exception e)
        {
            return new() { Tag = FfiTag.Err, Err = ToError(e) };
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ReaderCancel(nint reader)
    {
        try
        {
            Target<ReaderSlot>(reader).Cancel();
        }
        catch
        {
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultUnit ReaderTakeError(nint reader)
    {
        try
        {
            return Target<ReaderSlot>(reader).TakeError() is { } error
                ? new() { Tag = FfiTag.Err, Err = ToError(error) }
                : new() { Tag = FfiTag.Ok };
        }
        catch (Exception e)
        {
            return new() { Tag = FfiTag.Err, Err = ToError(e) };
        }
    }

    // ============================================================================== lazyframe_resolver.v1

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultHandle ResolverDeserialize(FfiSlice bytes) => new() { Tag = FfiTag.Err, Err = NotSerializable("a resolver") };

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultBuf ResolverSerialize(nint resolver) => new() { Tag = FfiTag.Err, Err = NotSerializable("a resolver") };

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static nint ResolverClone(nint resolver)
    {
        try
        {
            return Target<ScanResolver>(resolver).AddReference();
        }
        catch
        {
            return resolver;
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ResolverRelease(nint resolver)
    {
        try
        {
            Target<ScanResolver>(resolver).RemoveReference();
        }
        catch
        {
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiBuf ResolverName(nint resolver)
    {
        try
        {
            return FfiBuf.From(Target<ScanResolver>(resolver).Name);
        }
        catch
        {
            return default;
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static ulong ResolverFingerprint(nint resolver)
    {
        try
        {
            return Target<ScanResolver>(resolver).Fingerprint;
        }
        catch
        {
            return 0;
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultSchema ResolverSchema(nint resolver)
    {
        try
        {
            Schema schema = Target<ScanResolver>(resolver).Schema;
            FfiResultSchema result = new() { Tag = FfiTag.Ok };
            ExportStruct(schema, &result.Ok);
            return result;
        }
        catch (Exception e)
        {
            return new() { Tag = FfiTag.Err, Err = ToError(e) };
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static FfiResultResolved ResolverResolve(nint handle, FfiResolveArgs* args)
    {
        try
        {
            ScanResolver resolver = Target<ScanResolver>(handle);
            ResolveRequest request = Request(args);
            ScanPlan? plan = resolver.Resolve(request);
            if (plan is null)
            {
                if (request.ExistingVersionKey is null)
                    throw new InvalidOperationException($"The resolver {resolver.Name} kept a resolution it was not given.");
                return new() { Tag = FfiTag.Ok, Ok = new() { VersionKey = FfiOptionBuf.From(request.ExistingVersionKey) } };
            }
            return new() { Tag = FfiTag.Ok, Ok = Resolved(plan, request.Filters.Count) };
        }
        catch (Exception e)
        {
            return new() { Tag = FfiTag.Err, Err = ToError(e) };
        }
    }

    private static ResolveRequest Request(FfiResolveArgs* args)
    {
        string[]? projection = null;
        if (args->Projection.Tag == FfiTag.Some) projection = Texts(args->Projection.Value);
        string[] documents = Texts(args->Filters);
        var filters = new PushedFilter[documents.Length];
        for (int i = 0; i < filters.Length; i++)
        {
            var (exact, implied) = FilterDocuments.Translate(documents[i]);
            filters[i] = new PushedFilter(i, documents[i], exact, implied);
        }
        return new ResolveRequest(
            projection,
            args->Limit.Get is { } limit ? checked((long)limit) : null,
            filters,
            Texts(args->FilterColumns),
            args->FilterDropColumnsIdx.Get is { } from ? checked((int)from) : null,
            args->ExistingVersionKey.Get,
            args->TargetNSources.Get is { } target ? (int)Math.Min(target, int.MaxValue) : null,
            args->Verbose != 0);

        static string[] Texts(FfiSlice slice)
        {
            ReadOnlySpan<FfiStr> items = slice.Items<FfiStr>();
            var texts = new string[items.Length];
            for (int i = 0; i < texts.Length; i++) texts[i] = items[i].Text;
            return texts;
        }
    }

    /// <summary>The plan as the contract's resolution: everything that can fail first, the builder's reference last.</summary>
    private static FfiResolved Resolved(ScanPlan plan, int filterCount)
    {
        ArgumentNullException.ThrowIfNull(plan.Reader);
        IReadOnlyList<int> applied = plan.AppliedFilters ?? [];
        var indices = new ulong[applied.Count];
        for (int i = 0; i < indices.Length; i++)
        {
            if ((uint)applied[i] >= (uint)filterCount)
                throw new InvalidOperationException($"The scan reported filter {applied[i]} applied; the query pushed {filterCount}.");
            indices[i] = (ulong)applied[i];
        }
        FfiResolved resolved = default;
        resolved.Scan.Tag = FfiTag.Some;
        ref FfiResolvedScan scan = ref resolved.Scan.Value;
        bool referenced = false;
        try
        {
            scan.Sources = FfiVec.From(plan.Sources);
            scan.AppliedFilters = FfiVec.From(indices);
            scan.ReaderPackage = FfiBuf.From(PackageUtf8);
            scan.RowEstimate = FfiOptionU64.From(plan.RowEstimate is { } rows ? (ulong)Math.Max(0, rows) : null);
            resolved.VersionKey = FfiOptionBuf.From(plan.VersionKey);
            scan.Reader = plan.Reader.AddReference();
            referenced = true;
            // Last: a schema the export refused leaves nothing exported to release.
            ExportStruct(plan.Reader.Schema, (CArrowSchema*)Unsafe.AsPointer(ref scan.Schema));
            return resolved;
        }
        catch
        {
            if (referenced) plan.Reader.RemoveReference();
            if (scan.Sources.Release != null) scan.Sources.Release(scan.Sources.Owner);
            if (scan.AppliedFilters.Release != null) scan.AppliedFilters.Release(scan.AppliedFilters.Owner);
            if (scan.ReaderPackage.Release != null) scan.ReaderPackage.Release(scan.ReaderPackage.Owner);
            if (resolved.VersionKey.Value.Release != null) resolved.VersionKey.Value.Release(resolved.VersionKey.Value.Owner);
            throw;
        }
    }

    // ============================================================================================ state

    /// <summary>
    /// One execution of a scan: the engine's reference and one per open reader, so the execution outlives the
    /// engine's release while its readers run.
    /// </summary>
    private sealed class ScanSlot(ScanExecution execution, string[] columns)
    {
        private int _references = 1;

        public ScanExecution Execution { get; } = execution;

        /// <summary>The scan's columns: what the engine gets of each source's schema.</summary>
        public string[] Columns { get; } = columns;

        public void AddReference() => Interlocked.Increment(ref _references);

        public void Release()
        {
            if (Interlocked.Decrement(ref _references) == 0) Execution.Dispose();
        }
    }

    /// <summary>
    /// One source's reader: its cancellation (sticky), the error of its last failed batch, and its disposal once the
    /// engine released it and no stream of it is open.
    /// </summary>
    private sealed class ReaderSlot
    {
        private readonly object _lock = new();
        private readonly ScanSlot _scan;
        private readonly CancellationTokenSource _cancel = new();
        private Exception? _error;
        private int _openStreams;
        private bool _released;
        private bool _disposed;

        public ReaderSlot(ScanSlot scan, string source)
        {
            Reader = scan.Execution.OpenSource(source);
            _scan = scan;
            scan.AddReference();
        }

        public SourceReader Reader { get; }

        public string[] Columns => _scan.Columns;

        public void Cancel()
        {
            lock (_lock)
            {
                if (_disposed) return;
            }
            // Outside the lock: the token's callbacks run inline, and a reader's may fail (Failed takes the lock).
            try
            {
                _cancel.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // Released meanwhile: nothing left to stop.
            }
        }

        public Exception? TakeError()
        {
            lock (_lock)
            {
                Exception? error = _error;
                _error = null;
                return error;
            }
        }

        public IEnumerator<RecordBatch> Read(string[] columns)
        {
            CancellationToken token;
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                token = _cancel.Token;
                _openStreams++;
            }
            try
            {
                // A cancelled reader starts no work: its stream ends at once.
                IEnumerable<RecordBatch> batches = token.IsCancellationRequested ? [] : Reader.Read(columns, token);
                return new Batches(this, batches, token);
            }
            catch
            {
                StreamClosed();
                throw;
            }
        }

        private void Failed(Exception error)
        {
            lock (_lock) _error = error;
        }

        private void StreamClosed()
        {
            lock (_lock)
            {
                _openStreams--;
                if (!_released || _openStreams > 0 || _disposed) return;
                _disposed = true;
            }
            Dispose();
        }

        public void Release()
        {
            lock (_lock)
            {
                _released = true;
                if (_openStreams > 0 || _disposed) return;
                _disposed = true;
            }
            Dispose();
        }

        private void Dispose()
        {
            try
            {
                Reader.Dispose();
            }
            finally
            {
                _cancel.Dispose();
                _scan.Release();
            }
        }

        /// <summary>The reader's batches as the exported stream pulls them.</summary>
        private sealed class Batches(ReaderSlot slot, IEnumerable<RecordBatch> source, CancellationToken token) : IEnumerator<RecordBatch>
        {
            private IEnumerator<RecordBatch>? _inner;
            private bool _closed;

            public RecordBatch Current => _inner!.Current;

            object System.Collections.IEnumerator.Current => Current;

            public bool MoveNext()
            {
                // Cancelled: the stream ends (the engine stopped wanting it).
                if (token.IsCancellationRequested) return false;
                try
                {
                    _inner ??= source.GetEnumerator();
                    return _inner.MoveNext();
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    return false;
                }
                catch (Exception e)
                {
                    slot.Failed(e);
                    throw;
                }
            }

            public void Reset() => throw new NotSupportedException();

            public void Dispose()
            {
                if (_closed) return;
                _closed = true;
                try
                {
                    _inner?.Dispose();
                }
                finally
                {
                    slot.StreamClosed();
                }
            }
        }
    }
}
