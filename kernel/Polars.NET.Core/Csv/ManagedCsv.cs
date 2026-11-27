using System.Buffers;
using System.IO.Pipelines;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.IO;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Enums;
using Polars.NET.Core.IO.Plugins;
using Polars.NET.Core.Wrappers;
using static Polars.NET.Core.IO.ManagedFrames;

namespace Polars.NET.Core.Csv;

/// <summary>
/// How CSV is read, as the public API gives it (the engine's parameters, mapped onto the managed reader): types given
/// (<see cref="Schema"/>, by name with a header, by position without; <see cref="DtypeOverride"/> by name, names the
/// files lack passed over), how the files are written, which rows and extra columns are read.
/// </summary>
public sealed record CsvScanArgs
{
    /// <summary>Every column and its type (borrowed); null infers them.</summary>
    public SchemaHandle? Schema { get; init; }

    /// <summary>The types of some inferred columns (borrowed).</summary>
    public SchemaHandle? DtypeOverride { get; init; }

    public bool HasHeader { get; init; } = true;
    public char Separator { get; init; } = ',';

    /// <summary>The quote; null or '\0' read without quoting.</summary>
    public char? QuoteChar { get; init; } = '"';

    /// <summary>The line end: '\n' (the default) ends records at LF, CRLF or a CR alone; another byte ends them at it.</summary>
    public char EolChar { get; init; } = '\n';

    public bool IgnoreErrors { get; init; }
    public bool TryParseDates { get; init; } = true;
    public bool Glob { get; init; } = true;

    /// <summary>Whether an eager read gives contiguous columns (else chunks as read).</summary>
    public bool Rechunk { get; init; }

    public bool RaiseIfEmpty { get; init; } = true;
    public ulong SkipRows { get; init; }
    public ulong SkipRowsAfterHeader { get; init; }
    public ulong SkipLines { get; init; }
    public ulong? NRows { get; init; }

    /// <summary>The records the types are inferred from (null, or more than 16,777,216: all; 0: every column a string).</summary>
    public ulong? InferSchemaLength { get; init; } = 100;

    /// <summary>The files the types are inferred from (null or <see cref="ulong.MaxValue"/>: 10, as dataframe engines take).</summary>
    public ulong? InferSchemaFiles { get; init; }

    public ulong? NThreads { get; init; }
    public string? RowIndexName { get; init; }
    public ulong RowIndexOffset { get; init; }
    public string? IncludeFilePaths { get; init; }

    /// <summary>The files' encoding (null: UTF-8).</summary>
    public System.Text.Encoding? Encoding { get; init; }

    /// <summary>Whether text the encoding does not define is read as U+FFFD (else an error naming the row).</summary>
    public bool LossyEncoding { get; init; }

    public IReadOnlyList<string>? NullValues { get; init; }
    public bool MissingIsNull { get; init; } = true;
    public string? CommentPrefix { get; init; }
    public bool DecimalComma { get; init; }
    public bool TruncateRaggedLines { get; init; }
    public IReadOnlyList<string>? TrueValues { get; init; }
    public IReadOnlyList<string>? FalseValues { get; init; }
    public string? ThousandsSeparator { get; init; }
    public bool AllowMissingColumns { get; init; }
    public bool IgnoreExtraColumns { get; init; }
    public bool HivePartitioning { get; init; }
}

/// <summary>How CSV is written, as the public API gives it.</summary>
public sealed record CsvWriteArgs
{
    public bool IncludeBom { get; init; }
    public bool IncludeHeader { get; init; } = true;
    public char Separator { get; init; } = ',';
    public char QuoteChar { get; init; } = '"';
    public PlQuoteStyle QuoteStyle { get; init; } = PlQuoteStyle.Necessary;
    public string? NullValue { get; init; }
    public string? LineTerminator { get; init; } = "\n";
    public bool? FloatScientific { get; init; }
    public int? FloatPrecision { get; init; }
    public bool DecimalComma { get; init; }
    public string? DateFormat { get; init; }
    public string? TimeFormat { get; init; }
    public string? DatetimeFormat { get; init; }
    public string? ThousandsSeparator { get; init; }

    /// <summary>The output's encoding (null: UTF-8).</summary>
    public System.Text.Encoding? Encoding { get; init; }

    /// <summary>Whether a character the encoding cannot write is written as '?' (else an error naming the row).</summary>
    public bool LossyEncoding { get; init; }

    public PlExternalCompression Compression { get; init; } = PlExternalCompression.Uncompressed;

    /// <summary>The level (-1: the codec's default).</summary>
    public int CompressionLevel { get; init; } = -1;
}

/// <summary>
/// CSV read and written by the managed reader and writer (Seseragi.Files): frames come in and go out as Arrow record
/// batches. Paths (files, directories, globs, URLs of a store) are read as datasets (<see cref="CsvDataset"/>: the
/// files' columns united by name, the types inferred from the first files); memory and streams as one input. Scans
/// infer the columns once, when made, and read the data each time the plan runs (the columns and leading rows the
/// query needs; the engine filters the rows: a predicate is pushed down only by paths that hold it as IR, such as
/// LINQ). Writes and sinks encode on every core, sinks batch by batch as the streaming engine produces them;
/// partitioned sinks write Hive partitions (<see cref="CsvDatasetWriter"/>).
/// </summary>
/// <remarks>
/// Batches read are handed to the engine without being disposed here: exporting them pins their native buffers until
/// the engine releases them. Handles given to sinks are consumed (also when the sink fails).
/// </remarks>
public static class ManagedCsv
{
    /// <summary>The inference lengths from which the whole input is taken (as the engine's reader did).</summary>
    private const ulong WholeInputFrom = 16_777_216;

    // ================================================================= encodings

    /// <summary>The encoding of a code page (932, 1252, 65001 …), legacy code pages included (no provider registered).</summary>
    /// <exception cref="ArgumentException">.NET knows no encoding of that code page.</exception>
    internal static System.Text.Encoding EncodingOf(int codePage)
    {
        // 0 is the machine's ANSI code page on Windows: a file's encoding never depends on where it is read.
        if (codePage <= 0)
            throw new ArgumentOutOfRangeException(nameof(codePage), codePage, "A code page names one encoding (932 for Shift_JIS, 1252 for Windows-1252 …): 0 and below name none.");
        try
        {
            return Kaminari.Sazanami.Text.TextEncodings.Resolve(codePage);
        }
        catch (Exception e) when (e is NotSupportedException or ArgumentException)
        {
            throw new ArgumentException(
                $"Code page {codePage} is no text encoding .NET knows: give another code page (932 for Shift_JIS, 1252 for Windows-1252 …) or a name.", nameof(codePage), e);
        }
    }

    /// <summary>The encoding of a name ("shift_jis", "gbk", "windows-1252" …; case does not matter).</summary>
    /// <exception cref="ArgumentException">.NET knows no encoding of that name.</exception>
    internal static System.Text.Encoding EncodingOf(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        try
        {
            return Kaminari.Sazanami.Text.TextEncodings.Resolve(name);
        }
        catch (Exception e) when (e is NotSupportedException or ArgumentException)
        {
            throw new ArgumentException(
                $"'{name}' names no text encoding .NET knows: give a name such as \"shift_jis\", \"gbk\" or \"windows-1252\", or a code page.", nameof(name), e);
        }
    }

    // ================================================================= options

    /// <summary>How the files are written and read (the given types put in by the dataset or the reader).</summary>
    private static CsvReadOptions Format(CsvScanArgs a, IReadOnlyDictionary<string, Apache.Arrow.Types.IArrowType>? overrides) => new()
    {
        Separator = a.Separator,
        Quote = a.QuoteChar is null or '\0' ? null : a.QuoteChar,
        LineEnd = a.EolChar == '\n' ? null : a.EolChar,
        HasHeader = a.HasHeader,
        CommentPrefix = a.CommentPrefix,
        SkipLines = Count(a.SkipLines),
        SkipRows = Count(a.SkipRows),
        SkipRowsAfterHeader = Count(a.SkipRowsAfterHeader),
        NullValues = a.NullValues,
        MissingIsNull = a.MissingIsNull,
        TruncateRaggedLines = a.TruncateRaggedLines,
        DecimalComma = a.DecimalComma,
        ThousandsSeparator = a.ThousandsSeparator,
        TrueValues = a.TrueValues,
        FalseValues = a.FalseValues,
        TryParseDates = a.TryParseDates,
        Encoding = a.Encoding,
        InvalidText = a.LossyEncoding ? CsvInvalidText.Replace : CsvInvalidText.Error,
        SchemaOverrides = overrides,
        InferSchemaLength = a.InferSchemaLength switch { null => null, > WholeInputFrom => null, var n => (int)n.Value },
        RaiseIfEmpty = a.RaiseIfEmpty,
        IgnoreErrors = a.IgnoreErrors,
        MaxDegreeOfParallelism = Threads(a),
        LeaveOpen = true,
    };

    private static long Count(ulong n) => (long)Math.Min(n, long.MaxValue);

    private static int Threads(CsvScanArgs a) => a.NThreads is { } n and > 0 ? (int)Math.Min(n, int.MaxValue) : Environment.ProcessorCount;

    private static long? RowLimit(ulong? rows) => rows is { } n ? (long)Math.Min(n, long.MaxValue) : null;

    private static uint IndexOffset(ulong offset) =>
        offset <= uint.MaxValue ? (uint)offset : throw new ArgumentOutOfRangeException(nameof(offset), offset, "The row index starts past what UInt32 holds.");

    /// <summary>A dataset's options: the files' format, the types given, inference over the first files, extra columns.</summary>
    private static CsvDatasetOptions DatasetOptions(CsvScanArgs a, GivenTypes given, IFileStore? store) => new()
    {
        Store = store,
        Format = Format(a, given.Overrides),
        Schema = given.Schema,
        InferSchemaFiles = a.InferSchemaFiles switch { null or ulong.MaxValue => 10, 0 => 0, var n => (int)Math.Min(n.Value, int.MaxValue) },
        AllowMissingColumns = a.AllowMissingColumns,
        IgnoreExtraColumns = a.IgnoreExtraColumns,
        FilePathColumn = a.IncludeFilePaths,
        Partitioning = a.HivePartitioning ? HivePartitioning.On : HivePartitioning.Off,
        Glob = a.Glob,
    };

    // A run's options: what the query reads (the dataset or the reader keeps how the files are written).
    private static CsvReadOptions RunOptions(CsvReadOptions format, CsvScanArgs a, string[]? columns, bool indexed, long? limit) => format with
    {
        Columns = columns,
        MaxRows = limit,
        RowIndexColumn = indexed ? a.RowIndexName : null,
        RowIndexOffset = 0,
    };

    // ================================================================= read

    /// <summary>
    /// Reads CSV files into a frame: a file, a directory, a glob, or a URL of any of them on a store (read as a dataset).
    /// </summary>
    /// <param name="path">The file, directory or glob (a URL for a store).</param>
    /// <param name="columns">The columns read, in this order (all when null).</param>
    /// <param name="args">How the files are read.</param>
    /// <param name="cloud">The store's settings (a local path takes none).</param>
    public static DataFrameHandle ReadCsv(string path, string[]? columns, CsvScanArgs args, CloudArgs? cloud)
    {
        using CsvSource source = OpenDataset(path, args, cloud);
        return source.Read(columns, args);
    }

    /// <summary>Reads CSV bytes in place (they stay unchanged during the call).</summary>
    public static DataFrameHandle ReadCsv(ReadOnlyMemory<byte> bytes, string[]? columns, CsvScanArgs args)
    {
        using var given = new GivenTypes(args.Schema, args.DtypeOverride, args.IgnoreErrors);
        CsvReadOptions format = Format(args, given.Overrides) with { Schema = given.Schema };
        using var source = new CsvSource(null, format, Open: options => CsvReader.Open(bytes, options), Dataset: null, Owner: null);
        return given.CastBack(source.Read(columns, args));
    }

    /// <summary>Reads CSV bytes in place, pinned during the call.</summary>
    public static unsafe DataFrameHandle ReadCsv(ReadOnlySpan<byte> bytes, string[]? columns, CsvScanArgs args)
    {
        // The read is eager: it ends before this returns, the span pinned throughout.
        fixed (byte* p = bytes)
            return ReadCsv(PointerMemory.Of(p, bytes.Length), columns, args);
    }

    /// <summary>Reads a stream (from its position; left open) as it arrives.</summary>
    public static DataFrameHandle ReadCsv(Stream stream, string[]? columns, CsvScanArgs args)
    {
        ArgumentNullException.ThrowIfNull(stream);
        using var given = new GivenTypes(args.Schema, args.DtypeOverride, args.IgnoreErrors);
        CsvReadOptions format = Format(args, given.Overrides) with { Schema = given.Schema };
        bool once = false;
        using var source = new CsvSource(null, format, Open: options => !once && (once = true) ? CsvReader.Open(stream, options) : throw new InvalidOperationException("A stream is read once."),
            Dataset: null, Owner: null);
        return given.CastBack(source.Read(columns, args));
    }

    /// <summary>A dataset of the files at a path (listed, their columns inferred), the types given kept for the cast back.</summary>
    private static CsvSource OpenDataset(string path, CsvScanArgs args, CloudArgs? cloud)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(args);
        bool url = ManagedStores.IsUrl(path);
        if (!url && !File.Exists(path) && !Directory.Exists(path) && (!args.Glob || path.IndexOfAny(['*', '?', '[']) < 0))
            throw new FileNotFoundException($"CSV file not found: {path}", path);
        var (store, owner) = ManagedStores.Open([path], (cloud ?? CloudArgs.Local).WithProviderFrom(path));
        GivenTypes? given = null;
        try
        {
            given = new GivenTypes(args.Schema, args.DtypeOverride, args.IgnoreErrors);
            CsvDatasetOptions options = DatasetOptions(args, given, store);
            return new CsvSource(given, options.Format, Open: null, CsvDataset.Open(path, options), owner);
        }
        catch
        {
            given?.Dispose();
            owner?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// What an eager read reads from: a dataset (paths), or one input opened by <see cref="Open"/>; the types given, cast
    /// back once read.
    /// </summary>
    private sealed record CsvSource(GivenTypes? Given, CsvReadOptions Format, Func<CsvReadOptions, CsvReader>? Open, CsvDataset? Dataset, IDisposable? Owner)
        : IDisposable
    {
        /// <summary>
        /// The rows read into a frame (one reader opened: a stream is read once): the columns asked in their order (the
        /// row index among them when named, as the engine selects them after its scan), a row index numbered as the
        /// engine numbers rows, the types given cast back (a dataset's).
        /// </summary>
        public DataFrameHandle Read(string[]? columns, CsvScanArgs args)
        {
            bool indexed = args.RowIndexName is { } index && (columns is null || columns.Contains(index));
            string[]? read = columns is null ? null : [.. columns.Where(c => c != args.RowIndexName)];
            long? limit = RowLimit(args.NRows);
            CsvDatasetReader? datasetReader = null;
            CsvReader? reader = null;
            try
            {
                Schema schema;
                if (Dataset is { } dataset)
                {
                    datasetReader = dataset.OpenReader(RunOptions(new CsvReadOptions { MaxDegreeOfParallelism = Format.MaxDegreeOfParallelism, IgnoreErrors = args.IgnoreErrors }, args, read, indexed, limit));
                    schema = datasetReader.Schema;
                }
                else
                {
                    reader = Open!(RunOptions(Format, args, read, indexed, limit));
                    schema = reader.Schema;
                }
                var scan = new Scan(schema, indexed ? args.RowIndexName : null, IndexOffset(args.RowIndexOffset), limit);
                IEnumerable<RecordBatch> batches = (datasetReader?.ReadBatches() ?? reader!.ReadBatches()).Select(b => scan.Deliver(b, indexed));
                Schema delivered = Delivered(scan.Schema, columns);
                DataFrameHandle df = Import(Ordered(batches, delivered), delivered, args.Rechunk);
                return Given is null ? df : Given.CastBack(df);
            }
            finally
            {
                datasetReader?.Dispose();
                reader?.Dispose();
            }
        }

        public void Dispose()
        {
            Dataset?.Dispose();
            Owner?.Dispose();
            Given?.Dispose();
        }
    }

    // The schema of the frame read: the columns asked, in their order (else all), as the scan delivers them.
    private static Schema Delivered(Schema scan, string[]? columns)
    {
        if (columns is null) return scan;
        return new Schema([.. columns.Select(c => scan.GetFieldByName(c) ?? throw new ArgumentException($"The CSV has no column '{c}'.", nameof(columns)))], scan.Metadata);
    }

    // Batches with their columns in the schema's order (nothing copied).
    private static IEnumerable<RecordBatch> Ordered(IEnumerable<RecordBatch> batches, Schema schema)
    {
        foreach (RecordBatch batch in batches)
        {
            if (batch.Schema.FieldsList.Select(f => f.Name).SequenceEqual(schema.FieldsList.Select(f => f.Name))) yield return batch;
            else yield return new RecordBatch(schema, [.. schema.FieldsList.Select(f => batch.Column(batch.Schema.GetFieldIndex(f.Name)))], batch.Length);
        }
    }

    /// <summary>
    /// The batches into a frame: chunked as read (the engine's default for CSV), or contiguous with
    /// <paramref name="rechunk"/> (concatenated on every core; columns that cannot be, rechunked by the engine).
    /// </summary>
    private static DataFrameHandle Import(IEnumerable<RecordBatch> read, Schema schema, bool rechunk)
    {
        List<RecordBatch> batches = [];
        try
        {
            foreach (RecordBatch batch in read) batches.Add(batch);
            if (rechunk && batches.Count > 1 && Concatenated(batches) is { } one)
            {
                foreach (RecordBatch batch in batches) batch.Dispose(); // shared view data stays with the concatenated batch
                batches = [one];
            }
        }
        catch
        {
            foreach (RecordBatch batch in batches) batch.Dispose();
            throw;
        }
        if (!rechunk || batches.Count <= 1) return ArrowStreamInterop.ImportEagerStrict(batches, schema);
        using DataFrameHandle chunked = ArrowStreamInterop.ImportEagerStrict(batches, schema);
        return PolarsWrapper.DataFrameRechunk(chunked);

        static RecordBatch? Concatenated(List<RecordBatch> batches)
        {
            try
            {
                return ArrowConcat.Concatenate(batches, Environment.ProcessorCount);
            }
            catch (Exception e) when (e is NotSupportedException or OverflowException)
            {
                return null;
            }
        }
    }

    // ================================================================= scan

    /// <summary>
    /// A lazy scan of CSV files: a file, a directory, a glob, or a URL of any of them on a store. The files are listed
    /// and the columns inferred now (from the first files); the data is read each time the plan runs.
    /// </summary>
    /// <param name="path">The file, directory or glob (a URL for a store).</param>
    /// <param name="args">How the files are read.</param>
    /// <param name="cloud">The store's settings (a local path takes none).</param>
    public static LazyFrameHandle ScanCsv(string path, CsvScanArgs args, CloudArgs? cloud)
    {
        CsvSource source = OpenDataset(path, args, cloud);
        CsvDataset dataset = source.Dataset!;
        Scan scan;
        try
        {
            Schema read;
            using (CsvDatasetReader probe = dataset.OpenReader(new CsvReadOptions { RowIndexColumn = args.RowIndexName })) read = probe.Schema;
            scan = new Scan(read, args.RowIndexName, IndexOffset(args.RowIndexOffset), limit: null);
        }
        catch
        {
            source.Dispose();
            throw;
        }
        int threads = Threads(args);
        GivenTypes given = source.Given!;
        // The scan owns the source (the dataset, its store, the types given) from here on.
        LazyFrameHandle lf = ManagedScans.Open($"csv[{ManagedScans.Shown(path)}]", Fingerprint(path, args, cloud, scan.Schema), scan,
            args.RowIndexName, Batches, source);
        return given.CastBack(ManagedScans.Head(lf, RowLimit(args.NRows)));

        IEnumerable<RecordBatch> Batches(ScanRun run)
        {
            var options = RunOptions(new CsvReadOptions { MaxDegreeOfParallelism = threads, IgnoreErrors = args.IgnoreErrors }, args, run.Columns, run.Indexed, run.Limit)
                with { Filter = run.Filter, FilterRows = run.Filter is not null };
            using CsvDatasetReader reader = dataset.OpenReader(options);
            foreach (RecordBatch batch in reader.ReadBatches()) yield return batch;
        }
    }

    /// <summary>
    /// A path scan's fingerprint: what decides the data it reads (the paths, the store, how the files are read, the
    /// columns as typed); not what only shapes the work (threads, rechunking) or is applied above the scan (the rows
    /// limit, the types cast back).
    /// </summary>
    internal static ulong Fingerprint(string path, CsvScanArgs a, CloudArgs? cloud, Schema scan) => new FingerprintBuilder()
        .Add("csv").Add(path).Add(cloud).Add(scan)
        .Add(a.HasHeader).Add(a.Separator).Add(a.QuoteChar).Add(a.EolChar).Add(a.IgnoreErrors).Add(a.TryParseDates).Add(a.Glob)
        .Add(a.RaiseIfEmpty).Add(a.SkipRows).Add(a.SkipRowsAfterHeader).Add(a.SkipLines).Add(a.InferSchemaLength).Add(a.InferSchemaFiles)
        .Add(a.RowIndexName).Add(a.RowIndexOffset).Add(a.IncludeFilePaths).Add(a.Encoding).Add(a.LossyEncoding).Add(a.NullValues)
        .Add(a.MissingIsNull).Add(a.CommentPrefix).Add(a.DecimalComma).Add(a.TruncateRaggedLines).Add(a.TrueValues).Add(a.FalseValues)
        .Add(a.ThousandsSeparator).Add(a.AllowMissingColumns).Add(a.IgnoreExtraColumns).Add(a.HivePartitioning)
        .Value;

    /// <summary>
    /// A lazy scan of CSV in memory, read in place each time the plan runs: <paramref name="owner"/> (what keeps the
    /// memory: a copy, a pin) is disposed once the engine releases the scan. The columns are inferred now.
    /// </summary>
    public static LazyFrameHandle ScanCsv(ReadOnlyMemory<byte> bytes, IDisposable owner, CsvScanArgs args)
    {
        ArgumentNullException.ThrowIfNull(owner);
        ArgumentNullException.ThrowIfNull(args);
        GivenTypes given;
        try
        {
            given = new GivenTypes(args.Schema, args.DtypeOverride, args.IgnoreErrors);
        }
        catch
        {
            owner.Dispose();
            throw;
        }
        using (given)
        {
            CsvReadOptions format;
            Scan scan;
            try
            {
                // The columns, decided once: every run reads with them (nothing inferred again).
                format = Format(args, given.Overrides) with { Schema = given.Schema };
                Schema full;
                using (CsvReader probe = CsvReader.Open(bytes, format)) full = probe.Schema;
                format = format with { Schema = full, SchemaOverrides = null, Columns = null };
                Schema read;
                using (CsvReader probe = CsvReader.Open(bytes, format with { RowIndexColumn = args.RowIndexName })) read = probe.Schema;
                scan = new Scan(read, args.RowIndexName, IndexOffset(args.RowIndexOffset), limit: null);
            }
            catch
            {
                owner.Dispose();
                throw;
            }
            // Memory is read by this scan only: no other scan reads the same data.
            LazyFrameHandle lf = ManagedScans.Open("csv[memory]", ManagedScans.Identity(), scan, args.RowIndexName, Batches, owner);
            return given.CastBack(ManagedScans.Head(lf, RowLimit(args.NRows)));

            IEnumerable<RecordBatch> Batches(ScanRun run)
            {
                using CsvReader reader = CsvReader.Open(bytes, RunOptions(format, args, run.Columns, run.Indexed, run.Limit) with { Filter = run.Filter, FilterRows = run.Filter is not null });
                foreach (RecordBatch batch in reader.ReadBatches()) yield return batch;
            }
        }
    }

    /// <summary>A lazy scan of CSV bytes: copied once into native memory the scan keeps.</summary>
    public static LazyFrameHandle ScanCsv(ReadOnlySpan<byte> bytes, CsvScanArgs args)
    {
        var copy = new NativeBufferWriter(Math.Max(1, bytes.Length));
        try
        {
            bytes.CopyTo(copy.GetSpan(bytes.Length));
            copy.Advance(bytes.Length);
        }
        catch
        {
            copy.Dispose();
            throw;
        }
        return ScanCsv(copy.WrittenMemory, copy, args);
    }

    /// <summary>
    /// A lazy scan of CSV in memory read in place: pinned until the engine releases the scan, unchanged meanwhile.
    /// </summary>
    public static unsafe LazyFrameHandle ScanCsv(ReadOnlyMemory<byte> bytes, CsvScanArgs args)
    {
        // Pinned once for the scan's life: the readers see the pointer (pinning it again pins nothing of the caller's).
        MemoryHandle pin = bytes.Pin();
        return ScanCsv(PointerMemory.Of((byte*)pin.Pointer, bytes.Length), new Pinned(pin), args);
    }

    /// <summary>A lazy scan of a stream (from its position; left open): read now, once, into native memory the scan keeps.</summary>
    public static LazyFrameHandle ScanCsv(Stream stream, CsvScanArgs args)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var copy = new NativeBufferWriter(stream.CanSeek ? (int)Math.Clamp(stream.Length - stream.Position, 1, int.MaxValue) : 1 << 16);
        try
        {
            copy.ReadFrom(stream);
        }
        catch
        {
            copy.Dispose();
            throw;
        }
        return ScanCsv(copy.WrittenMemory, copy, args);
    }

    // ================================================================= write a frame

    private static CsvWriteOptions WriteOptions(CsvWriteArgs w) => new()
    {
        IncludeHeader = w.IncludeHeader,
        IncludeBom = w.IncludeBom,
        Separator = w.Separator,
        Quote = w.QuoteChar,
        QuoteStyle = w.QuoteStyle switch
        {
            PlQuoteStyle.Necessary => CsvQuoteStyle.Necessary,
            PlQuoteStyle.Always => CsvQuoteStyle.Always,
            PlQuoteStyle.NonNumeric => CsvQuoteStyle.NonNumeric,
            PlQuoteStyle.Never => CsvQuoteStyle.Never,
            _ => throw new ArgumentOutOfRangeException(nameof(w), w.QuoteStyle, "Unknown quote style."),
        },
        NullValue = w.NullValue ?? "",
        LineTerminator = w.LineTerminator ?? "\n",
        FloatScientific = w.FloatScientific,
        FloatPrecision = w.FloatPrecision,
        DecimalComma = w.DecimalComma,
        DateFormat = w.DateFormat,
        TimeFormat = w.TimeFormat,
        DatetimeFormat = w.DatetimeFormat,
        ThousandsSeparator = w.ThousandsSeparator,
        Encoding = w.Encoding,
        InvalidText = w.LossyEncoding ? CsvInvalidText.Replace : CsvInvalidText.Error,
        Compression = w.Compression switch
        {
            PlExternalCompression.Uncompressed => CsvCompression.None,
            PlExternalCompression.Gzip => CsvCompression.Gzip,
            PlExternalCompression.ZSTD => CsvCompression.Zstd,
            _ => throw new ArgumentOutOfRangeException(nameof(w), w.Compression, "Unknown compression."),
        },
        CompressionLevel = w.CompressionLevel < 0 || w.Compression == PlExternalCompression.Uncompressed ? null : w.CompressionLevel,
        // The batches exported are handed over: encoded side by side, each released once encoded.
        BatchOwnership = BatchOwnership.Writer,
        LeaveOpen = true,
    };

    // A CSV writer as the shared loops drive it.
    private static FrameWriter Frame(CsvWriter writer) => new(writer.Write, writer.WriteAsync, writer.Complete, writer.CompleteAsync, writer);

    /// <summary>Writes a frame to a file (replaced; a failed write leaves none).</summary>
    public static void WriteCsv(DataFrameHandle df, string path, CsvWriteArgs args) =>
        WriteCsv(df, path, args, checkExtension: false, PlSyncOnClose.None, mkdir: false, cloud: null);

    /// <summary>Writes a frame to a file, local or on a store: published whole once written (a failed write leaves none).</summary>
    public static void WriteCsv(DataFrameHandle df, string path, CsvWriteArgs args, bool checkExtension, PlSyncOnClose syncOnClose, bool mkdir, CloudArgs? cloud)
    {
        ArgumentNullException.ThrowIfNull(path);
        if (checkExtension) CheckExtension(path, args.Compression);
        IDisposable? owner = null;
        try
        {
            FileWriteStream file;
            if (ManagedStores.IsUrl(path))
            {
                (IWritableFileStore store, owner) = OpenStore(path, cloud);
                file = store.OpenWrite(path, createOnly: false);
            }
            else file = LocalWrite(path, mkdir, syncOnClose != PlSyncOnClose.None);
            using (file)
            {
                WriteCsv(df, file, args);
                file.Commit();
            }
        }
        finally
        {
            owner?.Dispose();
        }
    }

    /// <summary>Writes a frame to a stream (flushed, left open).</summary>
    public static void WriteCsv(DataFrameHandle df, Stream stream, CsvWriteArgs args)
    {
        ArgumentNullException.ThrowIfNull(stream);
        WriteFrameAsync(df, schema => Frame(CsvWriter.Create(stream, schema, WriteOptions(args))), false, default).GetAwaiter().GetResult();
    }

    /// <summary>Writes a frame to a stream with asynchronous writes only (flushed, left open).</summary>
    public static Task WriteCsvAsync(DataFrameHandle df, Stream stream, CsvWriteArgs args, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return WriteFrameAsync(df, schema => Frame(CsvWriter.Create(stream, schema, WriteOptions(args))), true, cancellationToken);
    }

    /// <summary>Writes a frame to a buffer writer (a <see cref="PipeWriter"/> is flushed as it fills, never completed).</summary>
    public static void WriteCsv(DataFrameHandle df, IBufferWriter<byte> output, CsvWriteArgs args)
    {
        ArgumentNullException.ThrowIfNull(output);
        WriteFrameAsync(df, schema => Frame(CsvWriter.Create(output, schema, WriteOptions(args))), false, default).GetAwaiter().GetResult();
    }

    /// <summary>Writes a frame to a pipe with asynchronous flushes (flushed, not completed).</summary>
    public static Task WriteCsvAsync(DataFrameHandle df, PipeWriter writer, CsvWriteArgs args, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return WriteFrameAsync(df, schema => Frame(CsvWriter.Create(writer, schema, WriteOptions(args))), true, cancellationToken);
    }

    /// <summary>A frame as CSV bytes.</summary>
    public static byte[] WriteCsvToMemory(DataFrameHandle df, CsvWriteArgs args)
    {
        using var output = new NativeBufferWriter();
        WriteCsv(df, output, args);
        return output.WrittenSpan.ToArray();
    }

    /// <summary>A frame as CSV in native memory, handed over without a copy.</summary>
    public static NativeBuffer WriteCsvNativeMemory(DataFrameHandle df, CsvWriteArgs args) => ToNative(stream => WriteCsv(df, stream, args));

    // ================================================================= sink a query

    // The query's columns (borrowed), for a sink without rows (its header).
    private static Schema QuerySchema(LazyFrameHandle lf)
    {
        using SchemaHandle schema = PolarsWrapper.GetLazySchema(lf);
        return ArrowSchemaOf(schema);
    }

    private static Task Sink(LazyFrameHandle lf, Func<Schema, FrameWriter> create, bool maintainOrder, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(lf);
        Schema declared;
        try
        {
            declared = QuerySchema(lf);
        }
        catch
        {
            lf.Dispose();
            throw;
        }
        return SinkAsync(lf, create, maintainOrder, useAsync, ct, () => declared);
    }

    /// <summary>
    /// Sinks a query (consumed) as CSV into a file, local or on a store: published whole once written (a failed sink
    /// leaves none).
    /// </summary>
    public static void SinkCsv(LazyFrameHandle lf, string path, CsvWriteArgs args, bool checkExtension, bool maintainOrder, PlSyncOnClose syncOnClose,
        bool mkdir, CloudArgs? cloud)
    {
        ArgumentNullException.ThrowIfNull(path);
        FileWriteStream file;
        IDisposable? owner = null;
        try
        {
            if (checkExtension) CheckExtension(path, args.Compression);
            if (ManagedStores.IsUrl(path))
            {
                (IWritableFileStore store, owner) = OpenStore(path, cloud);
                file = store.OpenWrite(path, createOnly: false);
            }
            else file = LocalWrite(path, mkdir, syncOnClose != PlSyncOnClose.None);
        }
        catch
        {
            lf.Dispose();
            owner?.Dispose();
            throw;
        }
        try
        {
            using (file)
            {
                Sink(lf, schema => Frame(CsvWriter.Create(file, schema, WriteOptions(args))), maintainOrder, false, default).GetAwaiter().GetResult();
                file.Commit();
            }
        }
        finally
        {
            owner?.Dispose();
        }
    }

    /// <summary>Sinks a query (consumed) as CSV into a stream (flushed, left open).</summary>
    public static void SinkCsv(LazyFrameHandle lf, Stream stream, CsvWriteArgs args, bool maintainOrder)
    {
        ArgumentNullException.ThrowIfNull(stream);
        Sink(lf, schema => Frame(CsvWriter.Create(stream, schema, WriteOptions(args))), maintainOrder, false, default).GetAwaiter().GetResult();
    }

    /// <summary>Sinks a query (consumed) into a stream with asynchronous writes only, the engine read off the caller's thread.</summary>
    public static Task SinkCsvAsync(LazyFrameHandle lf, Stream stream, CsvWriteArgs args, bool maintainOrder, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return Sink(lf, schema => Frame(CsvWriter.Create(stream, schema, WriteOptions(args))), maintainOrder, true, cancellationToken);
    }

    /// <summary>Sinks a query (consumed) into a pipe with asynchronous flushes (flushed, not completed).</summary>
    public static Task SinkCsvAsync(LazyFrameHandle lf, PipeWriter writer, CsvWriteArgs args, bool maintainOrder, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return Sink(lf, schema => Frame(CsvWriter.Create(writer, schema, WriteOptions(args))), maintainOrder, true, cancellationToken);
    }

    /// <summary>A query's rows (consumed) as CSV bytes.</summary>
    public static byte[] SinkCsvToMemory(LazyFrameHandle lf, CsvWriteArgs args, bool maintainOrder)
    {
        using var output = new NativeBufferWriter();
        Sink(lf, schema => Frame(CsvWriter.Create(output, schema, WriteOptions(args))), maintainOrder, false, default).GetAwaiter().GetResult();
        return output.WrittenSpan.ToArray();
    }

    /// <summary>A query's rows (consumed) as CSV in native memory, handed over without a copy.</summary>
    public static NativeBuffer SinkCsvNativeMemory(LazyFrameHandle lf, CsvWriteArgs args, bool maintainOrder) =>
        ToNative(stream => SinkCsv(lf, stream, args, maintainOrder));

    // ================================================================= partitioned

    /// <summary>
    /// Sinks a query (consumed) as a dataset of CSV files under <paramref name="path"/>, in Hive partitions of
    /// <paramref name="partitionBy"/> (<c>key=value</c> directories), files named <c>part-{index:D5}-{write id}.csv</c>,
    /// each with its header, cut by rows and size, each published whole.
    /// </summary>
    public static void SinkCsvPartitioned(LazyFrameHandle lf, string path, string[] partitionBy, bool includeKeys, bool keysPreGrouped,
        ulong maxRowsPerFile, ulong approxBytesPerFile, CsvWriteArgs args, bool maintainOrder, PlSyncOnClose syncOnClose, ExistingData existingData,
        CloudArgs? cloud)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(partitionBy);
        IDisposable? owner = null;
        CsvDatasetWriter? writer = null;
        try
        {
            IWritableFileStore? store = null;
            if (ManagedStores.IsUrl(path)) (store, owner) = OpenStore(path, cloud);
            var options = new CsvDatasetWriteOptions
            {
                Store = store,
                PartitionBy = partitionBy,
                IncludePartitionColumns = includeKeys,
                KeysPreGrouped = keysPreGrouped,
                ExistingData = existingData,
                MaxRowsPerFile = maxRowsPerFile == 0 ? null : (long)Math.Min(maxRowsPerFile, long.MaxValue),
                ApproxBytesPerFile = approxBytesPerFile == 0 ? null : (long)Math.Min(approxBytesPerFile, long.MaxValue),
                SyncOnCommit = syncOnClose != PlSyncOnClose.None,
                Writer = WriteOptions(args) with { BatchOwnership = BatchOwnership.Caller },
            };
            using CollectBatchesHandle batches = PolarsWrapper.LazyCollectBatches(lf, PlEngine.Streaming, maintainOrder);
            while (true)
            {
                using DataFrameHandle frame = PolarsWrapper.CollectBatchesNext(batches, out bool done);
                if (done || frame.IsInvalid) break;
                using Apache.Arrow.Ipc.IArrowArrayStream chunks = ArrowStreamInterop.ExportToStream(frame, wideIntegers: true);
                while (chunks.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult() is { } chunk)
                {
                    using (chunk)
                    {
                        writer ??= CsvDatasetWriter.Create(path, chunk.Schema, options);
                        writer.Write(chunk);
                    }
                }
            }
            writer?.Complete();
        }
        catch
        {
            writer?.Abort();
            throw;
        }
        finally
        {
            lf.Dispose();
            writer?.Dispose();
            owner?.Dispose();
        }
    }
}
