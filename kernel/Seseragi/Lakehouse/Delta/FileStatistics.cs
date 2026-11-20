using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.Unicode;
using System.Diagnostics.CodeAnalysis;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;
using Kaminari.Seseragi.Files.Parquet.Thrift;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Files.Parquet;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// The <c>stats</c> of a data file, from its Parquet footer: the row count, and for the leaves of the columns (struct
/// fields at any depth, nested as Spark nests them; not arrays or maps) the null count and (not for booleans and
/// binary) the minimum and maximum.
/// </summary>
/// <remarks>
/// Only what the footer proves is written: a bound is left out when a row group with values has no bound, a null
/// count when a row group has none. Bounds follow Spark: timestamps cut to milliseconds, a string minimum cut to
/// <see cref="StringPrefixLength"/> characters, a longer string maximum left out (a cut one would not be an upper
/// bound), NaN and infinite bounds left out.
/// </remarks>
public static class FileStatistics
{
    /// <summary>The longest string kept whole in statistics.</summary>
    public const int StringPrefixLength = 32;

    /// <summary>The stats JSON of a data file, for every leaf of the columns (struct fields at any depth; not arrays or maps).</summary>
    /// <param name="metadata">The file footer.</param>
    /// <param name="dataFields">The table columns the file holds (not the partition columns), found in the file by their physical names.</param>
    public static string FromParquet(ParquetMetadata metadata, IReadOnlyList<StructField> dataFields)
    {
        ArgumentNullException.ThrowIfNull(dataFields);
        return FromParquet(metadata, new StatisticsLeaves(Leaves(dataFields, _ => true)));
    }

    /// <summary>
    /// The stats JSON of a data file for the given leaves: paths of fields from a table column down to a field that
    /// is not a struct (see <see cref="IndexedLeaves(IReadOnlyList{StructField}, IReadOnlyDictionary{string, string}?)"/>). Nested fields' statistics nest under their structs, keyed
    /// by physical names, as Spark writes them. A leaf inside a struct counts as null where the struct is null.
    /// </summary>
    /// <param name="metadata">The file footer.</param>
    /// <param name="leaves">The leaves to give statistics for; arrays and maps get none.</param>
    public static string FromParquet(ParquetMetadata metadata, IReadOnlyList<StructField[]> leaves)
    {
        ArgumentNullException.ThrowIfNull(leaves);
        return FromParquet(metadata, new StatisticsLeaves(leaves));
    }

    /// <summary>
    /// <see cref="FromParquet(ParquetMetadata, IReadOnlyList{StructField[]})"/> with the leaves prepared once for the
    /// files of a write.
    /// </summary>
    internal static string FromParquet(ParquetMetadata metadata, StatisticsLeaves leaves)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        ArgumentNullException.ThrowIfNull(leaves);
        LeafPlacement placement = Placed(leaves, metadata.Schema, topLevel: false);
        LeafStatistics[] results = Rent(placement.Count);
        try
        {
            GatherParsed(metadata, placement, results);
            return Render(metadata.RowCount, leaves, results, []);
        }
        finally
        {
            results.AsSpan(0, placement.Count).Clear(); // let the footer's bound arrays go
        }
    }

    /// <summary>
    /// The stats JSON of a data file from its footer as its writer wrote it: read in place from the Thrift bytes (the
    /// schema, shared by the files of a write and placed once; the row counts; the statistics of the leaves' chunks;
    /// nothing else read, nothing parsed into objects). A footer read elsewhere, or bytes not as a valid footer has
    /// them, is read from its metadata (parsed, with the errors of parsing).
    /// </summary>
    internal static string FromFooter(ParquetFooter footer, StatisticsLeaves leaves)
    {
        ArgumentNullException.ThrowIfNull(footer);
        ArgumentNullException.ThrowIfNull(leaves);
        if (footer.HasBytes)
        {
            ReadOnlySpan<byte> thrift = footer.Thrift;
            if (GatherBytes(thrift, leaves, topLevel: false, out LeafPlacement? placement, out LeafStatistics[]? results, out long rows))
                return Render(rows, leaves, results, thrift);
        }
        return FromParquet(footer.Metadata, leaves);
    }

    /// <summary>Per thread: the statistics of the leaves of the file being described.</summary>
    [ThreadStatic] private static LeafStatistics[]? t_results;

    /// <summary>Per thread: the chunks of one row group, by leaf, while a footer's bytes are read.</summary>
    [ThreadStatic] private static Statistic[]? t_chunks;

    /// <summary>This thread's statistics of <paramref name="count"/> leaves, cleared.</summary>
    private static LeafStatistics[] Rent(int count)
    {
        LeafStatistics[] results = t_results is { } kept && kept.Length >= count ? kept : t_results = new LeafStatistics[Math.Max(count, 16)];
        results.AsSpan(0, count).Clear();
        return results;
    }

    /// <summary>What a leaf's statistics are in one file: gathered row group by row group, then what is written.</summary>
    private struct LeafStatistics
    {
        public Bound Min, Max;
        public long NullCount;
        public bool Missing, NullsUnknown, MinUnknown, MaxUnknown; // while gathering
        public bool HasMin, HasMax, HasNullCount;                  // what is written
    }

    private enum Part { Min, Max, Nulls }

    /// <summary>The bytes of a bound: a whole array (a parsed footer's) or a range of a footer's Thrift bytes; none by default.</summary>
    private readonly record struct Slice(byte[]? Array, int Start, int Length, bool Present)
    {
        public static Slice Of(byte[]? array) => array is null ? default : new(array, 0, array.Length, true);

        public ReadOnlySpan<byte> In(ReadOnlySpan<byte> footer) => Array is { } array ? array : footer.Slice(Start, Length);
    }

    /// <summary>The statistics of one column chunk, as the footer has them.</summary>
    private readonly record struct Statistic(bool Exists, Slice Min, Slice Max, long? NullCount);

    /// <summary>
    /// Where leaves are in one file's schema: each one's schema element (-1 where the file has none) and column chunk
    /// (its ordinal among the schema's leaves), and the leaves each chunk gives statistics for.
    /// </summary>
    private sealed class LeafPlacement
    {
        public required object Key { get; init; }
        public required IReadOnlyList<ParquetSchemaElement> Schema { get; init; }
        public required string[][] Paths { get; init; }
        /// <summary>The paths in UTF-8, encoded when first asked for (only a footer's bytes are read by them).</summary>
        public byte[][][] Utf8Paths => _utf8Paths ??= [.. Paths.Select(static path => path.Select(Encoding.UTF8.GetBytes).ToArray())];

        private byte[][][]? _utf8Paths;
        public required DeltaType[] Types { get; init; }
        public required int[] Element { get; init; }
        public required int[] Ordinal { get; init; }
        public required int LeafCount { get; init; }
        public required int[][] LeavesAt { get; init; }

        public int Count => Paths.Length;
    }

    /// <summary>A type with no bounds: the top-level leaves of <see cref="NullCounts(ParquetMetadata)"/> are counted only.</summary>
    private static readonly PrimitiveType Counted = new(PrimitiveType.Boolean);

    /// <summary>
    /// Places <paramref name="leaves"/> in <paramref name="schema"/>, or (without leaves) its top-level leaves that
    /// are not repeated, each name once, for their null counts.
    /// </summary>
    private static LeafPlacement Place(object key, IReadOnlyList<ParquetSchemaElement> schema, StatisticsLeaves? leaves)
    {
        int elements = schema.Count;
        int[] ends = new int[elements], ordinals = new int[elements];
        (int rootEnd, int leafCount) = Index(schema, ends, ordinals);
        string[][] paths;
        DeltaType[] types;
        int[] element;
        if (leaves is null)
        {
            List<int> found = [];
            HashSet<string> names = [];
            for (int at = 1; at < rootEnd; at = ends[at])
            {
                if (schema[at].ChildCount == 0 && schema[at].Repetition != 2 && names.Add(schema[at].Name)) found.Add(at);
            }
            element = [.. found];
            paths = [.. found.Select(at => (string[])[schema[at].Name])];
            types = [.. found.Select(_ => (DeltaType)Counted)];
        }
        else
        {
            (paths, types) = (leaves.Paths, leaves.Types);
            element = [.. paths.Select(path => Find(schema, ends, rootEnd, path))];
        }
        int[] ordinal = [.. element.Select(e => e < 0 ? -1 : ordinals[e])];
        var byChunk = new List<int>?[leafCount];
        for (int i = 0; i < ordinal.Length; i++)
        {
            if (ordinal[i] >= 0) (byChunk[ordinal[i]] ??= []).Add(i);
        }
        return new LeafPlacement
        {
            Key = key, Schema = schema, Paths = paths, Types = types, Element = element, Ordinal = ordinal,
            LeafCount = leafCount, LeavesAt = [.. byChunk.Select(l => l is null ? [] : l.ToArray())],
        };

        // Indexes the flattened schema: for each element the index after its subtree and the ordinal of its first leaf (a
        // leaf's column chunk in every row group). Returns the index after the root's children, and the leaves.
        static (int RootEnd, int Leaves) Index(IReadOnlyList<ParquetSchemaElement> schema, int[] ends, int[] ordinals)
        {
            if (schema.Count == 0) return (0, 0);
            int at = 1, leaf = 0;
            for (int child = 0; child < schema[0].ChildCount && at < schema.Count; child++) at = Walk(schema, at, ends, ordinals, ref leaf);
            return (at, leaf);

            static int Walk(IReadOnlyList<ParquetSchemaElement> schema, int at, int[] ends, int[] ordinals, ref int leaf)
            {
                ParquetSchemaElement element = schema[at];
                ordinals[at] = leaf;
                int next = at + 1;
                if (element.ChildCount == 0) leaf++;
                for (int i = 0; i < element.ChildCount; i++) next = Walk(schema, next, ends, ordinals, ref leaf);
                ends[at] = next;
                return next;
            }
        }

        // The schema element of the leaf at path (names from a top-level column down), or -1 when the
        // file has none there or the leaf is repeated or under a repeated group (a list or map). Of equal names, the last.
        static int Find(IReadOnlyList<ParquetSchemaElement> schema, int[] ends, int rootEnd, string[] path)
        {
            int from = 1, to = rootEnd;
            for (int depth = 0; depth < path.Length; depth++)
            {
                int found = -1;
                for (int at = from; at < to; at = ends[at])
                {
                    if (schema[at].Name == path[depth]) found = at;
                }
                if (found < 0 || schema[found].Repetition == 2) return -1;
                bool last = depth == path.Length - 1;
                if (last != (schema[found].ChildCount == 0)) return -1;
                (from, to) = (found + 1, ends[found]);
                if (last) return found;
            }
            return -1;
        }
    }

    /// <summary>The placement for a parsed schema: the last one of the write when its files' schemas are equal, else placed now.</summary>
    private static LeafPlacement Placed(StatisticsLeaves leaves, IReadOnlyList<ParquetSchemaElement> schema, bool topLevel)
    {
        if ((topLevel ? leaves.PlacedTopLevel : leaves.Placed) is LeafPlacement kept && Same(kept.Schema, schema)) return kept;
        LeafPlacement placement = Place(schema, schema, topLevel ? null : leaves);
        if (topLevel) leaves.PlacedTopLevel = placement;
        else leaves.Placed = placement;
        return placement;

        static bool Same(IReadOnlyList<ParquetSchemaElement> a, IReadOnlyList<ParquetSchemaElement> b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a.Count != b.Count) return false;
            for (int i = 0; i < a.Count; i++)
            {
                if (!a[i].Equals(b[i])) return false;
            }
            return true;
        }
    }

    /// <summary>Gathers the statistics of the placed leaves from a parsed footer (its Thrift objects, or its views alone).</summary>
    private static void GatherParsed(ParquetMetadata metadata, LeafPlacement placement, LeafStatistics[] results)
    {
        var chunks = new Chunks(metadata);
        int groups = chunks.GroupCount;
        for (int g = 0; g < groups; g++)
        {
            long rows = chunks.Rows(g);
            for (int i = 0; i < placement.Count; i++)
            {
                if (placement.Element[i] < 0 || results[i].Missing) continue;
                if (chunks.TryGet(g, placement.Ordinal[i], placement.Paths[i], out Statistic chunk))
                    Fold(ref results[i], chunk, rows, placement.Types[i], placement.Schema[placement.Element[i]], []);
                else results[i].Missing = true;
            }
        }
        Finish(placement, results, metadata.RowCount, []);
    }

    /// <summary>
    /// Gathers the statistics of the leaves (or of the top-level leaves) from a footer's Thrift bytes, read in place;
    /// false when they are not as a valid footer has them (its row groups before its schema, a column count that is
    /// not the schema's, a chunk without metadata or of another path, an encrypted footer, damage).
    /// </summary>
    private static bool GatherBytes(ReadOnlySpan<byte> footer, StatisticsLeaves leaves, bool topLevel,
        [NotNullWhen(true)] out LeafPlacement? placement, [NotNullWhen(true)] out LeafStatistics[]? results, out long rowCount)
    {
        placement = null;
        results = null;
        rowCount = 0;
        try
        {
            var r = new ThriftCompactReader(footer);
            r.EnterStruct();
            bool hasRows = false, hasGroups = false;
            short last = 0;
            while (r.NextField(ref last, out short id, out byte type))
            {
                switch (id)
                {
                    case 2 when type == ThriftType.List && placement is null:
                        int at = r.Position;
                        r.Skip(type);
                        placement = PlacedRead(leaves, leaves.Interner.Schema(r.Since(at)), topLevel);
                        results = Rent(placement.Count);
                        break;
                    case 3 when type == ThriftType.I64:
                        rowCount = r.ReadI64();
                        hasRows = true;
                        break;
                    case 4 when type == ThriftType.List && placement is not null && !hasGroups:
                        if (!RowGroups(ref r, footer, placement, results!)) return false;
                        hasGroups = true;
                        break;
                    case 2 or 3 or 4 or 8: return false; // out of order, repeated, of another type; 8: encrypted
                    default:
                        r.Skip(type);
                        break;
                }
            }
            if (!hasRows || !hasGroups || placement is null || results is null) return false;
            Finish(placement, results, rowCount, footer);
            return true;
        }
        catch (ParquetFormatException)
        {
            return false;
        }

        // The placement for a schema read from a footer's bytes: the last one of the write when its files share the
        // schema (the interner gives them one array), else placed now (after checking the schema makes a tree: a
        // ParquetFormatException otherwise).
        static LeafPlacement PlacedRead(StatisticsLeaves leaves, SchemaElement[] schema, bool topLevel)
        {
            if ((topLevel ? leaves.PlacedTopLevel : leaves.Placed) is LeafPlacement kept && ReferenceEquals(kept.Key, schema)) return kept;
            int leafCount = SchemaTree.CountLeaves(schema);
            LeafPlacement placement = Place(schema, [.. schema.Select(ParquetMetadata.SchemaElement)], topLevel ? null : leaves);
            if (placement.LeafCount != leafCount) throw new ParquetFormatException("The schema's leaves were miscounted.");
            if (topLevel) leaves.PlacedTopLevel = placement;
            else leaves.Placed = placement;
            return placement;
        }

        // The row groups: per group, its chunks of the placed leaves, folded once its row count is read.
        static bool RowGroups(ref ThriftCompactReader r, ReadOnlySpan<byte> footer, LeafPlacement placement, LeafStatistics[] results)
        {
            int count = r.ReadListHeader(out byte element);
            if (element != ThriftType.Struct) return false;
            Statistic[] chunks = t_chunks is { } kept && kept.Length >= placement.Count ? kept : t_chunks = new Statistic[Math.Max(placement.Count, 16)];
            for (int g = 0; g < count; g++)
            {
                chunks.AsSpan(0, placement.Count).Clear();
                r.EnterStruct();
                long? rows = null;
                bool columns = false;
                short last = 0;
                while (r.NextField(ref last, out short id, out byte type))
                {
                    if (id == 1 && type == ThriftType.List && !columns)
                    {
                        if (!Columns(ref r, placement, chunks)) return false;
                        columns = true;
                    }
                    else if (id == 3 && type == ThriftType.I64) rows = r.ReadI64();
                    else r.Skip(type);
                }
                r.ExitStruct();
                if (rows is not long n || !columns) return false;
                for (int i = 0; i < placement.Count; i++)
                {
                    if (placement.Element[i] >= 0) Fold(ref results[i], chunks[i], n, placement.Types[i], placement.Schema[placement.Element[i]], footer);
                }
            }
            return true;
        }

        // A row group's column chunks: one per schema leaf, each with its metadata; the placed leaves' read.
        static bool Columns(ref ThriftCompactReader r, LeafPlacement placement, Statistic[] chunks)
        {
            int count = r.ReadListHeader(out byte element);
            if (element != ThriftType.Struct || count != placement.LeafCount) return false;
            for (int c = 0; c < count; c++)
            {
                int[] wanted = placement.LeavesAt[c];
                r.EnterStruct();
                bool meta = false;
                short last = 0;
                while (r.NextField(ref last, out short id, out byte type))
                {
                    if (id == 3 && type == ThriftType.Struct && !meta)
                    {
                        meta = true;
                        if (wanted.Length == 0) r.Skip(type);
                        else if (!Chunk(ref r, placement.Utf8Paths[wanted[0]], wanted, chunks)) return false;
                    }
                    else r.Skip(type);
                }
                r.ExitStruct();
                if (!meta) return false;
            }
            return true;
        }

        // A chunk's metadata: its path (the leaf's, or the footer is not read in place) and its statistics.
        static bool Chunk(ref ThriftCompactReader r, byte[][] path, int[] wanted, Statistic[] chunks)
        {
            r.EnterStruct();
            bool pathRead = false;
            Slice min = default, max = default;
            long? nulls = null;
            short last = 0;
            while (r.NextField(ref last, out short id, out byte type))
            {
                switch (id)
                {
                    case 3 when type == ThriftType.List && !pathRead:
                        if (!SamePath(ref r, path)) return false;
                        pathRead = true;
                        break;
                    case 12 when type == ThriftType.Struct:
                        Statistics(ref r, out min, out max, out nulls);
                        break;
                    default:
                        r.Skip(type);
                        break;
                }
            }
            r.ExitStruct();
            if (!pathRead) return false;
            foreach (int i in wanted) chunks[i] = new Statistic(true, min, max, nulls);
            return true;
        }

        static bool SamePath(ref ThriftCompactReader r, byte[][] path)
        {
            int count = r.ReadListHeader(out byte element);
            if (element != ThriftType.Binary || count != path.Length) return false;
            for (int i = 0; i < count; i++)
            {
                if (!r.ReadBinary().SequenceEqual(path[i])) return false;
            }
            return true;
        }

        // The min_value / max_value bounds (ranges of the footer) and the null count; the legacy min / max ignored.
        static void Statistics(ref ThriftCompactReader r, out Slice min, out Slice max, out long? nulls)
        {
            (min, max, nulls) = (default, default, null);
            r.EnterStruct();
            short last = 0;
            while (r.NextField(ref last, out short id, out byte type))
            {
                switch (id)
                {
                    case 3 when type == ThriftType.I64: nulls = r.ReadI64(); break;
                    case 5 when type == ThriftType.Binary: max = Here(ref r); break;
                    case 6 when type == ThriftType.Binary: min = Here(ref r); break;
                    default: r.Skip(type); break;
                }
            }
            r.ExitStruct();

            static Slice Here(ref ThriftCompactReader r)
            {
                int length = r.ReadBinary().Length;
                return new Slice(null, r.Position - length, length, true);
            }
        }
    }

    /// <summary>Folds one row group's chunk of a leaf into its statistics.</summary>
    private static void Fold(ref LeafStatistics result, in Statistic chunk, long rows, DeltaType type, ParquetSchemaElement leaf, ReadOnlySpan<byte> footer)
    {
        if (chunk.NullCount is long nulls) result.NullCount += nulls;
        else result.NullsUnknown = true;
        if (!HasBounds(type)) return;
        bool allNull = chunk.NullCount is long n && n >= rows;
        if (allNull || rows == 0) return;
        if (!chunk.Min.Present || !Decode(chunk.Min, footer, type, leaf, out Bound lo)) result.MinUnknown = true;
        else if (result.Min.Kind == BoundKind.None || Compare(lo, result.Min, footer) < 0) result.Min = lo;
        if (!chunk.Max.Present || !Decode(chunk.Max, footer, type, leaf, out Bound hi)) result.MaxUnknown = true;
        else if (result.Max.Kind == BoundKind.None || Compare(hi, result.Max, footer) > 0) result.Max = hi;

        // Decodes a PLAIN-encoded bound; false when the physical type does not fit the column type.
        static bool Decode(Slice slice, ReadOnlySpan<byte> footer, DeltaType type, ParquetSchemaElement element, out Bound bound)
        {
            bound = default;
            ReadOnlySpan<byte> bytes = slice.In(footer);
            int? physical = element.PhysicalType;
            switch (type)
            {
                case DecimalType decimalType:
                {
                    if (element.DecimalScale is null) return false;
                    if (physical is not (1 or 2 or 6 or 7) || (physical is 1 && bytes.Length != 4) || (physical is 2 && bytes.Length != 8) || bytes.Length == 0)
                        return false;
                    if (element.DecimalScale != decimalType.Scale) return false;
                    bound = physical switch
                    {
                        1 => new Bound { Kind = BoundKind.Integer, Integer = BinaryPrimitives.ReadInt32LittleEndian(bytes) },
                        2 => new Bound { Kind = BoundKind.Integer, Integer = BinaryPrimitives.ReadInt64LittleEndian(bytes) },
                        _ when bytes.Length <= 16 => new Bound { Kind = BoundKind.Integer, Integer = BigEndian(bytes) },
                        _ => new Bound { Kind = BoundKind.LongDecimal, Bytes = slice },
                    };
                    return true;
                }
                case PrimitiveType p:
                    switch (p.Name)
                    {
                        case PrimitiveType.Byte or PrimitiveType.Short or PrimitiveType.Integer or PrimitiveType.Date:
                            if (physical != 1 || bytes.Length != 4) return false;
                            bound = new Bound { Kind = BoundKind.Integer, Integer = BinaryPrimitives.ReadInt32LittleEndian(bytes) };
                            return true;
                        case PrimitiveType.Long:
                            if (physical == 2 && bytes.Length == 8) bound = new Bound { Kind = BoundKind.Integer, Integer = BinaryPrimitives.ReadInt64LittleEndian(bytes) };
                            else if (physical == 1 && bytes.Length == 4) bound = new Bound { Kind = BoundKind.Integer, Integer = BinaryPrimitives.ReadInt32LittleEndian(bytes) };
                            else return false;
                            return true;
                        case PrimitiveType.Float or PrimitiveType.Double:
                        {
                            double value;
                            if (physical == 4 && bytes.Length == 4) value = BinaryPrimitives.ReadSingleLittleEndian(bytes);
                            else if (physical == 5 && bytes.Length == 8) value = BinaryPrimitives.ReadDoubleLittleEndian(bytes);
                            else return false;
                            if (double.IsNaN(value)) return false;
                            bound = new Bound { Kind = BoundKind.Float, Float = value };
                            return true;
                        }
                        case PrimitiveType.Timestamp or PrimitiveType.TimestampNtz:
                        {
                            if (physical != 2 || bytes.Length != 8 || element.TimeUnit is not ParquetTimeUnit unit) return false;
                            long raw = BinaryPrimitives.ReadInt64LittleEndian(bytes);
                            Int128 micros = unit switch
                            {
                                ParquetTimeUnit.Millis => (Int128)raw * 1000,
                                ParquetTimeUnit.Micros => raw,
                                _ => unchecked(raw - (raw < 0 && raw % 1000 != 0 ? 1000 : 0)) / 1000, // floor
                            };
                            bound = new Bound { Kind = BoundKind.Integer, Integer = micros };
                            return true;
                        }
                        case PrimitiveType.String:
                            // A bound that is not valid UTF-8 (a cut inside a character) is not trusted.
                            if (physical != 6 || !Utf8.IsValid(bytes)) return false;
                            bound = new Bound { Kind = BoundKind.Utf8, Bytes = slice };
                            return true;
                    }
                    break;
            }
            return false;

            // A two's complement big-endian integer of at most 16 bytes.
            static Int128 BigEndian(ReadOnlySpan<byte> bytes)
            {
                Span<byte> wide = stackalloc byte[16];
                wide[..^bytes.Length].Fill((bytes[0] & 0x80) != 0 ? (byte)0xFF : (byte)0);
                bytes.CopyTo(wide[^bytes.Length..]);
                return BinaryPrimitives.ReadInt128BigEndian(wide);
            }
        }

        static int Compare(in Bound a, in Bound b, ReadOnlySpan<byte> footer) => (a.Kind, b.Kind) switch
        {
            (BoundKind.Integer, BoundKind.Integer) => a.Integer.CompareTo(b.Integer),
            (BoundKind.Integer or BoundKind.LongDecimal, BoundKind.Integer or BoundKind.LongDecimal) => a.Big(footer).CompareTo(b.Big(footer)),
            (BoundKind.Float, BoundKind.Float) => a.Float.CompareTo(b.Float),
            _ => a.Bytes.In(footer).SequenceCompareTo(b.Bytes.In(footer)), // unsigned bytes: the Parquet order of UTF-8
        };
    }

    /// <summary>
    /// Settles what is written for each leaf once every row group is folded, leaf by leaf: nothing where a row group
    /// has no chunk for it, every row null where the file has no such leaf.
    /// </summary>
    private static void Finish(LeafPlacement placement, LeafStatistics[] results, long rowCount, ReadOnlySpan<byte> footer)
    {
        for (int i = 0; i < placement.Count; i++)
        {
            ref LeafStatistics result = ref results[i];
            if (placement.Element[i] < 0)
            {
                // Not in the file: every row is null (an old file read with a newer schema has the same shape).
                result = default;
                result.NullCount = rowCount;
                result.HasNullCount = true;
                continue;
            }
            if (result.Missing)
            {
                result = default;
                continue;
            }
            result.HasNullCount = !result.NullsUnknown;
            DeltaType type = placement.Types[i];
            if (!HasBounds(type)) continue;
            result.HasMin = !result.MinUnknown && result.Min.Kind != BoundKind.None && Written(result.Min, type, isMax: false, footer);
            result.HasMax = !result.MaxUnknown && result.Max.Kind != BoundKind.None && Written(result.Max, type, isMax: true, footer);
        }

        // Whether a bound is written: not a non-finite float, not a string maximum longer than the prefix kept. A date
        // or time the log cannot hold fails here, leaf by leaf, as when the bounds were formatted while gathered.
        static bool Written(in Bound bound, DeltaType type, bool isMax, ReadOnlySpan<byte> footer) => type switch
        {
            PrimitiveType { Name: PrimitiveType.Float or PrimitiveType.Double } => double.IsFinite(bound.Float),
            PrimitiveType { Name: PrimitiveType.String } => !isMax || Utf16Cut(bound.Bytes.In(footer)) == bound.Bytes.Length,
            PrimitiveType { Name: PrimitiveType.Date } => DayOf(bound) >= DateOnly.MinValue,
            PrimitiveType { Name: PrimitiveType.Timestamp or PrimitiveType.TimestampNtz } => InstantOf(bound) >= DateTime.MinValue,
            _ => true,
        };
    }

    private static bool HasBounds(DeltaType type) =>
        type is DecimalType || type is PrimitiveType { Name: not (PrimitiveType.Boolean or PrimitiveType.Binary) };

    /// <summary>
    /// The column chunks of a footer, found by path: in its Thrift objects (no views made) first at the leaf's ordinal,
    /// where a valid file has it, else (a footer built from its views alone, or one whose chunks name other paths)
    /// by search.
    /// </summary>
    private readonly struct Chunks(ParquetMetadata metadata)
    {
        private readonly FileMetaData? _thrift = metadata.Thrift;

        public int GroupCount => _thrift?.RowGroups.Length ?? metadata.RowGroups.Count;

        public long Rows(int group) => _thrift is { } thrift ? thrift.RowGroups[group].NumRows : metadata.RowGroups[group].RowCount;

        /// <summary>The chunk of the leaf in the group; false when the group has none.</summary>
        public bool TryGet(int group, int ordinal, ReadOnlySpan<string> path, out Statistic statistic)
        {
            if (_thrift is { } thrift)
            {
                ColumnChunk[] columns = thrift.RowGroups[group].Columns;
                if ((uint)ordinal < (uint)columns.Length && columns[ordinal].MetaData is { } meta && Same(meta.PathInSchema, path))
                    return Found(meta, out statistic);
                foreach (ColumnChunk chunk in columns)
                {
                    if (chunk.MetaData is { } other && Same(other.PathInSchema, path)) return Found(other, out statistic);
                }
            }
            else
            {
                foreach (ParquetColumnChunk chunk in metadata.RowGroups[group].Columns)
                {
                    if (!Same(chunk.Path, path)) continue;
                    ParquetColumnStatistics? s = chunk.Statistics;
                    statistic = new Statistic(true, Slice.Of(s?.Min), Slice.Of(s?.Max), s?.NullCount);
                    return true;
                }
            }
            statistic = default;
            return false;

            static bool Found(ColumnMetaData meta, out Statistic statistic)
            {
                Statistics? s = meta.Statistics;
                statistic = new Statistic(true, Slice.Of(s?.MinValue), Slice.Of(s?.MaxValue), s?.NullCount);
                return true;
            }

            static bool Same(IReadOnlyList<string> a, ReadOnlySpan<string> b)
            {
                if (a.Count != b.Length) return false;
                for (int i = 0; i < b.Length; i++)
                {
                    if (a[i] != b[i]) return false;
                }
                return true;
            }
        }
    }

    /// <summary>The stats JSON of the gathered statistics: only the text is allocated.</summary>
    private static string Render(long rowCount, StatisticsLeaves leaves, LeafStatistics[] results, ReadOnlySpan<byte> footer)
    {
        using var buffer = new NativeBufferWriter(1024);
        using (var json = new Utf8JsonWriter(buffer, ActionWriter.JsonOptions))
        {
            json.WriteStartObject();
            json.WriteNumber("numRecords"u8, rowCount);
            if (leaves.Count > 0)
            {
                Section(json, "minValues"u8, leaves, results, Part.Min, footer);
                Section(json, "maxValues"u8, leaves, results, Part.Max, footer);
                Section(json, "nullCount"u8, leaves, results, Part.Nulls, footer);
            }
            json.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>
    /// One section of the stats (minimum, maximum or null count of the leaves that have one), nested under the structs
    /// of the leaves: a struct's object only holds what is written for its fields, and is left out when that is
    /// nothing.
    /// </summary>
    private static void Section(Utf8JsonWriter json, ReadOnlySpan<byte> name, StatisticsLeaves leaves, LeafStatistics[] results, Part part, ReadOnlySpan<byte> footer)
    {
        json.WriteStartObject(name);
        if (leaves.Contiguous)
        {
            // The leaves of a struct follow one another: open its object at its first written leaf, close it after
            // its last.
            int open = 0, previous = -1;
            for (int i = 0; i < leaves.Count; i++)
            {
                if (!Has(results[i], part)) continue;
                int[] prefixes = leaves.Prefixes[i];
                int depth = prefixes.Length - 1, common = 0;
                if (previous >= 0)
                {
                    int[] before = leaves.Prefixes[previous];
                    while (common < open && common < depth && before[common] == prefixes[common]) common++;
                }
                for (; open > common; open--) json.WriteEndObject();
                for (; open < depth; open++) json.WriteStartObject(leaves.Names[i][open]);
                json.WritePropertyName(leaves.Names[i][depth]);
                Value(json, leaves.Types[i], results[i], part, footer);
                previous = i;
            }
            for (; open > 0; open--) json.WriteEndObject();
        }
        else
        {
            List<int> written = [];
            for (int i = 0; i < leaves.Count; i++)
            {
                if (Has(results[i], part)) written.Add(i);
            }
            Nest(json, leaves, results, part, written, 0, footer);
        }
        json.WriteEndObject();

        // Leaves in any order: the values under one struct grouped under its first leaf's place.
        static void Nest(Utf8JsonWriter json, StatisticsLeaves leaves, LeafStatistics[] results, Part part, List<int> items, int depth, ReadOnlySpan<byte> footer)
        {
            foreach (IGrouping<int, int> group in items.GroupBy(i => leaves.Prefixes[i][depth]))
            {
                int first = group.First();
                json.WritePropertyName(leaves.Names[first][depth]);
                List<int> members = [.. group];
                if (members.Count == 1 && leaves.Prefixes[first].Length == depth + 1)
                {
                    Value(json, leaves.Types[first], results[first], part, footer);
                    continue;
                }
                json.WriteStartObject();
                Nest(json, leaves, results, part, members, depth + 1, footer);
                json.WriteEndObject();
            }
        }

        static bool Has(in LeafStatistics result, Part part) => part switch
        {
            Part.Min => result.HasMin,
            Part.Max => result.HasMax,
            _ => result.HasNullCount,
        };

        static void Value(Utf8JsonWriter json, DeltaType type, in LeafStatistics result, Part part, ReadOnlySpan<byte> footer)
        {
            switch (part)
            {
                case Part.Min: Write(json, result.Min, type, footer); break;
                case Part.Max: Write(json, result.Max, type, footer); break;
                default: json.WriteNumberValue(result.NullCount); break;
            }
        }

        // Writes a bound as the log holds it.
        static void Write(Utf8JsonWriter json, in Bound bound, DeltaType type, ReadOnlySpan<byte> footer)
        {
            Span<byte> text = stackalloc byte[64];
            int length;
            switch (type)
            {
                case DecimalType d:
                    if (bound.Kind == BoundKind.LongDecimal)
                    {
                        json.WriteRawValue(WideDecimalText(bound.Big(footer), d.Scale));
                        return;
                    }
                    length = DecimalText(bound.Integer, d.Scale, text);
                    break;
                case PrimitiveType { Name: PrimitiveType.Float }:
                    ((float)bound.Float).TryFormat(text, out length, "R", CultureInfo.InvariantCulture);
                    break;
                case PrimitiveType { Name: PrimitiveType.Double }:
                    bound.Float.TryFormat(text, out length, "R", CultureInfo.InvariantCulture);
                    break;
                case PrimitiveType { Name: PrimitiveType.Date }:
                    text[0] = (byte)'"';
                    DayOf(bound).TryFormat(text[1..], out length, "yyyy-MM-dd", CultureInfo.InvariantCulture);
                    text[++length] = (byte)'"';
                    length++;
                    break;
                case PrimitiveType { Name: PrimitiveType.Timestamp or PrimitiveType.TimestampNtz } p:
                {
                    text[0] = (byte)'"';
                    InstantOf(bound).TryFormat(text[1..], out length, p.Name == PrimitiveType.Timestamp ? "yyyy-MM-dd'T'HH:mm:ss.fff'Z'" : "yyyy-MM-dd'T'HH:mm:ss.fff", CultureInfo.InvariantCulture);
                    text[++length] = (byte)'"';
                    length++;
                    break;
                }
                case PrimitiveType { Name: PrimitiveType.String }:
                    ReadOnlySpan<byte> utf8 = bound.Bytes.In(footer);
                    json.WriteStringValue(utf8[..Utf16Cut(utf8)]);
                    return;
                default:
                    json.WriteNumberValue(checked((long)bound.Integer));
                    return;
            }
            json.WriteRawValue(text[..length]); // validated: a float column with a double bound beyond its range gives Infinity, refused as before

            // An unscaled decimal of at most 128 bits as text.
            static int DecimalText(Int128 unscaled, int scale, Span<byte> text)
            {
                Span<byte> digits = stackalloc byte[40];
                UInt128 magnitude = unscaled < 0 ? (UInt128)(-(unscaled + 1)) + 1 : (UInt128)unscaled;
                magnitude.TryFormat(digits, out int count, default, CultureInfo.InvariantCulture);
                int padded = Math.Max(count, scale + 1), at = 0;
                if (unscaled < 0) text[at++] = (byte)'-';
                Span<byte> whole = text.Slice(at, padded);
                whole[..(padded - count)].Fill((byte)'0');
                digits[..count].CopyTo(whole[(padded - count)..]);
                at += padded;
                if (scale == 0) return at;
                whole[^scale..].CopyTo(text[(at - scale + 1)..]);
                text[at - scale] = (byte)'.';
                return at + 1;
            }

            // A wider decimal (beyond the 38 digits of the table's type: kept as written).
            static string WideDecimalText(BigInteger unscaled, int scale)
            {
                string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture).PadLeft(scale + 1, '0');
                string text = scale == 0 ? digits : digits[..^scale] + "." + digits[^scale..];
                return unscaled.Sign < 0 ? "-" + text : text;
            }
        }
    }

    /// <summary>The leaves under the columns (struct fields at any depth, arrays and maps as leaves), in schema order, where <paramref name="keep"/> holds.</summary>
    private static List<StructField[]> Leaves(IReadOnlyList<StructField> fields, Func<string, bool> keep)
    {
        var result = new List<StructField[]>();
        void Walk(StructField field, List<StructField> path)
        {
            path.Add(field);
            if (field.Type is StructType s)
            {
                foreach (StructField child in s.Fields) Walk(child, path);
            }
            else if (keep(string.Join('.', path.Select(f => f.Name))))
            {
                result.Add([.. path]);
            }
            path.RemoveAt(path.Count - 1);
        }
        foreach (StructField field in fields) Walk(field, []);
        return result;
    }

    /// <summary>The table property naming the columns that get statistics; it takes precedence over the count.</summary>
    public const string StatsColumnsKey = "delta.dataSkippingStatsColumns";

    /// <summary>The table property counting the leading leaf columns that get statistics (32 by default, -1 for all).</summary>
    public const string NumIndexedColsKey = "delta.dataSkippingNumIndexedCols";

    /// <summary>
    /// The leaves whose statistics are written, as Spark selects them: the columns <see cref="StatsColumnsKey"/>
    /// names (dotted for nested fields; a struct names all its fields), else the first <see cref="NumIndexedColsKey"/>
    /// leaves of the data schema in order (32 by default, -1 for all), where every struct field is a leaf and an
    /// array or map is one. Arrays and maps are counted but left out (their statistics cannot be read from a footer).
    /// </summary>
    /// <param name="dataFields">The table columns that are not partition columns, in schema order.</param>
    /// <param name="configuration">The table properties.</param>
    /// <returns>Paths of fields from a column down to the leaf, in schema order.</returns>
    public static IReadOnlyList<StructField[]> IndexedLeaves(IReadOnlyList<StructField> dataFields, IReadOnlyDictionary<string, string>? configuration) =>
        IndexedLeaves(dataFields, configuration, includeContainers: false);

    /// <summary>
    /// <see cref="IndexedLeaves(IReadOnlyList{StructField}, IReadOnlyDictionary{string, string}?)"/>, with arrays and
    /// maps kept when <paramref name="includeContainers"/>: Spark counts their nulls (a checkpoint's
    /// <c>nullCount</c> lists them) though it keeps no bounds for them.
    /// </summary>
    internal static IReadOnlyList<StructField[]> IndexedLeaves(
        IReadOnlyList<StructField> dataFields, IReadOnlyDictionary<string, string>? configuration, bool includeContainers)
    {
        ArgumentNullException.ThrowIfNull(dataFields);
        List<StructField[]> leaves;
        if (configuration is not null && configuration.TryGetValue(StatsColumnsKey, out string? named))
        {
            // Set, even empty (Spark: an empty list collects no column statistics, only the row count).
            var names = new List<string>();
            foreach (IReadOnlyList<string> parts in StatsColumns(named)) names.Add(string.Join('.', parts));
            leaves = Leaves(dataFields, path => names.Any(n =>
                path.Equals(n, StringComparison.OrdinalIgnoreCase) || path.StartsWith(n + ".", StringComparison.OrdinalIgnoreCase)));
        }
        else
        {
            int count = 32;
            if (configuration is not null && configuration.TryGetValue(NumIndexedColsKey, out string? text)
                && int.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int parsed))
                count = parsed;
            leaves = Leaves(dataFields, _ => true);
            if (count >= 0) leaves = [.. leaves.Take(count)];
        }
        return includeContainers ? leaves : [.. leaves.Where(p => p[^1].Type is not (ArrayType or MapType))];
    }

    /// <summary>
    /// The entries of <see cref="StatsColumnsKey"/>: comma-separated dotted names, each part possibly in backticks
    /// (a backticked part may hold dots); blank entries skipped.
    /// </summary>
    public static List<IReadOnlyList<string>> StatsColumns(string property)
    {
        ArgumentNullException.ThrowIfNull(property);
        var entries = new List<IReadOnlyList<string>>();
        foreach (string entry in property.Split(','))
        {
            if (entry.Trim().Length > 0) entries.Add(ColumnNames.Split(entry.Trim()));
        }
        return entries;
    }

    /// <summary>The null count of every top-level column the footer has statistics for, summed over row groups.</summary>
    public static IReadOnlyDictionary<string, long> NullCounts(ParquetMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        LeafPlacement placement = Place(metadata.Schema, metadata.Schema, leaves: null);
        LeafStatistics[] results = Rent(placement.Count);
        GatherParsed(metadata, placement, results);
        return Counts(placement, results);
    }

    /// <summary>
    /// <see cref="NullCounts(ParquetMetadata)"/> of a footer as its writer wrote it, read in place as
    /// <see cref="FromFooter"/> reads it (the schema placed once per write of <paramref name="leaves"/>).
    /// </summary>
    internal static Dictionary<string, long> NullCounts(ParquetFooter footer, StatisticsLeaves leaves)
    {
        ArgumentNullException.ThrowIfNull(footer);
        ArgumentNullException.ThrowIfNull(leaves);
        if (footer.HasBytes && GatherBytes(footer.Thrift, leaves, topLevel: true, out LeafPlacement? placement, out LeafStatistics[]? results, out _))
            return Counts(placement, results);
        ParquetMetadata metadata = footer.Metadata;
        LeafPlacement parsed = Placed(leaves, metadata.Schema, topLevel: true);
        LeafStatistics[] counted = Rent(parsed.Count);
        GatherParsed(metadata, parsed, counted);
        return Counts(parsed, counted);
    }

    private static Dictionary<string, long> Counts(LeafPlacement placement, LeafStatistics[] results)
    {
        var counts = new Dictionary<string, long>(StringComparer.Ordinal);
        for (int i = 0; i < placement.Count; i++)
        {
            if (results[i].HasNullCount) counts[placement.Paths[i][0]] = results[i].NullCount;
        }
        results.AsSpan(0, placement.Count).Clear();
        return counts;
    }

    // ------------------------------------------------------------------
    // Values
    // ------------------------------------------------------------------

    private enum BoundKind : byte { None, Integer, Float, Utf8, LongDecimal }

    /// <summary>
    /// A decoded bound: an integer (also dates, timestamps in microseconds, decimals unscaled), a float, UTF-8 bytes,
    /// or the big-endian bytes of a decimal wider than 128 bits; the bytes are the footer's own (an array of a parsed
    /// footer, or a range of its Thrift bytes).
    /// </summary>
    private readonly struct Bound
    {
        public BoundKind Kind { get; init; }
        public Int128 Integer { get; init; }
        public double Float { get; init; }
        public Slice Bytes { get; init; }

        public BigInteger Big(ReadOnlySpan<byte> footer) =>
            Kind == BoundKind.LongDecimal ? new BigInteger(Bytes.In(footer), isUnsigned: false, isBigEndian: true) : (BigInteger)Integer;
    }

    /// <summary>The day of a date bound (days since the epoch).</summary>
    /// <exception cref="ArgumentOutOfRangeException">The day is beyond the dates .NET holds.</exception>
    private static DateOnly DayOf(in Bound bound) =>
        DateOnly.FromDayNumber(DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber + checked((int)bound.Integer));

    /// <summary>The instant of a timestamp bound (microseconds since the epoch), cut down to milliseconds (readers allow for the cut).</summary>
    /// <exception cref="OverflowException">The microseconds are beyond a long.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The instant is beyond the times .NET holds.</exception>
    private static DateTime InstantOf(in Bound bound)
    {
        long micros = checked((long)bound.Integer);
        long millis = micros / 1000 - (micros % 1000 < 0 ? 1 : 0); // floor
        return DateTime.UnixEpoch.AddMilliseconds(millis);
    }

    /// <summary>
    /// The UTF-8 bytes of the first <see cref="StringPrefixLength"/> UTF-16 code units of valid UTF-8 text (all of
    /// it when shorter), without splitting a surrogate pair: a cut minimum is still a lower bound.
    /// </summary>
    private static int Utf16Cut(ReadOnlySpan<byte> utf8)
    {
        int units = 0, at = 0;
        while (at < utf8.Length)
        {
            byte lead = utf8[at];
            (int length, int width) = lead switch
            {
                < 0x80 => (1, 1),
                < 0xE0 => (2, 1),
                < 0xF0 => (3, 1),
                _ => (4, 2),
            };
            if (units + width > StringPrefixLength) break;
            units += width;
            at += length;
        }
        return at;
    }

}

/// <summary>
/// The leaves a write gives statistics for, prepared once for all its files: their physical paths, types, JSON
/// names, and which struct each level belongs to (whether the leaves of each struct follow one another).
/// </summary>
internal sealed class StatisticsLeaves
{
    /// <summary>Prepares <paramref name="leaves"/> (paths of fields from a table column down; struct, array and map leaves are left out).</summary>
    public StatisticsLeaves(IReadOnlyList<StructField[]> leaves)
    {
        StructField[][] kept = [.. leaves.Where(p => p[^1].Type is not (StructType or ArrayType or MapType))];
        Count = kept.Length;
        Paths = new string[Count][];
        Types = new DeltaType[Count];
        Names = new JsonEncodedText[Count][];
        Prefixes = new int[Count][];
        var ids = new Dictionary<(int Parent, string Name), int>();
        HashSet<int> started = [];
        Contiguous = true;
        for (int i = 0; i < Count; i++)
        {
            StructField[] leaf = kept[i];
            Paths[i] = [.. leaf.Select(f => f.PhysicalName)];
            Types[i] = leaf[^1].Type;
            Names[i] = [.. Paths[i].Select(n => JsonEncodedText.Encode(n, ActionWriter.JsonOptions.Encoder))];
            int[] prefixes = new int[leaf.Length];
            int parent = -1;
            for (int d = 0; d < leaf.Length; d++)
            {
                if (!ids.TryGetValue((parent, Paths[i][d]), out int id)) ids[(parent, Paths[i][d])] = id = ids.Count;
                prefixes[d] = parent = id;
            }
            // Each struct's leaves in one run: a struct (or leaf) that starts here was not seen before.
            int[]? before = i > 0 ? Prefixes[i - 1] : null;
            for (int d = 0; d < prefixes.Length; d++)
            {
                if (before is not null && d < before.Length && before[d] == prefixes[d]) continue;
                if (!started.Add(prefixes[d])) Contiguous = false;
            }
            Prefixes[i] = prefixes;
        }
    }

    /// <summary>The number of leaves.</summary>
    public int Count { get; }

    /// <summary>Each leaf's path of physical names.</summary>
    public string[][] Paths { get; }

    /// <summary>Each leaf's type.</summary>
    public DeltaType[] Types { get; }

    /// <summary>Each leaf's path of names as JSON property names.</summary>
    public JsonEncodedText[][] Names { get; }

    /// <summary>Each leaf's path as ids of its structs (and of itself, last): equal ids, the same struct.</summary>
    public int[][] Prefixes { get; }

    /// <summary>Whether the leaves of every struct follow one another (as in schema order).</summary>
    public bool Contiguous { get; }

    /// <summary>The footers' schemas of the write: read once for all its files of one schema.</summary>
    public FooterInterner Interner => _interner ??= new();

    private FooterInterner? _interner;

    /// <summary>
    /// The placement of the leaves, and of the top-level leaves, in the last schema read from a footer's bytes (a
    /// write's files share it): the statistics' own type, held as an object here.
    /// </summary>
    internal object? Placed, PlacedTopLevel;
}
