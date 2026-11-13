using System.Buffers.Binary;
using System.Text;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Kaminari.Seseragi.Files.Parquet;

// A small, dependency-free Parquet writer: just enough to write Delta checkpoints, whose schema is fixed. It is a
// stopgap until the engine can write Parquet maps (or Seseragi has its own Parquet IO), and is meant to be replaced.
//
// What it writes: optional and required fields, structs, MAP (repeated key_value groups) and three-level LIST
// columns; BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY leaves, annotated (as Spark
// annotates them) as strings, 8- and 16-bit integers, dates, decimals and timestamps. One row group, one data page
// (v1) per column, PLAIN values, RLE levels, no compression.

/// <summary>The repetition of a Parquet schema node.</summary>
internal enum Repetition { Required = 0, Optional = 1, Repeated = 2 }

/// <summary>The physical types this writer supports.</summary>
internal enum PhysicalType { Boolean = 0, Int32 = 1, Int64 = 2, Float = 4, Double = 5, ByteArray = 6, FixedLenByteArray = 7 }

/// <summary>What a leaf's values mean beyond their physical type.</summary>
internal enum Annotation
{
    /// <summary>Plain values.</summary>
    None,
    /// <summary>INT32 holding a signed 8-bit integer.</summary>
    Int8,
    /// <summary>INT32 holding a signed 16-bit integer.</summary>
    Int16,
    /// <summary>INT32 days since 1970-01-01.</summary>
    Date,
    /// <summary>An unscaled decimal: INT32, INT64 (values as numbers) or FIXED_LEN_BYTE_ARRAY (big-endian two's complement bytes).</summary>
    Decimal,
    /// <summary>INT64 microseconds since the epoch, an instant (UTC).</summary>
    TimestampMicros,
    /// <summary>INT64 microseconds since the epoch, a local date-time (no time zone).</summary>
    LocalTimestampMicros,
}

/// <summary>A node of a Parquet schema as the writer sees it.</summary>
internal abstract record ParquetNode(string Name, Repetition Repetition);

/// <summary>
/// A column of values. <see cref="Utf8"/> marks strings; <see cref="Annotation"/> other meanings, with the decimal's
/// <see cref="Precision"/> and <see cref="Scale"/> and a fixed-length array's <see cref="TypeLength"/>.
/// </summary>
internal sealed record ParquetLeaf(
    string Name, Repetition Repetition, PhysicalType Type, bool Utf8 = false, Annotation Annotation = Annotation.None,
    int Precision = 0, int Scale = 0, int TypeLength = 0) : ParquetNode(Name, Repetition);

/// <summary>A struct: values are <c>IReadOnlyDictionary&lt;string, object?&gt;</c> keyed by child name.</summary>
internal sealed record ParquetStruct(string Name, Repetition Repetition, IReadOnlyList<ParquetNode> Children) : ParquetNode(Name, Repetition);

/// <summary>A list of elements: values are <c>IEnumerable</c> of element values.</summary>
internal sealed record ParquetList(string Name, Repetition Repetition, ParquetNode Element) : ParquetNode(Name, Repetition);

/// <summary>A map with string keys and nullable string values: values are <c>IEnumerable&lt;KeyValuePair&lt;string, string?&gt;&gt;</c>.</summary>
internal sealed record ParquetStringMap(string Name, Repetition Repetition) : ParquetNode(Name, Repetition);

/// <summary>Writes rows of nested values to a Parquet file.</summary>
internal static class MinimalParquetWriter
{
    private const string CreatedBy = "Kaminari.Seseragi.Files minimal writer";

    /// <summary>Writes the rows; each row is keyed by the names of the top-level fields.</summary>
    public static byte[] Write(IReadOnlyList<ParquetNode> schema, IReadOnlyList<IReadOnlyDictionary<string, object?>> rows)
    {
        List<Column> columns = [];
        foreach (ParquetNode node in schema) Collect(Normalize(node), [], 0, 0, columns);

        ParquetNode[] normalized = [.. schema.Select(Normalize)];
        foreach (var row in rows)
        {
            int at = 0;
            foreach (ParquetNode node in normalized)
                at += Shred(node, row.TryGetValue(node.Name, out object? v) ? v : null, 0, 0, columns, at);
        }

        using var file = new MemoryStream();
        file.Write("PAR1"u8);
        List<(Column Column, long Offset, long Size)> chunks = [];
        foreach (Column column in columns)
        {
            long offset = file.Position;
            WritePage(file, column);
            chunks.Add((column, offset, file.Position - offset));
        }

        byte[] footer = FileMetadata(schema, rows.Count, chunks);
        file.Write(footer);
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, footer.Length);
        file.Write(length);
        file.Write("PAR1"u8);
        return file.ToArray();
    }

    // ------------------------------------------------------------------
    // Schema: maps and lists expand to their standard group structure
    // ------------------------------------------------------------------

    private sealed record ListGroup(string Name, Repetition Repetition, ParquetNode Repeated, bool IsMap) : ParquetNode(Name, Repetition);

    /// <summary>
    /// The node as stored: a LIST is <c>optional group (LIST) { repeated group list { element } }</c>, a MAP is
    /// <c>optional group (MAP) { repeated group key_value { required key; optional value } }</c>.
    /// </summary>
    private static ParquetNode Normalize(ParquetNode node) => node switch
    {
        ParquetList l => new ListGroup(l.Name, l.Repetition,
            new ParquetStruct("list", Repetition.Repeated, [Normalize(l.Element with { Name = "element" } as ParquetNode)]), IsMap: false),
        ParquetStringMap m => new ListGroup(m.Name, m.Repetition,
            new ParquetStruct("key_value", Repetition.Repeated,
            [
                new ParquetLeaf("key", Repetition.Required, PhysicalType.ByteArray, Utf8: true),
                new ParquetLeaf("value", Repetition.Optional, PhysicalType.ByteArray, Utf8: true),
            ]), IsMap: true),
        ParquetStruct s => s with { Children = [.. s.Children.Select(Normalize)] },
        _ => node,
    };

    // ------------------------------------------------------------------
    // Shredding: values to repetition and definition levels (Dremel)
    // ------------------------------------------------------------------

    private sealed class Column(ParquetLeaf leaf, string[] path, int maxRep, int maxDef)
    {
        public ParquetLeaf Leaf { get; } = leaf;
        public string[] Path { get; } = path;
        public int MaxRep { get; } = maxRep;
        public int MaxDef { get; } = maxDef;
        public List<int> Rep { get; } = [];
        public List<int> Def { get; } = [];
        public List<object> Values { get; } = [];
    }

    private static int Collect(ParquetNode node, string[] parent, int rep, int def, List<Column> columns)
    {
        string[] path = [.. parent, node.Name];
        if (node.Repetition == Repetition.Optional) def++;
        if (node.Repetition == Repetition.Repeated) { def++; rep++; }
        int first = columns.Count;
        switch (node)
        {
            case ParquetLeaf leaf:
                columns.Add(new Column(leaf, path, rep, def));
                break;
            case ParquetStruct s:
                foreach (ParquetNode child in s.Children) Collect(child, path, rep, def, columns);
                break;
            case ListGroup g:
                Collect(g.Repeated, path, rep, def, columns);
                break;
        }
        return columns.Count - first;
    }

    /// <summary>Appends a value (and everything under it) to the columns from <paramref name="index"/> on; returns how many columns it covers.</summary>
    private static int Shred(ParquetNode node, object? value, int rep, int def, List<Column> columns, int index)
    {
        int width = LeafCount(node);
        if (value is null)
        {
            if (node.Repetition == Repetition.Required)
                throw new InvalidOperationException($"The required field '{node.Name}' is null.");
            for (int i = 0; i < width; i++) Null(columns[index + i], rep, def);
            return width;
        }
        if (node.Repetition == Repetition.Optional) def++;

        switch (node)
        {
            case ParquetLeaf:
                Column column = columns[index];
                column.Rep.Add(rep);
                column.Def.Add(def);
                column.Values.Add(value);
                break;

            case ParquetStruct s:
            {
                var fields = (IReadOnlyDictionary<string, object?>)value;
                int at = index;
                foreach (ParquetNode child in s.Children)
                    at += Shred(child, fields.TryGetValue(child.Name, out object? v) ? v : null, rep, def, columns, at);
                break;
            }

            case ListGroup g:
            {
                // The repeated group sits one repetition level and one definition level deeper.
                int repeatedRep = columns[index].MaxRep - RepeatedDepthBelow(g.Repeated);
                bool any = false;
                foreach (object? item in Items(value, g.IsMap))
                {
                    int r = any ? repeatedRep : rep;
                    any = true;
                    // The repeated group itself is never null: an element is present.
                    var inner = (ParquetStruct)g.Repeated;
                    int at = index;
                    foreach (ParquetNode child in inner.Children)
                    {
                        object? childValue = g.IsMap
                            ? (child.Name == "key" ? ((KeyValuePair<string, string?>)item!).Key : ((KeyValuePair<string, string?>)item!).Value)
                            : item;
                        at += Shred(child, childValue, r, def + 1, columns, at);
                    }
                }
                if (!any)
                {
                    // An empty list: defined up to the list itself, no element.
                    for (int i = 0; i < width; i++) Null(columns[index + i], rep, def);
                }
                break;
            }
        }
        return width;
    }

    private static IEnumerable<object?> Items(object value, bool isMap) => isMap
        ? ((IEnumerable<KeyValuePair<string, string?>>)value).Select(kv => (object?)kv)
        : ((System.Collections.IEnumerable)value).Cast<object?>();

    /// <summary>How many repeated levels lie strictly below the repeated group (for nested lists).</summary>
    private static int RepeatedDepthBelow(ParquetNode repeated) => repeated switch
    {
        ParquetStruct s => s.Children.Count == 0 ? 0 : s.Children.Max(c => c switch
        {
            ListGroup g => 1 + RepeatedDepthBelow(g.Repeated),
            ParquetStruct inner => RepeatedDepthBelow(inner),
            _ => 0,
        }),
        _ => 0,
    };

    private static void Null(Column column, int rep, int def)
    {
        column.Rep.Add(rep);
        column.Def.Add(def);
    }

    private static int LeafCount(ParquetNode node) => node switch
    {
        ParquetLeaf => 1,
        ParquetStruct s => s.Children.Sum(LeafCount),
        ListGroup g => LeafCount(g.Repeated),
        _ => 0,
    };

    // ------------------------------------------------------------------
    // Pages
    // ------------------------------------------------------------------

    private static void WritePage(Stream file, Column column)
    {
        using var body = new MemoryStream();
        if (column.MaxRep > 0) WriteLevels(body, column.Rep, column.MaxRep);
        if (column.MaxDef > 0) WriteLevels(body, column.Def, column.MaxDef);
        WritePlain(body, column);
        byte[] page = body.ToArray();

        var header = new ThriftCompactWriter();
        new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = page.Length,
            CompressedPageSize = page.Length,
            DataPageHeader = new DataPageHeader
            {
                NumValues = column.Def.Count, // levels, nulls included
                Encoding = ParquetEncoding.Plain,
                DefinitionLevelEncoding = ParquetEncoding.Rle,
                RepetitionLevelEncoding = ParquetEncoding.Rle,
            },
        }.Write(header);
        file.Write(header.ToArray());
        file.Write(page);
    }

    /// <summary>Levels in the RLE / bit-packing hybrid, as RLE runs only, after a 4-byte length.</summary>
    private static void WriteLevels(Stream stream, List<int> levels, int maxLevel)
    {
        int bitWidth = 32 - int.LeadingZeroCount(maxLevel);
        int valueBytes = (bitWidth + 7) / 8;
        var runs = new MemoryStream();
        for (int i = 0; i < levels.Count;)
        {
            int j = i;
            while (j < levels.Count && levels[j] == levels[i]) j++;
            WriteVarint(runs, (ulong)(j - i) << 1); // an RLE run: count << 1, low bit 0
            for (int b = 0; b < valueBytes; b++) runs.WriteByte((byte)(levels[i] >> (8 * b)));
            i = j;
        }
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, (int)runs.Length);
        stream.Write(length);
        runs.Position = 0;
        runs.CopyTo(stream);
    }

    private static void WritePlain(Stream stream, Column column)
    {
        Span<byte> buffer = stackalloc byte[8];
        switch (column.Leaf.Type)
        {
            case PhysicalType.Boolean:
            {
                var bits = new byte[(column.Values.Count + 7) / 8];
                for (int i = 0; i < column.Values.Count; i++)
                    if ((bool)column.Values[i]) bits[i / 8] |= (byte)(1 << (i % 8));
                stream.Write(bits);
                break;
            }
            case PhysicalType.Int32:
                foreach (object v in column.Values)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(buffer, Convert.ToInt32(v, System.Globalization.CultureInfo.InvariantCulture));
                    stream.Write(buffer[..4]);
                }
                break;
            case PhysicalType.Int64:
                foreach (object v in column.Values)
                {
                    BinaryPrimitives.WriteInt64LittleEndian(buffer, Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture));
                    stream.Write(buffer);
                }
                break;
            case PhysicalType.Float:
                foreach (object v in column.Values)
                {
                    BinaryPrimitives.WriteSingleLittleEndian(buffer, Convert.ToSingle(v, System.Globalization.CultureInfo.InvariantCulture));
                    stream.Write(buffer[..4]);
                }
                break;
            case PhysicalType.Double:
                foreach (object v in column.Values)
                {
                    BinaryPrimitives.WriteDoubleLittleEndian(buffer, Convert.ToDouble(v, System.Globalization.CultureInfo.InvariantCulture));
                    stream.Write(buffer);
                }
                break;
            case PhysicalType.ByteArray:
                foreach (object v in column.Values)
                {
                    byte[] bytes = v as byte[] ?? Encoding.UTF8.GetBytes((string)v);
                    BinaryPrimitives.WriteInt32LittleEndian(buffer, bytes.Length);
                    stream.Write(buffer[..4]);
                    stream.Write(bytes);
                }
                break;
            case PhysicalType.FixedLenByteArray:
                foreach (object v in column.Values)
                {
                    var bytes = (byte[])v;
                    if (bytes.Length != column.Leaf.TypeLength)
                        throw new InvalidOperationException($"The value of '{column.Leaf.Name}' has {bytes.Length} bytes, not {column.Leaf.TypeLength}.");
                    stream.Write(bytes);
                }
                break;
        }
    }

    // ------------------------------------------------------------------
    // Footer
    // ------------------------------------------------------------------

    private static byte[] FileMetadata(IReadOnlyList<ParquetNode> schema, long rows, List<(Column Column, long Offset, long Size)> chunks)
    {
        // The root, then the tree depth first.
        List<SchemaElement> elements = [new SchemaElement { Name = "schema", NumChildren = schema.Count }];
        foreach (ParquetNode node in schema) Elements(Normalize(node), elements);

        var meta = new FileMetaData
        {
            Version = 1,
            Schema = [.. elements],
            NumRows = rows,
            RowGroups =
            [
                new RowGroup
                {
                    Columns = [.. chunks.Select(c => new ColumnChunk
                    {
                        FileOffset = c.Offset,
                        MetaData = new ColumnMetaData
                        {
                            Type = (ParquetType)c.Column.Leaf.Type,
                            Encodings = [ParquetEncoding.Plain, ParquetEncoding.Rle],
                            PathInSchema = c.Column.Path,
                            Codec = CompressionCodec.Uncompressed,
                            NumValues = c.Column.Def.Count,
                            TotalUncompressedSize = c.Size,
                            TotalCompressedSize = c.Size,
                            DataPageOffset = c.Offset,
                        },
                    })],
                    TotalByteSize = chunks.Sum(c => c.Size),
                    NumRows = rows,
                },
            ],
            CreatedBy = CreatedBy,
        };
        var writer = new ThriftCompactWriter();
        meta.Write(writer);
        return writer.ToArray();
    }

    private static void Elements(ParquetNode node, List<SchemaElement> elements)
    {
        switch (node)
        {
            case ParquetLeaf leaf:
                elements.Add(LeafElement(leaf));
                break;
            case ParquetStruct s:
                elements.Add(new SchemaElement { Repetition = (FieldRepetition)s.Repetition, Name = s.Name, NumChildren = s.Children.Count });
                foreach (ParquetNode child in s.Children) Elements(child, elements);
                break;
            case ListGroup g:
                elements.Add(new SchemaElement
                {
                    Repetition = (FieldRepetition)g.Repetition,
                    Name = g.Name,
                    NumChildren = 1,
                    ConvertedType = g.IsMap ? ConvertedType.Map : ConvertedType.List,
                });
                var repeated = (ParquetStruct)g.Repeated;
                elements.Add(new SchemaElement
                {
                    Repetition = FieldRepetition.Repeated,
                    Name = repeated.Name,
                    NumChildren = repeated.Children.Count,
                    ConvertedType = g.IsMap ? ConvertedType.MapKeyValue : null,
                });
                foreach (ParquetNode child in repeated.Children) Elements(child, elements);
                break;
        }
    }

    /// <summary>A leaf's schema element: converted type and logical type as parquet-mr writes them.</summary>
    private static SchemaElement LeafElement(ParquetLeaf leaf)
    {
        bool isDecimal = leaf.Annotation == Annotation.Decimal;
        return new SchemaElement
        {
            Type = (ParquetType)leaf.Type,
            TypeLength = leaf.Type == PhysicalType.FixedLenByteArray ? leaf.TypeLength : null,
            Repetition = (FieldRepetition)leaf.Repetition,
            Name = leaf.Name,
            // A local timestamp has no converted type.
            ConvertedType = leaf.Utf8 ? ConvertedType.Utf8 : leaf.Annotation switch
            {
                Annotation.Decimal => ConvertedType.Decimal,
                Annotation.Date => ConvertedType.Date,
                Annotation.TimestampMicros => ConvertedType.TimestampMicros,
                Annotation.Int8 => ConvertedType.Int8,
                Annotation.Int16 => ConvertedType.Int16,
                _ => null,
            },
            Scale = isDecimal ? leaf.Scale : null,
            Precision = isDecimal ? leaf.Precision : null,
            LogicalType = leaf.Utf8 ? new LogicalType(LogicalTypeKind.String) : leaf.Annotation switch
            {
                Annotation.Decimal => new LogicalType(LogicalTypeKind.Decimal) { Scale = leaf.Scale, Precision = leaf.Precision },
                Annotation.Date => new LogicalType(LogicalTypeKind.Date),
                Annotation.TimestampMicros or Annotation.LocalTimestampMicros => new LogicalType(LogicalTypeKind.Timestamp)
                {
                    IsAdjustedToUtc = leaf.Annotation == Annotation.TimestampMicros,
                    Unit = ParquetTimeUnit.Micros,
                    UnitMember = 2,
                },
                Annotation.Int8 or Annotation.Int16 => new LogicalType(LogicalTypeKind.Integer)
                {
                    BitWidth = (sbyte)(leaf.Annotation == Annotation.Int8 ? 8 : 16),
                    IsSigned = true,
                },
                _ => null,
            },
        };
    }

    private static void WriteVarint(Stream stream, ulong value)
    {
        while (value >= 0x80)
        {
            stream.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        stream.WriteByte((byte)value);
    }
}
