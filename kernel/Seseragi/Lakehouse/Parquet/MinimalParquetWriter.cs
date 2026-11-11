using System.Buffers.Binary;
using System.Text;

namespace Kaminari.Seseragi.Lakehouse.Parquet;

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
    private const string CreatedBy = "Kaminari.Seseragi.Lakehouse minimal writer";

    /// <summary>Writes the rows; each row is keyed by the names of the top-level fields.</summary>
    public static byte[] Write(IReadOnlyList<ParquetNode> schema, IReadOnlyList<IReadOnlyDictionary<string, object?>> rows)
    {
        var columns = new List<Column>();
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
        var chunks = new List<(Column Column, long Offset, long Size)>();
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

        var header = new CompactWriter();
        header.I32(1, 0);                // type: DATA_PAGE
        header.I32(2, page.Length);      // uncompressed_page_size
        header.I32(3, page.Length);      // compressed_page_size
        header.BeginStruct(5);           // data_page_header
        header.I32(1, column.Def.Count); // num_values (levels, nulls included)
        header.I32(2, 0);                // encoding: PLAIN
        header.I32(3, 3);                // definition_level_encoding: RLE
        header.I32(4, 3);                // repetition_level_encoding: RLE
        header.EndStruct();
        header.Stop();
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
        var elements = new List<Action<CompactWriter>>
        {
            // The root, then the tree depth first.
            w =>
            {
                w.Binary(4, "schema");
                w.I32(5, schema.Count);
            }
        };
        foreach (ParquetNode node in schema) Elements(Normalize(node), elements);

        var meta = new CompactWriter();
        meta.I32(1, 1);                                  // version
        meta.BeginList(2, CompactWriter.Struct, elements.Count);
        foreach (var element in elements) { element(meta); meta.Stop(); }
        meta.I64(3, rows);                               // num_rows
        meta.BeginList(4, CompactWriter.Struct, 1);      // row_groups
        {
            meta.BeginList(1, CompactWriter.Struct, chunks.Count); // columns
            foreach (var (column, offset, size) in chunks)
            {
                meta.I64(2, offset);                     // file_offset
                meta.BeginStruct(3);                     // meta_data
                meta.I32(1, (int)column.Leaf.Type);
                meta.BeginList(2, CompactWriter.I32Type, 2);
                meta.ListI32(0);                         // PLAIN
                meta.ListI32(3);                         // RLE
                meta.BeginList(3, CompactWriter.BinaryType, column.Path.Length);
                foreach (string part in column.Path) meta.ListBinary(part);
                meta.I32(4, 0);                          // codec: UNCOMPRESSED
                meta.I64(5, column.Def.Count);           // num_values
                meta.I64(6, size);                       // total_uncompressed_size
                meta.I64(7, size);                       // total_compressed_size
                meta.I64(9, offset);                     // data_page_offset
                meta.EndStruct();
                meta.Stop();
            }
            meta.I64(2, chunks.Sum(c => c.Size));       // total_byte_size
            meta.I64(3, rows);                           // num_rows
            meta.Stop();
        }
        meta.Binary(6, CreatedBy);
        meta.Stop();
        return meta.ToArray();
    }

    private static void Elements(ParquetNode node, List<Action<CompactWriter>> elements)
    {
        switch (node)
        {
            case ParquetLeaf leaf:
                elements.Add(w => LeafElement(w, leaf));
                break;
            case ParquetStruct s:
                elements.Add(w =>
                {
                    w.I32(3, (int)s.Repetition);
                    w.Binary(4, s.Name);
                    w.I32(5, s.Children.Count);
                });
                foreach (ParquetNode child in s.Children) Elements(child, elements);
                break;
            case ListGroup g:
                elements.Add(w =>
                {
                    w.I32(3, (int)g.Repetition);
                    w.Binary(4, g.Name);
                    w.I32(5, 1);
                    w.I32(6, g.IsMap ? 1 : 3); // converted_type: MAP or LIST
                });
                var repeated = (ParquetStruct)g.Repeated;
                elements.Add(w =>
                {
                    w.I32(3, (int)Repetition.Repeated);
                    w.Binary(4, repeated.Name);
                    w.I32(5, repeated.Children.Count);
                    if (g.IsMap) w.I32(6, 2); // converted_type: MAP_KEY_VALUE
                });
                foreach (ParquetNode child in repeated.Children) Elements(child, elements);
                break;
        }
    }

    /// <summary>A leaf's schema element: converted type and logical type as parquet-mr writes them.</summary>
    private static void LeafElement(CompactWriter w, ParquetLeaf leaf)
    {
        w.I32(1, (int)leaf.Type);
        if (leaf.Type == PhysicalType.FixedLenByteArray) w.I32(2, leaf.TypeLength);
        w.I32(3, (int)leaf.Repetition);
        w.Binary(4, leaf.Name);
        // converted_type: UTF8 0, DECIMAL 5, DATE 6, TIMESTAMP_MICROS 10, INT_8 15, INT_16 16 (a local timestamp has none)
        int? converted = leaf.Utf8 ? 0 : leaf.Annotation switch
        {
            Annotation.Decimal => 5,
            Annotation.Date => 6,
            Annotation.TimestampMicros => 10,
            Annotation.Int8 => 15,
            Annotation.Int16 => 16,
            _ => null,
        };
        if (converted is int c) w.I32(6, c);
        if (leaf.Annotation == Annotation.Decimal)
        {
            w.I32(7, leaf.Scale);
            w.I32(8, leaf.Precision);
        }
        if (!leaf.Utf8 && leaf.Annotation == Annotation.None) return;

        w.BeginStruct(10); // logicalType (a union)
        if (leaf.Utf8)
        {
            w.BeginStruct(1); // STRING
            w.EndStruct();
        }
        else
        {
            switch (leaf.Annotation)
            {
                case Annotation.Decimal:
                    w.BeginStruct(5);
                    w.I32(1, leaf.Scale);
                    w.I32(2, leaf.Precision);
                    w.EndStruct();
                    break;
                case Annotation.Date:
                    w.BeginStruct(6);
                    w.EndStruct();
                    break;
                case Annotation.TimestampMicros or Annotation.LocalTimestampMicros:
                    w.BeginStruct(8);
                    w.Bool(1, leaf.Annotation == Annotation.TimestampMicros); // isAdjustedToUTC
                    w.BeginStruct(2); // unit
                    w.BeginStruct(2); // MICROS
                    w.EndStruct();
                    w.EndStruct();
                    w.EndStruct();
                    break;
                case Annotation.Int8 or Annotation.Int16:
                    w.BeginStruct(10);
                    w.I8(1, (sbyte)(leaf.Annotation == Annotation.Int8 ? 8 : 16)); // bitWidth
                    w.Bool(2, true); // isSigned
                    w.EndStruct();
                    break;
            }
        }
        w.EndStruct();
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

    /// <summary>The Thrift compact protocol, for the structs this writer needs.</summary>
    private sealed class CompactWriter
    {
        public const byte I32Type = 5, I64Type = 6, BinaryType = 8, ListType = 9, Struct = 12;

        private readonly MemoryStream _out = new();
        private readonly Stack<short> _lastField = new();
        private short _last;

        public byte[] ToArray() => _out.ToArray();

        private void FieldHeader(short id, byte type)
        {
            int delta = id - _last;
            if (delta is > 0 and <= 15) _out.WriteByte((byte)((delta << 4) | type));
            else
            {
                _out.WriteByte(type);
                WriteVarint(_out, ZigZag(id));
            }
            _last = id;
        }

        private static ulong ZigZag(long n) => (ulong)((n << 1) ^ (n >> 63));

        public void I32(short id, int value)
        {
            FieldHeader(id, I32Type);
            WriteVarint(_out, ZigZag(value));
        }

        public void Bool(short id, bool value) => FieldHeader(id, value ? (byte)1 : (byte)2);

        public void I8(short id, sbyte value)
        {
            FieldHeader(id, 3);
            _out.WriteByte((byte)value);
        }

        public void I64(short id, long value)
        {
            FieldHeader(id, I64Type);
            WriteVarint(_out, ZigZag(value));
        }

        public void Binary(short id, string value)
        {
            FieldHeader(id, BinaryType);
            ListBinary(value);
        }

        public void BeginStruct(short id)
        {
            FieldHeader(id, Struct);
            _lastField.Push(_last);
            _last = 0;
        }

        public void EndStruct()
        {
            _out.WriteByte(0);
            _last = _lastField.Pop();
        }

        /// <summary>Starts a list field; struct elements are then written field by field, each ended with <see cref="Stop"/>.</summary>
        public void BeginList(short id, byte elementType, int count)
        {
            FieldHeader(id, ListType);
            if (count < 15) _out.WriteByte((byte)((count << 4) | elementType));
            else
            {
                _out.WriteByte((byte)(0xF0 | elementType));
                WriteVarint(_out, (ulong)count);
            }
            if (elementType == Struct)
            {
                _lastField.Push(_last);
                _pendingStructs.Push(count);
                _last = 0;
            }
        }

        private readonly Stack<int> _pendingStructs = new();

        /// <summary>Ends a struct element of a list (or the top-level struct); the list ends after its last element.</summary>
        public void Stop()
        {
            _out.WriteByte(0);
            _last = 0;
            if (_pendingStructs.Count > 0)
            {
                int left = _pendingStructs.Pop() - 1;
                if (left > 0) _pendingStructs.Push(left);
                else _last = _lastField.Pop();
            }
        }

        public void ListI32(int value) => WriteVarint(_out, ZigZag(value));

        public void ListBinary(string value)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(value);
            WriteVarint(_out, (ulong)bytes.Length);
            _out.Write(bytes);
        }
    }
}
