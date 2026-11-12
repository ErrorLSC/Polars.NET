using System.Buffers.Binary;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Core.Delta;

/// <summary>
/// OUR OWN PATCH around the engine, TEMPORARY: the engine's Parquet reader (Polars 0.55, and 2.1 likewise) refuses a
/// whole file whose footer annotates a group with the VARIANT logical type (pola-rs/polars#28627), which every
/// variant column Spark writes carries. A variant is stored as a plain group of two binaries (<c>value</c>,
/// <c>metadata</c>), so the file reads correctly once the annotation is gone: this removes those bytes from the
/// footer. Data pages, column and offset indexes lie before the footer and keep their offsets. The patched file
/// lives in memory, so the scans of such tables read each file whole from storage (no ranged reads; the engine
/// still decodes only the columns and row groups it needs). Delete this, and the in-memory scan path, once the engine reads VARIANT.
/// </summary>
internal static class VariantFooter
{
    private const int Tail = 8; // footer length (4 bytes, little-endian) + "PAR1"

    /// <summary>The file with the VARIANT annotations removed from its footer; the same array when it has none.</summary>
    /// <exception cref="PolarsException">The file is not Parquet, or its footer has a shape this patch does not handle.</exception>
    public static byte[] Strip(byte[] file, string path)
    {
        if (file.Length < 12 || !file.AsSpan(file.Length - 4).SequenceEqual("PAR1"u8))
            throw new PolarsException($"{path} is not a Parquet file.");
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(file.Length - Tail));
        int footerStart = file.Length - Tail - footerLength;
        if (footerLength <= 0 || footerStart < 4) throw new PolarsException($"The Parquet footer of {path} is damaged.");

        List<(int Start, int End)> spans;
        try
        {
            var walker = new Walker(file.AsSpan(footerStart, footerLength));
            walker.FileMetaData();
            spans = walker.Annotations;
        }
        catch (Exception e) when (e is IndexOutOfRangeException or ArgumentOutOfRangeException or InvalidDataException)
        {
            throw new PolarsException($"The Parquet footer of {path} could not be read: {e.Message}", e);
        }
        if (spans.Count == 0) return file;

        int removed = spans.Sum(s => s.End - s.Start);
        int newFooterLength = footerLength - removed;
        var patched = new byte[file.Length - removed];
        file.AsSpan(0, footerStart).CopyTo(patched);
        int to = footerStart, from = footerStart;
        foreach (var (start, end) in spans)
        {
            file.AsSpan(from, footerStart + start - from).CopyTo(patched.AsSpan(to));
            to += footerStart + start - from;
            from = footerStart + end;
        }
        file.AsSpan(from, footerStart + footerLength - from).CopyTo(patched.AsSpan(to));
        BinaryPrimitives.WriteInt32LittleEndian(patched.AsSpan(patched.Length - Tail), newFooterLength);
        "PAR1"u8.CopyTo(patched.AsSpan(patched.Length - 4));
        return patched;
    }

    /// <summary>
    /// Walks the Thrift compact encoding of <c>FileMetaData</c>, noting the bytes of every <c>SchemaElement</c>'s
    /// <c>logicalType</c> field (id 10) that holds the VARIANT member (id 16) of the <c>LogicalType</c> union.
    /// </summary>
    private ref struct Walker(ReadOnlySpan<byte> footer)
    {
        private readonly ReadOnlySpan<byte> _data = footer;
        private int _at;

        public List<(int Start, int End)> Annotations { get; } = [];

        public void FileMetaData()
        {
            short last = 0;
            while (FieldHeader(ref last) is var (id, type) && type != Stop)
            {
                if (id == 2 && type == List) SchemaElements();
                else Skip(type);
            }
        }

        private void SchemaElements()
        {
            var (size, elementType) = ListHeader();
            for (int i = 0; i < size; i++)
            {
                if (elementType != Struct) throw new InvalidDataException("The schema is not a list of structs.");
                SchemaElement();
            }
        }

        private void SchemaElement()
        {
            short last = 0;
            (int Start, int End)? variant = null;
            while (true)
            {
                int start = _at;
                var (id, type) = FieldHeader(ref last);
                if (type == Stop) break;
                if (variant is not null)
                    throw new InvalidDataException("A field follows the VARIANT annotation of a schema element; this patch removes only a last one.");
                if (id == 10 && type == Struct)
                {
                    if (IsVariant()) variant = (start, _at);
                }
                else
                {
                    Skip(type);
                }
            }
            if (variant is { } span) Annotations.Add(span);
        }

        /// <summary>Reads a <c>LogicalType</c> union; whether its member is VARIANT (id 16).</summary>
        private bool IsVariant()
        {
            short last = 0;
            bool variant = false;
            while (FieldHeader(ref last) is var (id, type) && type != Stop)
            {
                variant |= id == 16;
                Skip(type);
            }
            return variant;
        }

        // ---- the compact protocol

        private const int Stop = 0, True = 1, False = 2, Byte = 3, I16 = 4, I32 = 5, I64 = 6, Double = 7, Binary = 8,
            List = 9, Set = 10, Map = 11, Struct = 12;

        private (short Id, int Type) FieldHeader(ref short last)
        {
            byte header = _data[_at++];
            int type = header & 0x0F;
            if (type == Stop) return (0, Stop);
            int delta = header >> 4;
            short id = delta != 0 ? (short)(last + delta) : (short)ZigZag(Varint());
            last = id;
            return (id, type);
        }

        private (int Size, int ElementType) ListHeader()
        {
            byte header = _data[_at++];
            int size = header >> 4;
            if (size == 15) size = checked((int)Varint());
            return (size, header & 0x0F);
        }

        private void Skip(int type)
        {
            switch (type)
            {
                case True or False:
                    break; // a field's boolean lives in its header
                case Byte:
                    _at++;
                    break;
                case I16 or I32 or I64:
                    Varint();
                    break;
                case Double:
                    _at += 8;
                    break;
                case Binary:
                {
                    // Read the length first: `_at += Varint()` would add it to the position from before the varint.
                    int length = checked((int)Varint());
                    _at += length;
                    break;
                }
                case List or Set:
                {
                    var (size, element) = ListHeader();
                    for (int i = 0; i < size; i++) SkipElement(element);
                    break;
                }
                case Map:
                {
                    int size = checked((int)Varint());
                    if (size == 0) break;
                    byte kinds = _data[_at++];
                    for (int i = 0; i < size; i++)
                    {
                        SkipElement(kinds >> 4);
                        SkipElement(kinds & 0x0F);
                    }
                    break;
                }
                case Struct:
                {
                    short last = 0;
                    while (FieldHeader(ref last) is var (_, inner) && inner != Stop) Skip(inner);
                    break;
                }
                default:
                    throw new InvalidDataException($"Unknown Thrift compact type {type}.");
            }
        }

        // In lists, sets and maps a boolean takes a byte of its own.
        private void SkipElement(int type)
        {
            if (type is True or False) _at++;
            else Skip(type);
        }

        private ulong Varint()
        {
            ulong value = 0;
            for (int shift = 0; ; shift += 7)
            {
                if (shift > 63) throw new InvalidDataException("A varint is too long.");
                byte b = _data[_at++];
                value |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) return value;
            }
        }

        private static long ZigZag(ulong n) => (long)(n >> 1) ^ -(long)(n & 1);
    }
}
