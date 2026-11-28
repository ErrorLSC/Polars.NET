using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Interop;

namespace Kaminari.Sazanami.Arrow.Builders;

/// <summary>Arrays holding one value on every row (a file's path, a partition's value), in native memory.</summary>
internal static unsafe class ConstantArrays
{
    /// <summary>
    /// <paramref name="length"/> rows of <paramref name="value"/>: in the view layout (<paramref name="view"/>) every view
    /// points at one copy of the bytes (16 bytes a row); else offsets over the bytes repeated.
    /// </summary>
    public static IArrowArray String(string value, int length, bool view)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        if (view)
        {
            using var builder = new NativeBinaryViewBuilder(StringViewType.Default);
            int buffer = bytes.Length > NativeBinaryViewBuilder.MaxInline ? builder.AddBuffer(bytes) : 0;
            builder.AppendViewSlots(length).Fill(NativeBinaryViewBuilder.View(bytes, buffer, 0));
            return builder.ExportArray();
        }
        using var strings = new NativeBinaryBuilder(StringType.Default);
        for (int i = 0; i < length; i++) strings.Append(bytes);
        return strings.ExportArray();
    }

    /// <summary><paramref name="length"/> rows of <paramref name="value"/>, of its type (see <see cref="Build"/>).</summary>
    public static IArrowArray Of(ArrowScalar value, int length)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        var export = new NativeBatchExport();
        try
        {
            return export.ImportArray(Build(export, value, length), value.Type);
        }
        catch
        {
            export.FreeAll();
            throw;
        }
    }

    /// <summary>
    /// <paramref name="length"/> rows of <paramref name="value"/> in the export's memory: nulls of any type; booleans,
    /// fixed-width values, strings and binaries in any layout (views pointing at one copy of the bytes), structs (their
    /// fields repeated) and dictionaries (every index 0 into a dictionary of the one value).
    /// </summary>
    /// <exception cref="NotSupportedException">A value of a type not listed (a list, a map, a union …) that is not null.</exception>
    /// <exception cref="OverflowException">The bytes repeated exceed what 32-bit offsets reach.</exception>
    internal static NativeCArray* Build(NativeBatchExport export, ArrowScalar value, int length)
    {
        IArrowType type = value.Type;
        if (value.Value is null) return NullArrays.Build(export, type, length);
        switch (type)
        {
            case BooleanType:
            {
                long bytes = (length + 7) / 8;
                var data = (byte*)export.Allocate(bytes);
                if ((bool)value.Value) new Span<byte>(data, (int)bytes).Fill(0xFF);
                return export.NewArray(length, 0, [0, (nint)data], []);
            }
            case DictionaryType d: // (a fixed-width type to Apache.Arrow: before those)
            {
                // Every index 0 (zeroed memory) into a dictionary of the one value.
                long width = ((IntegerType)d.IndexType).BitWidth / 8;
                nint indices = (nint)export.Allocate(Math.Max(1, length) * width);
                return export.NewArray(length, 0, [0, indices], [], Build(export, (ArrowScalar)value.Value, 1));
            }
            case FixedWidthType:
            {
                byte[] slot = (byte[])value.Value;
                long bytes = (long)slot.Length * length;
                var data = (byte*)export.AllocateUninitialized(bytes);
                Fill(data, slot, bytes);
                return export.NewArray(length, 0, [0, (nint)data], []);
            }
            case StringType or BinaryType or LargeStringType or LargeBinaryType:
            {
                byte[] bytes = (byte[])value.Value;
                bool large = type is LargeStringType or LargeBinaryType;
                long total = (long)bytes.Length * length;
                if (!large && total > int.MaxValue) throw new OverflowException($"{length} rows of a {bytes.Length}-byte value exceed what 32-bit offsets reach.");
                var offsets = (byte*)export.AllocateUninitialized((length + 1L) * (large ? 8 : 4));
                for (int i = 0; i <= length; i++)
                {
                    if (large) ((long*)offsets)[i] = (long)i * bytes.Length;
                    else ((int*)offsets)[i] = i * bytes.Length;
                }
                var data = (byte*)export.AllocateUninitialized(total);
                Fill(data, bytes, total);
                return export.NewArray(length, 0, [0, (nint)offsets, (nint)data], []);
            }
            case StringViewType or BinaryViewType:
            {
                byte[] bytes = (byte[])value.Value;
                var views = (Int128*)export.AllocateUninitialized(16L * length);
                new Span<Int128>(views, length).Fill(NativeBinaryViewBuilder.View(bytes, 0, 0));
                if (bytes.Length <= NativeBinaryViewBuilder.MaxInline) return export.NewArray(length, 0, [0, (nint)views], []);
                var data = (byte*)export.AllocateUninitialized(bytes.Length);
                bytes.CopyTo(new Span<byte>(data, bytes.Length));
                return export.NewArray(length, 0, [0, (nint)views, (nint)data], []);
            }
            case StructType s:
            {
                var fields = (ArrowScalar?[])value.Value;
                var children = new nint[s.Fields.Count];
                for (int c = 0; c < children.Length; c++)
                    children[c] = fields[c] is { } field ? (nint)Build(export, field, length) : (nint)NullArrays.Build(export, s.Fields[c].DataType, length);
                return export.NewArray(length, 0, [0], children);
            }
            default:
                throw new NotSupportedException($"A column of {type.Name} cannot be repeated from one value.");
        }

        // The slot's bytes over the whole buffer, by doubling copies.
        static void Fill(byte* data, byte[] slot, long bytes)
        {
            if (bytes == 0) return;
            slot.CopyTo(new Span<byte>(data, slot.Length));
            for (long filled = slot.Length; filled < bytes; filled *= 2)
            {
                long n = Math.Min(filled, bytes - filled);
                Buffer.MemoryCopy(data, data + filled, n, n);
            }
        }
    }

    /// <summary>
    /// <paramref name="length"/> rows of the value at <paramref name="index"/> of <paramref name="source"/> (a null
    /// when it is null), of the source's type: fixed-width values (numbers, dates, times, decimals), booleans, strings
    /// and binaries in any layout (views pointing at one copy).
    /// </summary>
    /// <exception cref="NotSupportedException">The source is nested or of a type without a flat layout.</exception>
    public static IArrowArray Repeat(IArrowArray source, int index, int length)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        IArrowType type = source.Data.DataType;
        bool valid = source.IsValid(index);
        int nulls = valid ? 0 : length;
        switch (type)
        {
            case BooleanType or FixedWidthType:
            {
                // In native memory (freed with the array), filled by doubling copies.
                var export = new NativeBatchExport();
                try
                {
                    nint validity = valid ? 0 : (nint)export.Allocate((length + 7) / 8); // zeroed: every row null
                    long bytes = type is BooleanType ? (length + 7) / 8 : (long)((FixedWidthType)type).BitWidth / 8 * length;
                    var data = (byte*)export.Allocate(bytes);
                    if (type is BooleanType)
                    {
                        if (valid && ((BooleanArray)source).GetValue(index) == true) new Span<byte>(data, (int)bytes).Fill(0xFF);
                    }
                    else if (valid && length > 0)
                    {
                        int width = ((FixedWidthType)type).BitWidth / 8;
                        source.Data.Buffers[1].Span.Slice((source.Offset + index) * width, width).CopyTo(new Span<byte>(data, width));
                        for (long filled = width; filled < bytes; filled *= 2)
                        {
                            long n = Math.Min(filled, bytes - filled);
                            Buffer.MemoryCopy(data, data + filled, n, n);
                        }
                    }
                    return export.ImportArray(export.NewArray(length, nulls, [validity, (nint)data], []), type);
                }
                catch
                {
                    export.FreeAll();
                    throw;
                }
            }
            case StringType or BinaryType or StringViewType or BinaryViewType:
            {
                byte[] bytes = valid ? Bytes(source, index) : [];
                using NativeColumnBuilder builder = NativeColumnBuilder.Create(type);
                if (!valid)
                {
                    for (int i = 0; i < length; i++) builder.AppendNull();
                }
                else if (builder is NativeBinaryViewBuilder views)
                {
                    int buffer = bytes.Length > NativeBinaryViewBuilder.MaxInline ? views.AddBuffer(bytes) : 0;
                    views.AppendViewSlots(length).Fill(NativeBinaryViewBuilder.View(bytes, buffer, 0));
                }
                else
                {
                    var binary = (NativeBinaryBuilder)builder;
                    for (int i = 0; i < length; i++) binary.Append(bytes);
                }
                return builder.ExportArray();
            }
            default:
                throw new NotSupportedException($"A column of {type.Name} cannot be repeated from one value.");
        }

        static byte[] Bytes(IArrowArray array, int index) => array switch
        {
            BinaryArray b => b.GetBytes(index).ToArray(),
            BinaryViewArray b => b.GetBytes(index).ToArray(),
            _ => throw new NotSupportedException(array.GetType().Name),
        };
    }
}
