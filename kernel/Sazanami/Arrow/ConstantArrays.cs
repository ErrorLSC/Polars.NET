using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Helpers;

namespace Kaminari.Sazanami.Arrow;

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
