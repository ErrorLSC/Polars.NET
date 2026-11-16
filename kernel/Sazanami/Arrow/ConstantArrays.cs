using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Helpers;

namespace Kaminari.Sazanami.Arrow;

/// <summary>Arrays holding one value on every row (a file's path, a partition's value), in native memory.</summary>
internal static class ConstantArrays
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
        ArrowBuffer validity = valid ? ArrowBuffer.Empty : new ArrowBuffer(new byte[(length + 7) / 8]);
        int nulls = valid ? 0 : length;
        switch (type)
        {
            case BooleanType:
            {
                bool value = valid && ((BooleanArray)source).GetValue(index) == true;
                var bits = new byte[(length + 7) / 8];
                if (value) bits.AsSpan().Fill(0xFF);
                return ArrowArrayFactory.BuildArray(new ArrayData(type, length, nulls, 0, [validity, new ArrowBuffer(bits)]));
            }
            case FixedWidthType fixedWidth:
            {
                int width = fixedWidth.BitWidth / 8;
                var data = new byte[(long)width * length];
                if (valid)
                {
                    ReadOnlySpan<byte> value = source.Data.Buffers[1].Span.Slice((source.Offset + index) * width, width);
                    for (int i = 0; i < length; i++) value.CopyTo(data.AsSpan(i * width, width));
                }
                return ArrowArrayFactory.BuildArray(new ArrayData(type, length, nulls, 0, [validity, new ArrowBuffer(data)]));
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
