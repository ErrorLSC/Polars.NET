using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

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
}
