using System.Buffers;
using Kaminari.Sazanami.Bitmaps;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Deletion vectors and row bitmaps as managed arrays, for assertions.</summary>
internal static class DeletionVectorRows
{
    /// <summary>The rows in ascending order.</summary>
    public static long[] ToArray(this RoaringBitmap64 rows)
    {
        var array = new long[rows.Cardinality];
        rows.CopyTo(array);
        return array;
    }

    /// <summary>The deleted rows in ascending order.</summary>
    public static long[] DeletedRows(this DeletionVector vector) => vector.Rows.ToArray();

    /// <summary>The vector as the protocol serializes it.</summary>
    public static byte[] Serialized(this DeletionVector vector)
    {
        var output = new ArrayBufferWriter<byte>();
        vector.Serialize(output);
        Assert.Equal(vector.SerializedSize, output.WrittenCount);
        return output.WrittenSpan.ToArray();
    }

    /// <summary>The rows of a serialized vector.</summary>
    public static long[] Decode(ReadOnlySpan<byte> data)
    {
        using RoaringBitmap64 rows = DeletionVector.Decode(data);
        return rows.ToArray();
    }
}
