using System.Runtime.CompilerServices;

namespace Kaminari.Sazanami.Buffers;

/// <summary>The bytes of a <see cref="MemoryStream"/>, read in place even when the stream hides its buffer.</summary>
internal static class MemoryStreams
{
    /// <summary>
    /// The stream's bytes from its origin to its length: the visible buffer (<see cref="MemoryStream.TryGetBuffer"/>),
    /// or, for a plain <see cref="MemoryStream"/> made over an array without exposing it (<c>new MemoryStream(bytes)</c>),
    /// its private buffer read through <see cref="UnsafeAccessorAttribute"/> (no reflection; trimming and AOT safe).
    /// Only reading is done with it: hiding the buffer keeps others from writing into it through the stream's API.
    /// False for subclasses (which may keep their bytes elsewhere) and should a runtime rename the fields.
    /// </summary>
    public static bool TryGetBytes(MemoryStream stream, out ArraySegment<byte> bytes)
    {
        if (stream.TryGetBuffer(out bytes)) return true;
        if (stream.GetType() != typeof(MemoryStream))
        {
            bytes = default;
            return false;
        }
        try
        {
            int origin = Origin(stream);
            bytes = new ArraySegment<byte>(Buffer(stream), origin, checked((int)stream.Length));
            return true;
        }
        catch (Exception e) when (e is MissingFieldException or ArgumentException)
        {
            bytes = default;
            return false;
        }
    }

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_buffer")]
    private static extern ref byte[] Buffer(MemoryStream stream);

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_origin")]
    private static extern ref int Origin(MemoryStream stream);
}
