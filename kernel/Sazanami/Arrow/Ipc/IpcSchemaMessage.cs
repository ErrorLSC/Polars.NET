using Apache.Arrow;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>
/// A schema as one encapsulated IPC message (the continuation marker, the length, the FlatBuffer padded to 8 bytes),
/// as Arrow writers store it in Parquet's <c>ARROW:schema</c>.
/// </summary>
internal static class IpcSchemaMessage
{
    /// <summary>The message of <paramref name="schema"/>.</summary>
    /// <exception cref="NotSupportedException">A type Arrow IPC cannot hold.</exception>
    public static byte[] Write(Schema schema)
    {
        byte[] flatBuffer = IpcMetadataWriter.SchemaMessage(IpcMetadataWriter.Schema(schema));
        int padded = (flatBuffer.Length + 7) / 8 * 8;
        byte[] message = new byte[8 + padded];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(message, IpcFraming.Continuation);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(message.AsSpan(4), padded);
        flatBuffer.CopyTo(message, 8);
        return message;
    }

    /// <summary>
    /// The schema of a message (with or without the continuation marker), or null when it is damaged, is not a schema,
    /// or has a field that needs a conversion (the 128-bit integers dataframe engines write: no Arrow type of its own).
    /// </summary>
    public static Schema? TryRead(ReadOnlySpan<byte> message)
    {
        try
        {
            if (message.Length < 8) return null;
            int length = IpcFraming.MetadataLength(message, message[4..], out int prefix);
            if (length == 0 || length > message.Length - prefix) return null;
            IpcMessage parsed = IpcMetadata.ParseMessage(message.Slice(prefix, length));
            return parsed.Schema is { } schema && !schema.Fields.Any(Converted) ? schema.Schema : null;
        }
        catch (InvalidDataException)
        {
            return null;
        }

        static bool Converted(IpcField field) => field.Conversion != IpcConversion.None || field.Children.Any(Converted);
    }
}
