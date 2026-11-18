using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>Reading IPC messages and bodies at offsets (the file format), shared by <see cref="IpcFile"/> and <see cref="IpcReader"/>.</summary>
internal static class IpcIO
{
    /// <summary>Byte gaps up to this between the buffers a projection needs are read rather than skipped.</summary>
    public const long Gap = 1 << 20;

    private const int Chunk = 1 << 30;

    public static T Sync<T>(ValueTask<T> task) => task.IsCompleted ? task.Result : task.AsTask().GetAwaiter().GetResult();

    public static void Sync(ValueTask task)
    {
        if (task.IsCompletedSuccessfully) return;
        task.AsTask().GetAwaiter().GetResult();
    }

    public static IpcFormatException Format(InvalidDataException e) => new(e.Message, e);

    /// <summary>Reads exactly <paramref name="length"/> bytes at <paramref name="offset"/> into native memory at <paramref name="destination"/>.</summary>
    public static async ValueTask ReadAsync(IRandomAccess source, long offset, nint destination, long length, bool useAsync, CancellationToken ct)
    {
        for (long done = 0; done < length;)
        {
            int n = (int)Math.Min(Chunk, length - done);
            int read = useAsync
                ? await source.ReadAtAsync(offset + done, Memory(destination + (nint)done, n), ct).ConfigureAwait(false)
                : Read(source, offset + done, destination + (nint)done, n);
            if (read != n) throw new IpcFormatException($"The Arrow IPC data ended early: {done + read} of {length} bytes at offset {offset}.");
            done += n;
        }

        static unsafe Memory<byte> Memory(nint p, int n) => new PointerMemory((byte*)p, n).Memory;
        static unsafe int Read(IRandomAccess source, long offset, nint p, int n) => source.ReadAt(offset, new Span<byte>((void*)p, n));
    }

    /// <summary>Reads the message a footer block points to (its metadata; the body stays where it is).</summary>
    public static async ValueTask<IpcMessage> MessageAsync(IRandomAccess source, IpcBlock block, bool useAsync, CancellationToken ct)
    {
        nint scratch = Alloc(block.MetadataLength);
        try
        {
            await ReadAsync(source, block.Offset, scratch, block.MetadataLength, useAsync, ct).ConfigureAwait(false);
            return Parse(Bytes(scratch, block.MetadataLength), block);
        }
        finally
        {
            Free(scratch);
        }
    }

    public static unsafe nint Alloc(long bytes) => (nint)NativeMemoryPool.Alloc(Math.Max(bytes, 1));

    public static unsafe void Free(nint p) => NativeMemoryPool.Free((void*)p);

    public static unsafe ReadOnlySpan<byte> Bytes(nint p, int length) => new((void*)p, length);

    private static IpcMessage Parse(ReadOnlySpan<byte> bytes, IpcBlock block)
    {
        try
        {
            if (bytes.Length < 8) throw new InvalidDataException($"Arrow IPC data is damaged: a message block of {bytes.Length} metadata bytes.");
            int length = IpcFraming.MetadataLength(bytes, bytes[4..], out int prefix);
            if (length == 0 || length > bytes.Length - prefix)
                throw new InvalidDataException($"Arrow IPC data is damaged: the message at {block.Offset} has {length} metadata bytes in a block of {bytes.Length}.");
            IpcMessage message = IpcMetadata.ParseMessage(bytes.Slice(prefix, length));
            if (message.BodyLength != block.BodyLength)
                throw new InvalidDataException($"Arrow IPC data is damaged: the message at {block.Offset} has a {message.BodyLength}-byte body; the footer says {block.BodyLength}.");
            return message;
        }
        catch (InvalidDataException e)
        {
            throw Format(e);
        }
    }

    /// <summary>
    /// The body of a message at <paramref name="bodyStart"/>: from the mapping when <paramref name="map"/> is given (and
    /// still mapped), else the ranges holding the <paramref name="needed"/> buffers read into native memory.
    /// </summary>
    public static async ValueTask<IpcBody> BodyAsync(IRandomAccess source, MappedFile? map, long mapStart, long bodyStart, IpcMessage message,
        bool[] needed, bool useAsync, CancellationToken ct)
    {
        IpcRecordBatch header = message.RecordBatch ?? message.DictionaryBatch!.Data;
        long length = message.BodyLength;
        if (map is not null && map.TryAcquire())
        {
            unsafe
            {
                byte* p;
                try
                {
                    p = map.At(mapStart + bodyStart, length);
                }
                catch (ArgumentOutOfRangeException)
                {
                    map.Release();
                    throw new IpcFormatException($"The Arrow IPC body at {bodyStart} ({length} bytes) reaches past the file.");
                }
                return new IpcBody(new IpcBodyMemory(map), length, [new IpcBodySegment(0, (nint)p, length)]);
            }
        }
        List<(long Start, long Length)> ranges = IpcFraming.Ranges(header.Buffers, needed, Gap);
        var memory = new IpcBodyMemory();
        try
        {
            var segments = new IpcBodySegment[ranges.Count];
            for (int i = 0; i < ranges.Count; i++)
            {
                (long start, long n) = ranges[i];
                nint p = Allocate(memory, n);
                await ReadAsync(source, bodyStart + start, p, n, useAsync, ct).ConfigureAwait(false);
                segments[i] = new IpcBodySegment(start, p, n);
            }
            return new IpcBody(memory, length, segments);
        }
        catch
        {
            memory.Sealed();
            throw;
        }

        static unsafe nint Allocate(IpcBodyMemory memory, long n) => (nint)memory.Allocate(n);
    }

    /// <summary>Decodes a dictionary batch and applies it (a delta appends, else it replaces; files allow no replacement).</summary>
    public static void Dictionary(IpcSchema schema, IpcMessage message, IpcBody body, IpcDictionaries dictionaries, IpcMetadataVersion version,
        bool validateUtf8, bool replacements)
    {
        try
        {
            IpcDictionaryBatch batch = message.DictionaryBatch!;
            if (!schema.Dictionaries.TryGetValue(batch.Id, out IpcField? field))
                throw new InvalidDataException($"Arrow IPC data is damaged: dictionary {batch.Id} belongs to no field.");
            IpcField values = field.ValueField();
            IpcArray array = IpcBatchDecoder.Decode([values], null, batch.Data, version, body, dictionaries, validateUtf8)[0]!;
            if (batch.IsDelta) dictionaries.Append(batch.Id, array, $"dictionary {batch.Id}");
            else
            {
                if (!replacements && dictionaries.Contains(batch.Id))
                    throw new InvalidDataException($"Arrow IPC data is invalid: dictionary {batch.Id} is replaced, which the file format does not allow.");
                dictionaries.Set(batch.Id, array);
            }
        }
        catch (InvalidDataException e)
        {
            throw Format(e);
        }
        finally
        {
            body.Memory.Sealed();
        }
    }

    /// <summary>Which buffers of a message the selected fields need.</summary>
    public static bool[] Plan(IpcField[] fields, bool[]? selected, IpcMessage message, IpcMetadataVersion version)
    {
        try
        {
            return IpcBatchDecoder.Plan(fields, selected, message.RecordBatch ?? message.DictionaryBatch!.Data, version, message.BodyLength);
        }
        catch (InvalidDataException e)
        {
            throw Format(e);
        }
    }
}
