using System.IO.Compression;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Files.Excel.Xml;

namespace Kaminari.Seseragi.Files.Excel.Xlsx;

/// <summary>A relationship of an OPC part: id, type URI and the target part name (resolved, without a leading slash).</summary>
internal readonly record struct OpcRelationship(string Id, string Type, string Target)
{
    /// <summary>Whether the type URI ends with <c>/</c><paramref name="suffix"/> (transitional and strict URIs differ
    /// only before the last segment).</summary>
    public bool IsType(string suffix) =>
        Type.EndsWith(suffix, StringComparison.Ordinal) && Type.Length > suffix.Length && Type[^(suffix.Length + 1)] == '/';
}

/// <summary>
/// The zip container of an xlsx workbook: part lookup (part names are case-insensitive), relationships, and the
/// small parts read whole. A stream that cannot seek is first copied into native memory (the zip directory sits at
/// the end of the file).
/// </summary>
internal sealed class XlsxPackage : IDisposable
{
    private readonly ZipArchive _zip;
    private readonly Dictionary<string, ZipArchiveEntry> _entries = new(StringComparer.OrdinalIgnoreCase);
    private readonly Stream? _ownedStream;
    private readonly NativeByteBuffer? _copy;

    private XlsxPackage(Stream stream, bool leaveOpen, Stream? ownedStream, NativeByteBuffer? copy)
    {
        _ownedStream = ownedStream;
        _copy = copy;
        try
        {
            _zip = new ZipArchive(stream, ZipArchiveMode.Read, leaveOpen);
        }
        catch (InvalidDataException e)
        {
            Dispose();
            throw new ExcelFormatException("The workbook is not a valid zip archive.", e);
        }
        foreach (var e in _zip.Entries) _entries.TryAdd(e.FullName.Replace('\\', '/').TrimStart('/'), e);
    }

    /// <summary>Opens a package over a stream; a non-seekable stream is copied into native memory first.</summary>
    public static XlsxPackage Open(Stream stream, bool leaveOpen)
    {
        if (stream.CanSeek) return new(stream, leaveOpen, null, null);
        var copy = new NativeByteBuffer(1 << 20);
        try
        {
            var sink = new NativeByteBufferStream(copy);
            stream.CopyTo(sink);
            if (!leaveOpen) stream.Dispose();
            return FromCopy(copy, (int)sink.Length);
        }
        catch
        {
            copy.Release();
            throw;
        }
    }

    /// <inheritdoc cref="Open"/>
    public static async Task<XlsxPackage> OpenAsync(Stream stream, bool leaveOpen, CancellationToken cancellationToken)
    {
        if (stream.CanSeek) return new(stream, leaveOpen, null, null);
        var copy = new NativeByteBuffer(1 << 20);
        try
        {
            var sink = new NativeByteBufferStream(copy);
            await stream.CopyToAsync(sink, cancellationToken).ConfigureAwait(false);
            if (!leaveOpen) await stream.DisposeAsync().ConfigureAwait(false);
            return FromCopy(copy, (int)sink.Length);
        }
        catch
        {
            copy.Release();
            throw;
        }
    }

    private static unsafe XlsxPackage FromCopy(NativeByteBuffer copy, int length)
    {
        var memory = new UnmanagedMemoryStream((byte*)copy.Pin().Pointer, length);
        return new(memory, false, memory, copy);
    }

    public bool HasPart(string name) => _entries.ContainsKey(name);

    /// <summary>Opens a part for streaming (decompressing) reads.</summary>
    public Stream OpenPart(string name) => _entries.TryGetValue(name, out var e)
        ? e.Open()
        : throw new ExcelFormatException($"The workbook lacks the part '{name}'.");

    /// <summary>Reads a small part whole.</summary>
    public byte[] ReadPart(string name)
    {
        if (!_entries.TryGetValue(name, out var e)) throw new ExcelFormatException($"The workbook lacks the part '{name}'.");
        using var s = e.Open();
        var bytes = new byte[e.Length];
        s.ReadExactly(bytes);
        return SkipBom(bytes);
    }

    private static byte[] SkipBom(byte[] bytes)
    {
        if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF) return bytes[3..];
        if (bytes.Length >= 2 && bytes[0] is 0xFE or 0xFF && bytes[1] is 0xFE or 0xFF)
            throw new ExcelFormatException("UTF-16 encoded workbook parts are not supported.");
        return bytes;
    }

    /// <summary>The relationships of a part (the package itself for <c>""</c>); empty when it has none.</summary>
    public IReadOnlyList<OpcRelationship> Relationships(string sourcePart)
    {
        int slash = sourcePart.LastIndexOf('/');
        string dir = slash < 0 ? "" : sourcePart[..(slash + 1)];
        string file = slash < 0 ? sourcePart : sourcePart[(slash + 1)..];
        string relsName = $"{dir}_rels/{file}.rels";
        if (!HasPart(relsName)) return [];
        var bytes = ReadPart(relsName);
        List<OpcRelationship> list = [];
        var x = new XmlSpanScanner(bytes);
        while (x.Read())
        {
            if (x.Token != XmlToken.StartElement || !x.Is("Relationship"u8)) continue;
            if (x.TryGetAttribute("TargetMode"u8, out var mode) && mode.SequenceEqual("External"u8)) continue;
            if (!x.TryGetAttribute("Id"u8, out var id) || !x.TryGetAttribute("Type"u8, out var type)
                || !x.TryGetAttribute("Target"u8, out var target)) continue;
            list.Add(new(XmlText.DecodeString(id, false), XmlText.DecodeString(type, false),
                Resolve(dir, Uri.UnescapeDataString(XmlText.DecodeString(target, false)))));
        }
        return list;
    }

    // A relationship target relative to the source part's folder, or absolute from the package root.
    private static string Resolve(string dir, string target)
    {
        target = target.Replace('\\', '/');
        string path = target.StartsWith('/') ? target[1..] : dir + target;
        if (!path.Contains("..") && !path.Contains("./")) return path;
        List<string> parts = [];
        foreach (var seg in path.Split('/'))
        {
            if (seg is "" or ".") continue;
            if (seg == "..")
            {
                if (parts.Count > 0) parts.RemoveAt(parts.Count - 1);
                continue;
            }
            parts.Add(seg);
        }
        return string.Join('/', parts);
    }

    public void Dispose()
    {
        _zip?.Dispose();
        _ownedStream?.Dispose();
        _copy?.Release();
    }
}
