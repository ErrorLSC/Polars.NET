using System.Buffers.Binary;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>Opening Parquet files from paths, streams and random-access sources.</summary>
public class ParquetFileTests
{
    private const string Sample = "apache/data/alltypes_plain.parquet";

    private static string Expected(string name) => ParquetFooterTests.RenderView(ParquetMetadata.Parse(ParquetFixtures.FooterBytes(name)));

    [Fact]
    public async Task EveryOpenPath_ReadsTheSameFooter()
    {
        string path = ParquetFixtures.FullPath(Sample);
        string expected = Expected(Sample);
        byte[] bytes = File.ReadAllBytes(path);

        using (var file = ParquetFile.Open(path)) Assert.Equal(expected, ParquetFooterTests.RenderView(file.Metadata));
        await using (var file = await ParquetFile.OpenAsync(path)) Assert.Equal(expected, ParquetFooterTests.RenderView(file.Metadata));
        using (var file = ParquetFile.Open(new MemoryStream(bytes))) Assert.Equal(expected, ParquetFooterTests.RenderView(file.Metadata));
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes))) Assert.Equal(expected, ParquetFooterTests.RenderView(file.Metadata));
        using (var file = ParquetFile.Open(new ForwardOnlyStream(bytes))) Assert.Equal(expected, ParquetFooterTests.RenderView(file.Metadata));
        await using (var file = await ParquetFile.OpenAsync(new ForwardOnlyStream(bytes))) Assert.Equal(expected, ParquetFooterTests.RenderView(file.Metadata));
        using (var file = ParquetFile.Open(new MemorySource(bytes))) Assert.Equal(expected, ParquetFooterTests.RenderView(file.Metadata));
        await using (var file = await ParquetFile.OpenAsync(new MemorySource(bytes))) Assert.Equal(expected, ParquetFooterTests.RenderView(file.Metadata));
    }

    [Fact]
    public void Stream_IsReadFromItsPosition_AndDisposedUnlessLeftOpen()
    {
        byte[] bytes = [.. "junk"u8, .. File.ReadAllBytes(ParquetFixtures.FullPath(Sample))];
        var stream = new MemoryStream(bytes) { Position = 4 };
        using (var file = ParquetFile.Open(stream, leaveOpen: true)) Assert.Equal(8, file.Metadata.RowCount);
        Assert.True(stream.CanRead);

        stream.Position = 4; // reading at offsets moves a shared stream
        using (ParquetFile.Open(stream)) { }
        Assert.False(stream.CanRead);

        var bad = new MemoryStream([.. "not parquet at all"u8]);
        Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(bad));
        Assert.False(bad.CanRead);
        var kept = new MemoryStream([.. "not parquet at all"u8]);
        Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(kept, leaveOpen: true));
        Assert.True(kept.CanRead);
    }

    /// <summary>A footer longer than the first tail read (64 KiB) is read with a second, exact read.</summary>
    [Fact]
    public async Task FooterLongerThanTheTailGuess_IsReadInASecondRead()
    {
        byte[] original = File.ReadAllBytes(ParquetFixtures.FullPath(Sample));
        FileMetaData meta = FileMetaData.Parse(ParquetFixtures.FooterBytes(Sample));
        string big = new('x', 100_000);
        var grown = new FileMetaData
        {
            Version = meta.Version,
            Schema = meta.Schema,
            NumRows = meta.NumRows,
            RowGroups = meta.RowGroups,
            KeyValueMetadata = [.. meta.KeyValueMetadata ?? [], new KeyValue("big", big)],
            CreatedBy = meta.CreatedBy,
        };
        byte[] footer = ParquetFooterTests.Encode(grown);
        int dataEnd = original.Length - 8 - BinaryPrimitives.ReadInt32LittleEndian(original.AsSpan(original.Length - 8));
        byte[] length = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, footer.Length);
        byte[] file = [.. original.AsSpan(0, dataEnd), .. footer, .. length, .. "PAR1"u8];

        var source = new MemorySource(file);
        using (var opened = ParquetFile.Open(source))
        {
            Assert.Equal(big, opened.Metadata.KeyValueMetadata.Single(p => p.Key == "big").Value);
            Assert.Equal(8, opened.Metadata.RowCount);
        }
        Assert.Equal(2, source.Reads);
        await using var asyncOpened = await ParquetFile.OpenAsync(new MemorySource(file));
        Assert.Equal(big, asyncOpened.Metadata.KeyValueMetadata.Single(p => p.Key == "big").Value);
    }

    [Fact]
    public void SmallFile_IsOpenedWithOneRead()
    {
        var source = new MemorySource(File.ReadAllBytes(ParquetFixtures.FullPath(Sample)));
        using var file = ParquetFile.Open(source);
        Assert.Equal(1, source.Reads);
        Assert.Equal(ParquetFixtures.Expected(Sample).GetProperty("created_by").GetString(), file.Metadata.CreatedBy);
    }

    [Theory]
    [InlineData(new byte[0], "too short")]
    [InlineData(new byte[] { 0x50, 0x41, 0x52, 0x31, 0x50, 0x41, 0x52, 0x31 }, "too short")]
    public void TooShort_IsRefused(byte[] bytes, string message)
    {
        var e = Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(new MemorySource(bytes)));
        Assert.Contains(message, e.Message);
    }

    [Fact]
    public void WrongMagic_LengthAndShortSource_AreRefused()
    {
        byte[] good = File.ReadAllBytes(ParquetFixtures.FullPath(Sample));

        byte[] noHead = (byte[])good.Clone();
        noHead[0] = (byte)'X';
        Assert.Contains("no PAR1 at the start", Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(new MemorySource(noHead))).Message);

        byte[] noTail = (byte[])good.Clone();
        noTail[^1] = (byte)'X';
        Assert.Contains("no PAR1 at the end", Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(new MemorySource(noTail))).Message);

        byte[] encrypted = (byte[])good.Clone();
        encrypted[^1] = (byte)'E';
        Assert.Contains("encrypted", Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(new MemorySource(encrypted))).Message);

        byte[] longFooter = (byte[])good.Clone();
        BinaryPrimitives.WriteInt32LittleEndian(longFooter.AsSpan(longFooter.Length - 8), good.Length);
        Assert.Contains("longer than the file", Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(new MemorySource(longFooter))).Message);

        byte[] negative = (byte[])good.Clone();
        BinaryPrimitives.WriteInt32LittleEndian(negative.AsSpan(negative.Length - 8), -5);
        Assert.Contains("invalid footer length", Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(new MemorySource(negative))).Message);

        Assert.Contains("ended early", Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(new MemorySource(good, lies: true))).Message);
    }

    [Fact]
    public void PlaintextFooterEncryption_IsRefusedAtOpen_ButTheFooterParses()
    {
        const string name = "apache/data/encrypt_columns_plaintext_footer.parquet.encrypted";
        var e = Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(ParquetFixtures.FullPath(name)));
        Assert.Contains("plaintext footer", e.Message);
        Assert.NotNull(FileMetaData.Parse(ParquetFixtures.FooterBytes(name)).EncryptionAlgorithm);
        Assert.Contains("encrypted", Assert.Throws<ParquetFormatException>(() => ParquetMetadata.Parse(ParquetFixtures.FooterBytes(name))).Message);
    }

    [Fact]
    public void InconsistentSchemaOrRowGroups_AreRefused()
    {
        FileMetaData meta = FileMetaData.Parse(ParquetFixtures.FooterBytes(Sample));
        SchemaElement root = meta.Schema[0];

        FileMetaData fewer = With(meta, [new SchemaElement { Name = root.Name, NumChildren = root.NumChildren + 1 }, .. meta.Schema[1..]]);
        Assert.Contains("fewer than its child counts", Assert.Throws<ParquetFormatException>(() => ParquetMetadata.FromThrift(fewer)).Message);

        FileMetaData more = With(meta, [new SchemaElement { Name = root.Name, NumChildren = root.NumChildren - 1 }, .. meta.Schema[1..]]);
        Assert.Contains("its tree takes", Assert.Throws<ParquetFormatException>(() => ParquetMetadata.FromThrift(more)).Message);

        FileMetaData negative = With(meta, [new SchemaElement { Name = root.Name, NumChildren = -1 }, .. meta.Schema[1..]]);
        Assert.Contains("-1 children", Assert.Throws<ParquetFormatException>(() => ParquetMetadata.FromThrift(negative)).Message);

        FileMetaData chunks = With(meta, meta.Schema, [new RowGroup { Columns = meta.RowGroups[0].Columns[1..], TotalByteSize = 0, NumRows = 8 }]);
        Assert.Contains("column chunks for", Assert.Throws<ParquetFormatException>(() => ParquetMetadata.FromThrift(chunks)).Message);

        static FileMetaData With(FileMetaData m, SchemaElement[] schema, RowGroup[]? groups = null) => new()
        {
            Version = m.Version,
            Schema = schema,
            NumRows = m.NumRows,
            RowGroups = groups ?? m.RowGroups,
        };
    }

    // ------------------------------------------------------------------

    /// <summary>A stream that cannot seek, as from a network.</summary>
    private sealed class ForwardOnlyStream(byte[] bytes) : Stream
    {
        private readonly MemoryStream _inner = new(bytes);

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>Bytes read at offsets, counting the reads; one that <paramref name="lies"/> claims to be longer than it is.</summary>
    private sealed class MemorySource(byte[] bytes, bool lies = false) : IRandomAccess
    {
        public int Reads { get; private set; }

        public long Length => bytes.Length + (lies ? 10 : 0);

        public int ReadAt(long offset, Span<byte> destination)
        {
            Reads++;
            int count = (int)Math.Clamp(bytes.Length - offset, 0, destination.Length);
            bytes.AsSpan((int)offset, count).CopyTo(destination);
            return count;
        }

        public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ReadAt(offset, destination.Span));
    }
}
