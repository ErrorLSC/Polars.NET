using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Writing;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// A chunk's dictionary as its pages are written, on any number of threads (pages are mapped to dictionaries of their
/// own, merged in order): the dictionary page holds the values in order of first appearance; each dictionary-encoded
/// page's indices are as wide as the dictionary is long once the page is in; pages go plain from the page after the
/// one that made the dictionary outgrow DictionaryPageBytes; a page of nulls after the first page holding values is
/// dictionary-encoded still. Read from the file's raw pages and checked against those rules worked out here.
/// </summary>
public class ParquetWriterDictionaryTests
{
    private const int Rows = 1000, PageRows = 100;

    /// <summary>Page p's values: a mix of the values before and some new ones, more pages bringing more (the dictionary outgrows 400 bytes on page 3).</summary>
    private static int Value(int row)
    {
        int page = row / PageRows;
        return (int)((uint)(row * 7919 + 13) % (uint)(20 + page * 30)) * 3 - 50;
    }

    private static byte[] Written(int threads)
    {
        var ints = new Int32Array.Builder();
        var strings = new StringArray.Builder();
        for (int row = 0; row < Rows; row++)
        {
            ints.Append(Value(row));
            // Page 1 holds nulls alone, after the first page holding values.
            if (row / PageRows == 1) strings.AppendNull();
            else strings.Append($"v{row % 7}");
        }
        var schema = new Schema([new Field("i", Int32Type.Default, false), new Field("s", StringType.Default, true)], null);
        using var batch = new RecordBatch(schema, [ints.Build(), strings.Build()], Rows);
        using var stream = new MemoryStream();
        ParquetWriter.Write(stream, schema, [batch], new ParquetWriteOptions
        {
            Compression = NoCompression.Instance,
            DataPageRows = PageRows,
            DictionaryPageBytes = 400,
            MaxDegreeOfParallelism = threads,
            LeaveOpen = true,
        });
        return stream.ToArray();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(16)]
    public void TheDictionary_IsInOrderOfFirstAppearance_AndEndsWhereItOutgrowsItsBytes(int threads)
    {
        byte[] bytes = Written(threads);
        // The rules, worked out from the values: pages dictionary-encoded until the dictionary has outgrown 400 bytes
        // (100 int32 entries) after a page, then plain; each page's width that of the entries so far.
        List<int> order = [];
        HashSet<int> seen = [];
        List<(bool Dictionary, int Width)> expected = [];
        bool full = false;
        for (int page = 0; page < Rows / PageRows; page++)
        {
            if (full)
            {
                expected.Add((false, 0));
                continue;
            }
            for (int row = page * PageRows; row < (page + 1) * PageRows; row++)
            {
                if (seen.Add(Value(row))) order.Add(Value(row));
            }
            expected.Add((true, order.Count <= 1 ? 0 : 32 - int.LeadingZeroCount(order.Count - 1)));
            full = order.Count * 4 > 400;
        }
        Assert.Contains(expected, p => !p.Dictionary); // the dictionary does outgrow its bytes

        var (dictionary, pages) = Pages(bytes, column: 0);
        Assert.Equal(order, [.. Enumerable.Range(0, dictionary.Length / 4).Select(k => BinaryPrimitives.ReadInt32LittleEndian(dictionary.AsSpan(k * 4)))]);
        Assert.Equal(expected, [.. pages.Select(p => (p.Encoding == ParquetEncoding.RleDictionary, p.Encoding == ParquetEncoding.RleDictionary ? (int)p.Body[0] : 0))]);

        // The string column: every page dictionary-encoded, the page of nulls too.
        var (_, stringPages) = Pages(bytes, column: 1);
        Assert.All(stringPages, p => Assert.Equal(ParquetEncoding.RleDictionary, p.Encoding));
        Assert.Equal(Rows / PageRows, stringPages.Count);
    }

    [Fact]
    public void TheFile_IsTheSame_OnOneThreadOrMany() => Assert.Equal(Written(1), Written(16));

    /// <summary>
    /// The bytes a column writer holds at most, without waiting (<see cref="ColumnWriter.BufferedBytesBound"/>, which
    /// row groups closing by size are first judged on), bound the exact ones while pages wait to be laid out: those
    /// pages' values may still enter the dictionary. Here no helper is free (the budget is taken), so pages wait; after
    /// a first page of few values each page brings new ones, uncompressed.
    /// </summary>
    [Fact]
    public void TheBound_HoldsThePagesWaitingToBeLaidOut()
    {
        var budget = new WorkBudget(4);
        for (int i = 0; i < budget.Max; i++) budget.Enter();
        var settings = new PageSettings(CompressionCodec.Uncompressed, 0, ParquetPageVersion.V1, PageBytes: 1 << 20, PageRows: 50, Dictionary: true,
            DictionaryBytes: 1 << 20, Statistics: true, TruncateLength: 64, Checksums: true, PageIndex: true, SizeStatistics: true, Budget: budget);
        var strings = new StringArray.Builder();
        for (int row = 0; row < 3000; row++) strings.Append(row < 50 ? $"few {row % 5}" : $"new value {row:D8} {new string('x', 20)}");
        using StringArray values = strings.Build();
        using var writer = new BytesColumnWriter(settings, 0, 0, null!, 0, BytesOrder.Unsigned, utf8: true, "s", direct: true);
        int checks = 0;
        for (int offset = 0; offset < values.Length; offset += 37)
        {
            writer.Write(values, offset, Math.Min(37, values.Length - offset));
            long bound = writer.BufferedBytesBound, exact = writer.BufferedBytes;
            Assert.True(bound >= exact, $"after {offset} rows: bound {bound} < {exact}");
            checks++;
        }
        Assert.True(checks > 50);
    }

    /// <summary>A column chunk's dictionary page body and its data pages (encoding, body; uncompressed, format 1, the values after any definition levels).</summary>
    private static (byte[] Dictionary, List<(ParquetEncoding Encoding, byte[] Body)> Pages) Pages(byte[] bytes, int column)
    {
        using var file = ParquetFile.Open(new MemoryStream(bytes));
        ColumnMetaData meta = file.Metadata.Thrift!.RowGroups[0].Columns[column].MetaData!;
        long at = meta.DictionaryPageOffset ?? meta.DataPageOffset, end = at + meta.TotalCompressedSize;
        byte[] dictionary = [];
        List<(ParquetEncoding, byte[])> pages = [];
        while (at < end)
        {
            PageHeader header = PageHeader.Parse(bytes.AsSpan((int)at), out int length);
            byte[] body = bytes.AsSpan((int)at + length, header.CompressedPageSize).ToArray();
            if (header.DictionaryPageHeader is not null) dictionary = body;
            else
            {
                // Definition levels first (a 4-byte length, then the levels) for an optional column.
                int skip = column == 1 ? 4 + BinaryPrimitives.ReadInt32LittleEndian(body) : 0;
                pages.Add((header.DataPageHeader!.Encoding, body[skip..]));
            }
            at += length + header.CompressedPageSize;
        }
        return (dictionary, pages);
    }
}
