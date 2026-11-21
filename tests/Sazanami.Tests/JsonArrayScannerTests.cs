using System.Text;
using System.Text.Json;
using Kaminari.Sazanami.Json;

namespace Sazanami.Tests;

/// <summary>
/// The commas between a top-level array's elements, found by <see cref="JsonArrayScanner"/>: every one of them (against
/// System.Text.Json's reading of the same document), the vectorized and the byte-at-a-time scans alike, whatever the
/// pieces the bytes come in.
/// </summary>
public class JsonArrayScannerTests
{
    // The scan of a document fed in pieces of the given sizes (each call sees the bytes since its last stop, as a stream
    // reader's buffer holds them), cutting wherever it can past cutEvery bytes from the last cut: the cuts, the commas
    // counted, and whether the scan finished.
    private static (List<int> Cuts, long Commas, bool Done) Scan(byte[] document, bool scalar, int piece, int cutEvery)
    {
        var scanner = new JsonArrayScanner { Scalar = scalar };
        List<int> cuts = [];
        long commas = 0;
        int start = 0, position = 0, available = 0;
        while (true)
        {
            available = Math.Min(document.Length, available + piece);
            bool final = available == document.Length;
            ReadOnlySpan<byte> data = document.AsSpan(start, available - start);
            int cut;
            while ((cut = scanner.Scan(data, ref position, cutEvery, final, start == 0, ref commas)) >= 0)
            {
                cuts.Add(start + cut);
                start += cut + 1;
                position -= cut + 1;
                data = document.AsSpan(start, available - start);
            }
            if (final) return (cuts, commas, scanner.Done);
        }
    }

    // The top-level commas as System.Text.Json reads the document.
    private static List<int> Reference(byte[] document)
    {
        List<int> commas = [];
        var reader = new Utf8JsonReader(document);
        reader.Read();
        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
        {
            reader.Skip();
            int at = (int)reader.BytesConsumed;
            while (document[at] is (byte)' ' or (byte)'\n' or (byte)'\r' or (byte)'\t') at++;
            if (document[at] == ',') commas.Add(at);
        }
        return commas;
    }

    private static string RandomValue(Random random, int depth)
    {
        string[] strings = ["\"\"", "\"]\"", "\"[{\"", "\"a,b\"", "\"\\\"\"", "\"\\\\\"", "\"\\\\\\\"]\"", "\"\\\\\\\\\"", "\"x\\\\\\\\\\\"y\"", "\"é😀\"", "\"\\u005b\""];
        return (depth > 4 ? random.Next(3) : random.Next(6)) switch
        {
            0 => strings[random.Next(strings.Length)],
            1 => random.Next(3) switch { 0 => "null", 1 => "true", _ => random.Next(-1000, 1000).ToString() },
            2 => "\"" + new string('w', random.Next(200)) + new string('\\', 2 * random.Next(3)) + "\"",
            3 => "[" + string.Join(", ", Enumerable.Range(0, random.Next(4)).Select(_ => RandomValue(random, depth + 1))) + "]",
            _ => "{" + string.Join(",\n ", Enumerable.Range(0, random.Next(5)).Select(i => $"\"k{i}\\\\\":" + RandomValue(random, depth + 1))) + "}",
        };
    }

    private static byte[] RandomDocument(Random random, int elements) =>
        Encoding.UTF8.GetBytes(" \n[" + string.Join(random.Next(2) == 0 ? "," : " ,\r\n ", Enumerable.Range(0, elements).Select(_ => RandomValue(random, 0))) + "]\n");

    [Fact]
    public void EveryTopLevelComma_IsFound_InAnyPieces()
    {
        var random = new Random(7);
        for (int n = 0; n < 200; n++)
        {
            byte[] document = RandomDocument(random, random.Next(0, 40));
            List<int> expected = Reference(document);
            foreach (bool scalar in (bool[])[false, true])
                foreach (int piece in (int[])[1, 3, 63, 64, 65, 1000, int.MaxValue / 2])
                {
                    var (cuts, commas, done) = Scan(document, scalar, piece, 0);
                    Assert.Equal(expected, cuts);
                    Assert.Equal(expected.Count, commas);
                    Assert.True(done);
                }
        }
    }

    [Fact]
    public void CutsComeAfterCutFrom_AtTheFirstCommaPastIt()
    {
        var random = new Random(11);
        for (int n = 0; n < 50; n++)
        {
            byte[] document = RandomDocument(random, 60);
            List<int> all = Reference(document);
            foreach (int every in (int[])[10, 100, 1000])
            {
                List<int> expected = [];
                int start = 0;
                foreach (int comma in all)
                    if (comma - start >= every)
                    {
                        expected.Add(comma);
                        start = comma + 1;
                    }
                foreach (bool scalar in (bool[])[false, true])
                    Assert.Equal(expected, Scan(document, scalar, 77, every).Cuts);
            }
        }
    }

    [Fact]
    public void AnythingElse_ScansAlike_VectorAndScalar()
    {
        // Broken or not-array input: no promise but the two scans agreeing (the parser reports what is wrong).
        var random = new Random(13);
        byte[] alphabet = "[]{},\"\\ ax\n"u8.ToArray();
        for (int n = 0; n < 2000; n++)
        {
            byte[] bytes = [.. "["u8, .. Enumerable.Range(0, random.Next(300)).Select(_ => alphabet[random.Next(alphabet.Length)])];
            foreach (int piece in (int[])[1, 64, 100, int.MaxValue / 2])
            {
                var (cuts, commas, done) = Scan(bytes, true, piece, 0);
                var vector = Scan(bytes, false, piece, 0);
                Assert.Equal(cuts, vector.Cuts);
                Assert.Equal(commas, vector.Commas);
                Assert.Equal(done, vector.Done);
            }
        }
    }

    // The first top-level comma of each segment after the first, found in parallel: segment summaries taken in order
    // give each segment's start state, from which a scan resumes inside the segment only.
    private static List<int> ParallelCuts(byte[] document, int segment)
    {
        int open = document.AsSpan().IndexOfAnyExcept(" \t\r\n"u8);
        if (open < 0 || document[open] != '[') return [];
        long elements = 0;
        List<int> cuts = [];
        long depth = 0;
        bool inString = false;
        for (int start = segment; start < document.Length; start += segment)
        {
            int previous = start - segment;
            var (odd, outside, inside) = JsonArrayScanner.Summarize(document.AsSpan(previous, segment), JsonArrayScanner.EscapedAfter(document.AsSpan(0, previous)));
            depth += inString ? inside : outside;
            inString ^= odd;
            if (start <= open) continue; // the array opens later
            if (depth < 1) break;
            var scanner = JsonArrayScanner.Resume(depth, inString, JsonArrayScanner.EscapedAfter(document.AsSpan(0, start)));
            int at = 0;
            int cut = scanner.Scan(document.AsSpan(start, Math.Min(segment, document.Length - start)), ref at, 0, true, false, ref elements);
            if (cut >= 0) cuts.Add(start + cut);
            else if (scanner.Done) break;
        }
        return cuts;
    }

    [Fact]
    public void ParallelCuts_AreEachSegmentsFirstTopLevelComma()
    {
        var random = new Random(19);
        for (int n = 0; n < 300; n++)
        {
            byte[] document = RandomDocument(random, random.Next(0, 30));
            List<int> all = Reference(document);
            foreach (int segment in (int[])[1, 2, 5, 63, 64, 65, 300, 5000])
            {
                List<int> expected = [];
                for (int start = segment; start < document.Length; start += segment)
                    if (all.FirstOrDefault(c => c >= start, -1) is var c && c >= 0 && c < start + segment) expected.Add(c);
                Assert.Equal(expected, ParallelCuts(document, segment));
            }
        }
    }

    [Fact]
    public void EscapedAfter_CountsTheTrailingBackslashes()
    {
        Assert.False(JsonArrayScanner.EscapedAfter(""u8));
        Assert.True(JsonArrayScanner.EscapedAfter("\\"u8));
        Assert.False(JsonArrayScanner.EscapedAfter("\\\\"u8));
        Assert.True(JsonArrayScanner.EscapedAfter("a\\\\\\"u8));
        Assert.False(JsonArrayScanner.EscapedAfter("\\a"u8));
    }

    [Theory]
    [InlineData("{\"a\":[1,2]}")]
    [InlineData("5")]
    [InlineData("\uFEFF {\"a\":1}")]
    public void ADocumentNotAnArray_HasNoCuts(string text)
    {
        foreach (bool scalar in (bool[])[false, true])
        {
            var (cuts, _, done) = Scan(Encoding.UTF8.GetBytes(text), scalar, 1, 0);
            Assert.Empty(cuts);
            Assert.True(done);
        }
    }

    [Fact]
    public void AByteOrderMark_IsPassedOver_NestedCommasAreNotCuts()
    {
        foreach (bool scalar in (bool[])[false, true])
        {
            foreach (int piece in (int[])[1, 2, 100])
                Assert.Equal([5, 7], Scan([0xEF, 0xBB, 0xBF, .. "[1,2,3]"u8], scalar, piece, 0).Cuts);
            Assert.Equal([2, 8], Scan("[1,[2,3],4]"u8.ToArray(), scalar, 3, 0).Cuts);
        }
    }
}
