using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>The compact protocol reader on hostile input: counts and lengths are checked before anything is allocated.</summary>
public class ThriftCompactTests
{
    /// <summary>Fails unless <paramref name="action"/> throws ParquetFormatException having allocated little.</summary>
    private static ParquetFormatException ThrowsCheaply(Action action)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        var e = Assert.Throws<ParquetFormatException>(action);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(allocated < 1 << 20, $"{allocated} bytes allocated before failing");
        return e;
    }

    [Theory]
    [InlineData("F5FFFFFFFF07")]       // 2^31 - 1 i32 elements
    [InlineData("F580808040")]             // 2^27 i32 elements
    [InlineData("F5FFFFFFFFFF01")] // beyond int
    public void HugeListCount_FailsBeforeAllocating(string hex)
    {
        byte[] header = Convert.FromHexString(hex);
        ThrowsCheaply(() =>
        {
            var r = new ThriftCompactReader(header);
            r.ReadI32List();
        });
        ThrowsCheaply(() =>
        {
            var r = new ThriftCompactReader([(byte)((header[0] & 0xF0) | ThriftType.I64), .. header[1..]]);
            r.ReadI64List();
        });
        ThrowsCheaply(() =>
        {
            var r = new ThriftCompactReader([(byte)((header[0] & 0xF0) | ThriftType.Binary), .. header[1..]]);
            r.ReadStringList();
        });
    }

    [Fact]
    public void HugeStructList_InAFooter_FailsBeforeAllocating()
    {
        // FileMetaData { 1: version = 1, 2: schema = list<struct> of 2^27 elements }
        byte[] footer = [0x15, 0x02, 0x19, 0xFC, 0x80, 0x80, 0x80, 0x40, 0x00];
        Assert.Contains("truncated", ThrowsCheaply(() => FileMetaData.Parse(footer)).Message);
    }

    [Theory]
    [InlineData("FFFFFFFF0F")]             // 2^32 - 1 bytes
    [InlineData("808080406162")]       // 2^27 bytes, two present
    [InlineData("FFFFFFFFFFFF01")] // beyond int
    public void HugeBinaryLength_FailsBeforeAllocating(string hex) =>
        ThrowsCheaply(() =>
        {
            var r = new ThriftCompactReader(Convert.FromHexString(hex));
            r.ReadBinary();
        });

    [Fact]
    public void DeepNesting_IsRefused()
    {
        // 100 nested struct fields (id 1), never closed: refused at depth 64, not by a stack overflow.
        byte[] nested = [.. Enumerable.Repeat((byte)0x1C, 100), .. Enumerable.Repeat((byte)0x00, 101)];
        Assert.Contains("nests too deeply", Assert.Throws<ParquetFormatException>(() => FileMetaData.Parse(nested)).Message);
    }

    [Fact]
    public void OverlongVarint_AndUnknownType_AreRefused()
    {
        Assert.Contains("too long", Assert.Throws<ParquetFormatException>(() =>
        {
            var r = new ThriftCompactReader([.. Enumerable.Repeat((byte)0xFF, 11), 0x01]);
            r.ReadI64();
        }).Message);
        Assert.Contains("Unknown Thrift compact type 13", Assert.Throws<ParquetFormatException>(() => FileMetaData.Parse([0x1D, 0x00])).Message);
    }

    [Fact]
    public void LongFormFieldHeaders_AndBoolsInBothForms_RoundTrip()
    {
        var w = new ThriftCompactWriter();
        w.BeginStruct();
        w.I32Field(1, 7);
        w.BoolField(20, true);    // delta 19: long form
        w.I64Field(32767, -1);    // the extension id
        w.BeginListField(32766, ThriftType.BoolTrue, 3); // a smaller id after a larger one: long form
        w.WriteBool(true);
        w.WriteBool(false);
        w.WriteBool(true);
        w.EndStruct();

        var r = new ThriftCompactReader(w.ToArray());
        r.EnterStruct();
        short last = 0;
        List<string> seen = [];
        while (r.NextField(ref last, out short id, out byte type))
        {
            seen.Add(id switch
            {
                1 => $"1={r.ReadI32()}",
                20 when ThriftCompactReader.IsBool(type) => $"20={r.BoolField()}",
                32767 => $"32767={r.ReadI64()}",
                32766 => $"32766={string.Join(',', r.ReadBoolList())}",
                _ => $"?{id}",
            });
        }
        r.ExitStruct();
        Assert.Equal(["1=7", "20=True", "32767=-1", "32766=True,False,True"], seen);
        Assert.Equal(0, r.Remaining);
    }

    [Fact]
    public void UnknownFields_AndMismatchedTypes_AreSkipped()
    {
        // KeyValue { 1: key = "k", 3: map<i32,string>{1:"x"} (unknown), then 2: value as an i32 (wrong type, long-form header), 32767: binary }
        byte[] bytes = [0x18, 0x01, 0x6B, 0x2B, 0x01, 0x58, 0x02, 0x01, 0x78, 0x05, 0x04, 0x04, 0x08, 0xFE, 0xFF, 0x03, 0x02, 0x61, 0x62, 0x00];
        var r = new ThriftCompactReader(bytes);
        KeyValue kv = KeyValue.Read(ref r);
        Assert.Equal(new KeyValue("k", null), kv);
        Assert.Equal(0, r.Remaining);
    }
}
