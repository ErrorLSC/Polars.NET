using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SeriesBinaryOpsTests
{
    [Fact]
    [Trait("Series", "Binary")]
    public void Test_Series_Bin_Size()
    {
        byte[][] data = [
            [0xFF, 0x00, 0xFF], 
            [],                
            new byte[1024],     
            new byte[2048],   
            null
        ];

        using Series s = Pl.CreateSeries("bin_data", data);
        using Series sizeBytes = s.Cast(DataType.Binary).Bin.Size();
        Assert.Equal([3u, 0u, 1024u, 2048u, null], sizeBytes.ToArray<uint?>());

        using Series sizeKb = s.Cast(DataType.Binary).Bin.Size(SizeUnit.Kilobytes);
        Assert.Equal([3.0 / 1024.0, 0.0, 1.0, 2.0, null], sizeKb.ToArray<double?>());
        
        using Series sizeMb = s.Cast(DataType.Binary).Bin.Size(SizeUnit.Megabytes);
        Assert.Equal([3.0 / 1048576.0, 0.0, 1024.0 / 1048576.0, 2048.0 / 1048576.0, null], sizeMb.ToArray<double?>());
    }
    [Fact]
    [Trait("Series", "BinaryContains")]
    public void Test_Series_Bin_Contains_Starts_Ends()
    {
        byte[][] data = [
            [0x00, 0x01, 0x02, 0x03], 
            [0xFF, 0x00, 0xFF],      
            [],                       
            null,                    
            [0x01, 0x02]             
        ];

        using Series sList = Pl.CreateSeries("bin_data", data);
        using Series s = sList.Cast(DataType.Binary);

        using Series contains = s.Bin.Contains(new byte[] { 0x01, 0x02 });
        // [0x00, 0x01, 0x02, 0x03] -> true
        // [0xFF, 0x00, 0xFF]       -> false
        // []                       -> false
        // null                     -> null
        // [0x01, 0x02]             -> true
        Assert.Equal([true, false, false, null, true], contains.ToArray<bool?>());

        using Series startsWith = s.Bin.StartsWith(new byte[] { 0xFF, 0x00 });
        Assert.Equal([false, true, false, null, false], startsWith.ToArray<bool?>());

        using Series endsWith = s.Bin.EndsWith(new byte[] { 0x03 });
        Assert.Equal([true, false, false, null, false], endsWith.ToArray<bool?>());
        
        using Series prefixSeries = s.Bin.Slice(0, 2); 
        using Series containsSelf = s.Bin.Contains(prefixSeries);

        Assert.Equal([true, true, true, null, true], containsSelf.ToArray<bool?>());
    }
    [Fact]
    [Trait("Series", "BinaryHead")]
    public void Test_Series_Bin_Head_Tail()
    {
        byte[][] data = [
            [0x01, 0x02, 0x03, 0x04, 0x05, 0x06], 
            [0xFF, 0xEE],                         
            [],                                   
            null                                  
        ];

        using Series sList = Pl.CreateSeries("bin_data", data);
        using Series s = sList.Cast(DataType.Binary);

        using Series head3 = s.Bin.Head(3);
        byte[][] expectedHead = [
            [0x01, 0x02, 0x03], 
            [0xFF, 0xEE],       
            [],                 
            null                
        ];
        
        using Series expectedHeadSeries = Pl.CreateSeries("expected_head", expectedHead).Cast(DataType.Binary);

        using Series headMask = head3.Eq(expectedHeadSeries);
        
        Assert.Equal([true, true, true, null], headMask.ToArray<bool?>());

        using Series tail3 = s.Bin.Tail(3);
        byte[][] expectedTail = [
            [0x04, 0x05, 0x06], 
            [0xFF, 0xEE],       
            [],                 
            null                
        ];
        using Series expectedTailSeries = Pl.CreateSeries("expected_tail", expectedTail).Cast(DataType.Binary);
        using Series tailMask = tail3.Eq(expectedTailSeries);
        
        Assert.Equal([true, true, true, null], tailMask.ToArray<bool?>());
    }
    [Fact]
    [Trait("Series", "BinaryReinterpret")]
    public void Test_Series_Bin_Reinterpret()
    {
        byte[][] data = [
            [0x01, 0x00, 0x00, 0x00],
            [0x00, 0x01, 0x00, 0x00], 
            [0xFF, 0xFF, 0xFF, 0xFF], 
            null                      
        ];

        using Series sList = Pl.CreateSeries("bin_data", data);
        using Series s = sList.Cast(DataType.Binary);

        using Series littleInt32 = s.Bin.Reinterpret(typeof(int));
        
        Assert.Equal(DataType.Int32, littleInt32.DataType);
        // 0x00000001 = 1
        // 0x00000100 = 256
        Assert.Equal([1, 256, -1, null], littleInt32.ToArray<int?>());


        using Series bigInt32 = s.Bin.Reinterpret(typeof(int), Endianness.Big);
        
        Assert.Equal(DataType.Int32, bigInt32.DataType);
        // 0x01000000 = 16777216
        // 0x00010000 = 65536
        Assert.Equal([16777216, 65536, -1, null], bigInt32.ToArray<int?>());

        using Series littleUInt32 = s.Bin.Reinterpret(typeof(uint)); // 默认 Little
        
        Assert.Equal(DataType.UInt32, littleUInt32.DataType);
        Assert.Equal([1u, 256u, uint.MaxValue, null], littleUInt32.ToArray<uint?>());
    }
}