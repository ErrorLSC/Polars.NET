using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SeriesListOpsTests
{
    [Fact]
    [Trait("Series", "ListGet")]
    public void Test_Series_List_Get()
    {
        // List Series
        // Row 0: [1, 2, 3]
        // Row 1: [4, 5]
        // Row 2: [6, 7, 8, 9]
        int[][] data = [
            [1, 2, 3],
            [4, 5],
            [6, 7, 8, 9]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        using Series get1 = listSeries.List.Get(1);
        Assert.Equal("list_col", get1.Name);
        Assert.Equal([2, 5, 7], get1.ToArray<int>());

        using Series getMinus1 = listSeries.List.Get(-1);
        Assert.Equal([3, 5, 9], getMinus1.ToArray<int>());

        using Series getOob = listSeries.List.Get(3, nullOnOob: true);
        int?[] expectedOob = [null, null, 9];
        Assert.Equal(expectedOob, getOob.ToArray<int?>());
    }

    [Fact]
    [Trait("Series", "ListGather")]
    public void Test_Series_List_Gather_IntoExpr()
    {
        // Row 0: [10, 20, 30]
        // Row 1: [40, 50, 60]
        // Row 2: [70, 80, 90]
        int[][] data = [
            [10, 20, 30],
            [40, 50, 60],
            [70, 80, 90]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // [[10, 30], [40, 60], [70, 90]]
        using Series gathered = listSeries.List.Gather(Pl.Lit([0, 2]));

        Assert.Equal(DataType.List(DataType.Int32), gathered.DataType);
        
        using Series get0 = gathered.List.Get(0);
        using Series get1 = gathered.List.Get(1);

        Assert.Equal([10, 40, 70], get0.ToArray<int>());
        Assert.Equal([30, 60, 90], get1.ToArray<int>());
    }

    [Fact]
    [Trait("Series", "ListGather")]
    public void Test_Series_List_Gather_ReadOnlySpan()
    {
        // Row 0: [1, 2, 3, 4]
        // Row 1: [5, 6, 7, 8]
        int[][] data = [
            [1, 2, 3, 4],
            [5, 6, 7, 8]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // [[4, 2], [8, 6]]
        using Series gathered = listSeries.List.Gather([3, 1]); 

        using Series get0 = gathered.List.Get(0);
        using Series get1 = gathered.List.Get(1);

        Assert.Equal([4, 8], get0.ToArray<int>());
        Assert.Equal([2, 6], get1.ToArray<int>());
    }
    [Fact]
    [Trait("Series", "ListGatherEvery")]
    public void Test_Series_List_GatherEvery()
    {
        // Row 0: [1, 2, 3, 4, 5]
        // Row 1: [10, 20, 30, 40, 50]
        int[][] data = [
            [1, 2, 3, 4, 5],
            [10, 20, 30, 40, 50]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // GatherEvery(n = 2) -> offset 为 0
        // [[1, 3, 5], [10, 30, 50]]
        using Series g1 = listSeries.List.GatherEvery(2);
        
        using Series g1_0 = g1.List.Get(0);
        using Series g1_1 = g1.List.Get(1);
        using Series g1_2 = g1.List.Get(2);

        Assert.Equal([1, 10], g1_0.ToArray<int>());
        Assert.Equal([3, 30], g1_1.ToArray<int>());
        Assert.Equal([5, 50], g1_2.ToArray<int>());

        // GatherEvery(n = 2, offset = 1)
        // [[2, 4], [20, 40]]
        using Series g2 = listSeries.List.GatherEvery(2, 1);
        
        using Series g2_0 = g2.List.Get(0);
        using Series g2_1 = g2.List.Get(1);

        Assert.Equal([2, 20], g2_0.ToArray<int>());
        Assert.Equal([4, 40], g2_1.ToArray<int>());
    }

    [Fact]
    [Trait("Series", "ListSlice")]
    public void Test_Series_List_Slice()
    {
        // Row 0: [1, 2, 3, 4]
        // Row 1: [5, 6, 7, 8]
        int[][] data = [
            [1, 2, 3, 4],
            [5, 6, 7, 8]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // Slice(offset = 1, length = 2)
        // -> [[2, 3], [6, 7]]
        using Series slice1 = listSeries.List.Slice(1, 2);
        
        using Series slice1_0 = slice1.List.Get(0);
        using Series slice1_1 = slice1.List.Get(1);
        Assert.Equal([2, 6], slice1_0.ToArray<int>());
        Assert.Equal([3, 7], slice1_1.ToArray<int>());

        // Slice(offset = 2, length = null) 
        //  -> [[3, 4], [7, 8]]
        using Series slice2 = listSeries.List.Slice(2); 
        
        using Series slice2_0 = slice2.List.Get(0);
        using Series slice2_1 = slice2.List.Get(1);
        Assert.Equal([3, 7], slice2_0.ToArray<int>());
        Assert.Equal([4, 8], slice2_1.ToArray<int>());
    }

    [Fact]
    [Trait("Series", "ListHead")]
    public void Test_Series_List_Head()
    {
        // Row 0: [1, 2, 3, 4, 5, 6]
        // Row 1: [10, 20]
        int[][] data = [
            [1, 2, 3, 4, 5, 6],
            [10, 20]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // Head(n = 2)
        // -> [[1, 2], [10, 20]]
        using Series head2 = listSeries.List.Head(2);
        
        using Series head2_0 = head2.List.Get(0);
        using Series head2_1 = head2.List.Get(1);
        Assert.Equal([1, 10], head2_0.ToArray<int>());
        Assert.Equal([2, 20], head2_1.ToArray<int>());

        // 2. Head() 
        // [[1, 2, 3, 4, 5], [10, 20]]
        using Series headDefault = listSeries.List.Head();
        
        using Series hd_last = headDefault.List.Last(); 
        Assert.Equal([5, 20], hd_last.ToArray<int>());  
    }

    [Fact]
    [Trait("Series", "ListTail")]
    public void Test_Series_List_Tail()
    {
        // Row 0: [1, 2, 3, 4]
        // Row 1: [10, 20, 30]
        int[][] data = [
            [1, 2, 3, 4],
            [10, 20, 30]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // Tail(n = 2)
        // -> [[3, 4], [20, 30]]
        using Series tail2 = listSeries.List.Tail(2);
        
        using Series tail2_0 = tail2.List.Get(0);
        using Series tail2_1 = tail2.List.Get(1);
        Assert.Equal([3, 20], tail2_0.ToArray<int>());
        Assert.Equal([4, 30], tail2_1.ToArray<int>());

        // Tail() 
        //  -> [[1, 2, 3, 4], [10, 20, 30]]
        using Series tailDefault = listSeries.List.Tail();
        
        using Series td_0 = tailDefault.List.First(); 
        Assert.Equal([1, 10], td_0.ToArray<int>());
    }
    [Fact]
    [Trait("Series", "ListShift")]
    public void Test_Series_List_Shift()
    {
        // Row 0: [1, 2, 3, 4]
        // Row 1: [10, 20]
        int[][] data = [
            [1, 2, 3, 4],
            [10, 20]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // Shift() -> Shift(1) -> 
        // [[null, 1, 2, 3], [null, 10]]
        using Series shift1 = listSeries.List.Shift();

        using Series s1_0 = shift1.List.Get(0);
        using Series s1_1 = shift1.List.Get(1);
        using Series s1_2 = shift1.List.Get(2,true);

        Assert.Equal([null, null], s1_0.ToArray<int?>()); 
        Assert.Equal([1, 10], s1_1.ToArray<int?>());      
        Assert.Equal([2, null], s1_2.ToArray<int?>());    

        // 2. Shift(-1) -> 
        // [[2, 3, 4, null], [20, null]]
        using Series shiftMinus1 = listSeries.List.Shift(-1); 
        
        using Series sm1_0 = shiftMinus1.List.Get(0);
        using Series sm1_last = shiftMinus1.List.Get(-1);

        Assert.Equal([2, 20], sm1_0.ToArray<int?>());
        Assert.Equal([null, null], sm1_last.ToArray<int?>()); 
    }

    [Fact]
    [Trait("Series", "ListDiff")]
    public void Test_Series_List_Diff()
    {
        // Row 0: [1, 3, 6, 10]   
        // Row 1: [5, 10, 20, 40]  
        int[][] data = [
            [1, 3, 6, 10],
            [5, 10, 20, 40]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // Diff() -> n = 1 -> 
        // [[null, 2, 3, 4], [null, 5, 10, 20]]
        using Series diff1 = listSeries.List.Diff();
        
        using Series d1_0 = diff1.List.Get(0);
        using Series d1_1 = diff1.List.Get(1);
        using Series d1_2 = diff1.List.Get(2);

        Assert.Equal([null, null], d1_0.ToArray<int?>()); 
        Assert.Equal([2, 5], d1_1.ToArray<int?>());      
        Assert.Equal([3, 10], d1_2.ToArray<int?>());     

        // 2. Diff(n = 2) ->  lag = 2
        // [[null, null, 5, 7], [null, null, 15, 30]]
        // 5 (6-1), 7 (10-3) ; 15 (20-5), 30 (40-10)
        using Series diff2 = listSeries.List.Diff(2);
        
        using Series d2_1 = diff2.List.Get(1);
        using Series d2_2 = diff2.List.Get(2);
        using Series d2_3 = diff2.List.Get(3);

        Assert.Equal([null, null], d2_1.ToArray<int?>());
        Assert.Equal([5, 15], d2_2.ToArray<int?>());
        Assert.Equal([7, 30], d2_3.ToArray<int?>());
    }
    [Fact]
    [Trait("Series", "ListSampleN")]
    public void Test_Series_List_SampleN()
    {
        // Row 0: [1, 2, 3, 4, 5]
        // Row 1: [10, 20, 30]
        int[][] data = [
            [1, 2, 3, 4, 5],
            [10, 20, 30]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // SampleN(2) ->
        using Series sampled2 = listSeries.List.SampleN(2, seed: 42ul);
        
        using Series s2_0 = sampled2.List.Get(0);
        using Series s2_1 = sampled2.List.Get(1);
        using Series s2_2 = sampled2.List.Get(2, nullOnOob: true); 

        Assert.NotNull(s2_0.ToArray<int?>()[0]);
        Assert.NotNull(s2_1.ToArray<int?>()[0]);

        Assert.Equal([null, null], s2_2.ToArray<int?>());

        // SampleN() -> SampleN(1) 
        using Series sampled1 = listSeries.List.SampleN(); 
        
        using Series s1_0 = sampled1.List.Get(0);
        using Series s1_1 = sampled1.List.Get(1, nullOnOob: true);
        
        Assert.NotNull(s1_0.ToArray<int?>()[0]);
        Assert.Equal([null, null], s1_1.ToArray<int?>());

        // (withReplacement = true)
        using Series sampled5 = listSeries.List.SampleN(5, withReplacement: true, seed: 100ul);
        using Series s5_4 = sampled5.List.Get(4); 
        Assert.NotNull(s5_4.ToArray<int?>()[0]);  
    }

    [Fact]
    [Trait("Series", "ListSampleFrac")]
    public void Test_Series_List_SampleFrac()
    {
        // Row 0: [1, 2, 3, 4]       (length 4)
        // Row 1: [10, 20, 30, 40]   (length 4)
        int[][] data = [
            [1, 2, 3, 4],
            [10, 20, 30, 40]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // SampleFrac(0.5) -> 4 * 0.5 = 2 elements
        using Series sampledHalf = listSeries.List.SampleFrac(0.5, seed: 42ul);

        using Series sh_0 = sampledHalf.List.Get(0);
        using Series sh_1 = sampledHalf.List.Get(1);
        using Series sh_2 = sampledHalf.List.Get(2, nullOnOob: true); 

        Assert.NotNull(sh_0.ToArray<int?>()[0]);
        Assert.NotNull(sh_1.ToArray<int?>()[0]);
        Assert.Equal([null, null], sh_2.ToArray<int?>());

        // SampleFrac(1.0) 
        using Series sampledAll = listSeries.List.SampleFrac(1.0, seed: 99ul);
        using Series sa_3 = sampledAll.List.Get(3);
        Assert.NotNull(sa_3.ToArray<int?>()[0]); 
    }
    [Fact]
    [Trait("Series", "ListSetUnion")]
    public void Test_Series_List_SetUnion()
    {
        int[][] data = [
            [1, 2, 3],
            [4, 5]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // [1, 2, 3] U [2, 3, 4] => [1, 2, 3, 4]
        // [4, 5] U [2, 3, 4] => [4, 5, 2, 3] 
        using Series union = listSeries.List.SetUnion([2, 3, 4]);
        union.Show();
        using Series u_0 = union.List.Get(0);
        using Series u_1 = union.List.Get(1);

        Assert.Equal([1, 4], u_0.ToArray<int>());
        Assert.Equal([2, 5], u_1.ToArray<int>()); 
    }

    [Fact]
    [Trait("Series", "ListSetDifference")]
    public void Test_Series_List_SetDifference()
    {
        int[][] data = [
            [1, 2, 3],
            [4, 5]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // [1, 2, 3] - [2, 3, 4] => [1]
        // [4, 5] - [2, 3, 4] => [5]
        using Series diff = listSeries.List.SetDifference([2, 3, 4]);

        using Series d_0 = diff.List.Get(0);
        Assert.Equal([1, 5], d_0.ToArray<int>());
    }

    [Fact]
    [Trait("Series", "ListSetIntersection")]
    public void Test_Series_List_SetIntersection()
    {
        int[][] data = [
            [1, 2, 3],
            [4, 5]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // [1, 2, 3] & [2, 3, 4] => [2, 3]
        // [4, 5] & [2, 3, 4] => [4]
        using Series intersection = listSeries.List.SetIntersection([2, 3, 4]);

        using Series i_0 = intersection.List.Get(0);
        using Series i_1 = intersection.List.Get(1, nullOnOob: true); 

        Assert.Equal([2, 4], i_0.ToArray<int>());
        Assert.Equal([3, null], i_1.ToArray<int?>());
    }

    [Fact]
    [Trait("Series", "ListSetSymmetricDifference")]
    public void Test_Series_List_SetSymmetricDifference()
    {
        int[][] data = [
            [1, 2, 3],
            [4, 5]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // [1, 2, 3] ^ [2, 3, 4] => [1, 4]
        // [4, 5] ^ [2, 3, 4] => [5, 2, 3]
        using Series symDiff = listSeries.List.SetSymmetricDifference([2, 3, 4]);

        using Series sd_0 = symDiff.List.Get(0);
        using Series sd_1 = symDiff.List.Get(1);
        using Series sd_2 = symDiff.List.Get(2, nullOnOob: true);

        Assert.Equal([1, 5], sd_0.ToArray<int>());
        Assert.Equal([4, 2], sd_1.ToArray<int>());
        Assert.Equal([null, 3], sd_2.ToArray<int?>());
    }
    [Fact]
    [Trait("Series", "ListSetOperations_Columns")]
    public void Test_Series_List_SetOperations_Between_Columns()
    {
        int[][] dataA = [
            [1, 2],
            [3, 4]
        ];
        int[][] dataB = [
            [2, 3],
            [4, 5]
        ];

        using Series seriesA = Pl.Series("list_A", dataA);
        using Series seriesB = Pl.Series("list_B", dataB);

        using Series symDiff = seriesA.List.SetSymmetricDifference(seriesB);

        // Row 0: [1, 2] ^ [2, 3] => [1, 3]
        // Row 1: [3, 4] ^ [4, 5] => [3, 5]
        
        using Series sd_0 = symDiff.List.Get(0);
        using Series sd_1 = symDiff.List.Get(1);

        Assert.Equal([1, 3], sd_0.ToArray<int>()); 
        Assert.Equal([3, 5], sd_1.ToArray<int>()); 
    }
    [Fact]
    [Trait("Series", "ListStringAndLength")]
    public void Test_Series_List_Join_And_Len()
    {
        // 构造包含 null 的字符串交错数组
        string[][] data = [
            ["a", "b", "c"],
            ["x", null, "y"]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // 1. Len() -> 获取每个子列表的长度
        // Polars 中长度相关的返回类型通常是 uint32
        using Series lengths = listSeries.List.Len();
        Assert.Equal([3u, 3u], lengths.ToArray<uint>());

        // 2. Join (ignoreNulls = true) -> 默认忽略 null
        using Series joined = listSeries.List.Join("-", ignoreNulls: true);
        Assert.Equal(["a-b-c", "x-y"], joined.ToArray<string>());

        // 3. Join (ignoreNulls = false) -> 包含 null 时，整个合并结果可能直接变为 null
        using Series joinedWithNull = listSeries.List.Join("-", ignoreNulls: false);
        Assert.Equal(["a-b-c", null], joinedWithNull.ToArray<string>());
    }

    [Fact]
    [Trait("Series", "ListFirstLast")]
    public void Test_Series_List_First_Last()
    {
        int[][] data = [
            [1, 2, 3],
            [10] 
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // 1. First()
        using Series firsts = listSeries.List.First();
        Assert.Equal([1, 10], firsts.ToArray<int>());

        // 2. Last()
        using Series lasts = listSeries.List.Last();
        Assert.Equal([3, 10], lasts.ToArray<int>());
    }

    [Fact]
    [Trait("Series", "ListMathAggregations")]
    public void Test_Series_List_Math_Aggregations()
    {
        int[][] data = [
            [1, 2, 3],
            [4, 6]    
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // Sum() 
        using Series sum = listSeries.List.Sum();
        Assert.Equal([6, 10], sum.ToArray<int>());

        // Min() -> [1, 4]
        using Series min = listSeries.List.Min();
        Assert.Equal([1, 4], min.ToArray<int>());

        // Max() -> [3, 6]
        using Series max = listSeries.List.Max();
        Assert.Equal([3, 6], max.ToArray<int>());

        // Mean() -> [2.0,5.0]
        using Series mean = listSeries.List.Mean();
        Assert.Equal([2.0, 5.0], mean.ToArray<double>());
    }

    [Fact]
    [Trait("Series", "ListAgg")]
    public void Test_Series_List_Agg()
    {
        int[][] data = [
            [1, 2, 3],
            [4, 5, 6]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        using Series aggFirst = listSeries.List.Agg(Pl.Element().First());

        Assert.Equal([1, 4], aggFirst.ToArray<int>());
    }
    [Fact]
    [Trait("Series", "ListBooleanOps")]
    public void Test_Series_List_All_Any()
    {
        bool[][] data = [
            [true, true, true],
            [true, false, true],
            [false, false]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        using Series allSeries = listSeries.List.All();
        Assert.Equal([true, false, false], allSeries.ToArray<bool>());

        using Series anySeries = listSeries.List.Any();
        Assert.Equal([true, true, false], anySeries.ToArray<bool>());
    }

    [Fact]
    [Trait("Series", "ListDropNullsNUnique")]
    public void Test_Series_List_DropNulls_NUnique()
    {
        int?[][] data = [
            [1, null, 1, 2, null],
            [3, 3, 3]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // DropNulls()
        using Series dropNulls = listSeries.List.DropNulls();
        
        using Series dn_0 = dropNulls.List.Get(0);
        using Series dn_1 = dropNulls.List.Get(1);
        using Series dn_2 = dropNulls.List.Get(2);
        using Series dn_3 = dropNulls.List.Get(3, nullOnOob: true); 

        Assert.Equal([1, 3], dn_0.ToArray<int>());
        Assert.Equal([1, 3], dn_1.ToArray<int>());
        Assert.Equal([2, 3], dn_2.ToArray<int>());
        Assert.Equal([null, null], dn_3.ToArray<int?>()); 

        // NUnique()
        using Series nUnique = listSeries.List.NUnique();
        Assert.Equal([3u, 1u], nUnique.ToArray<uint>());
    }

    [Fact]
    [Trait("Series", "ListArgMinMax")]
    public void Test_Series_List_ArgMin_ArgMax()
    {
        int[][] data = [
            [10, 50, 30, 90], 
            [99, 1, 5, 2]    
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // ArgMax()
        using Series argMax = listSeries.List.ArgMax();
        Assert.Equal([3u, 0u], argMax.ToArray<uint>());

        // ArgMin()
        using Series argMin = listSeries.List.ArgMin();
        Assert.Equal([0u, 1u], argMin.ToArray<uint>());
    }

    [Fact]
    [Trait("Series", "ListStatistics")]
    public void Test_Series_List_Stats()
    {
        int[][] data = [
            [1, 2, 3, 4, 5],
            [10, 10, 10, 10]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        // 1. Median() -> [3.0, 10.0]
        using Series median = listSeries.List.Median();
        Assert.Equal([3.0, 10.0], median.ToArray<double>());

        // 2. Var(ddof = 1) 
        using Series variance = listSeries.List.Var(1);
        Assert.Equal([2.5, 0.0], variance.ToArray<double>());

        // 3. Std(ddof = 1) 
        using Series std = listSeries.List.Std(1);
        double[] stdResults = std.ToArray<double>();
        
        Assert.Equal(1.5811388300841898, stdResults[0], precision: 6);
        Assert.Equal(0.0, stdResults[1], precision: 6);
    }
    [Fact]
    [Trait("Series", "ListConcat")]
    public void Test_Series_List_Concat_All_Signatures()
    {
        int[][] data1 = [ [1, 2], [3, 4] ];
        using Series s1 = Pl.Series("s1", data1);

        int[][] data2 = [ [5], [6] ];
        using Series s2 = Pl.Series("s2", data2);
        using Series concatSingle = s1.List.Concat(s2);

        Assert.Equal([1, 3], concatSingle.List.Get(0).ToArray<int>());

        int[][] data3 = [ [7, 8], [9, 10] ];
        using Series s3 = Pl.Series("s3", data3);
        using Series concatMultiple = s1.List.Concat(s2, s3); 

        Assert.Equal([8, 10], concatMultiple.List.Get(-1).ToArray<int>());

        using Series concatSpan = s1.List.Concat([99,100]);
        
        // [3, 4, 100]
        using Series cs_last = concatSpan.List.Get(-1); 
        concatSpan.Show();
        Assert.Equal([99, 100], cs_last.ToArray<int>()); 
    }
    [Fact]
    [Trait("Series", "ListReverse")]
    public void Test_Series_List_Reverse()
    {
        int[][] data = [
            [1, 2, 3],
            [10, 20]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        using Series reversed = listSeries.List.Reverse();

        using Series r_0 = reversed.List.Get(0);
        using Series r_1 = reversed.List.Get(1);
        using Series r_last = reversed.List.Get(-1); 

        Assert.Equal([3, 20], r_0.ToArray<int>()); 
        Assert.Equal([2, 10], r_1.ToArray<int>()); 
        Assert.Equal([1, 10], r_last.ToArray<int>()); 
    }

    [Fact]
    [Trait("Series", "ListExplode")]
    public void Test_Series_List_Explode()
    {
        int[][] data = [
            [1, 2],
            [],
            [3, 4, 5]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        using Series exploded = listSeries.List.Explode();
        using Series explodedEmptyDrop = listSeries.List.Explode(emptyAsNull:false);

        Assert.Equal([1, 2, 0, 3, 4, 5], exploded.ToArray<int>());
        Assert.Equal(6, exploded.Len()); 
        Assert.Equal([1,2,3,4,5],explodedEmptyDrop.ToArray<int>());
    }

    [Fact]
    [Trait("Series", "ListToArray")]
    public void Test_Series_List_ToArray()
    {
        int[][] data = [
            [1, 2, 3],
            [4, 5, 6]
        ];
        using Series listSeries = Pl.Series("list_col", data);
        using Series arraySeries = listSeries.List.ToArray(3);

        Assert.Equal(arraySeries.DataType, DataType.Array(typeof(int),3));

        using Series structSeries = arraySeries.Array.ToStruct(["a", "b", "c"]);
        using Series col_a = structSeries.Struct.Field("a");
        Assert.Equal([1, 4], col_a.ToArray<int>());
    }

    [Fact]
    [Trait("Series", "ListToStruct")]
    public void Test_Series_List_ToStruct()
    {
        int[][] data = [
            [10, 20],
            [30, 40]
        ];
        using Series listSeries = Pl.Series("list_col", data);

        using Series s1 = listSeries.List.ToStruct("A", "B");
        
        using Series s1_a = s1.Struct.Field("A");
        using Series s1_b = s1.Struct.Field("B");
        Assert.Equal([10, 30], s1_a.ToArray<int>());
        Assert.Equal([20, 40], s1_b.ToArray<int>());

        using Series s2 = listSeries.List.ToStruct(i => $"Score_{i + 1}", 2);
        
        using Series s2_1 = s2.Struct.Field("Score_1");
        using Series s2_2 = s2.Struct.Field("Score_2");
        Assert.Equal([10, 30], s2_1.ToArray<int>());
        Assert.Equal([20, 40], s2_2.ToArray<int>());

        using Series s3 = listSeries.List.ToStruct(upperBound: 2);
        
        using Series s3_0 = s3.Struct.Field("field_0");
        using Series s3_1 = s3.Struct.Field("field_1");
        Assert.Equal([10, 30], s3_0.ToArray<int>());
        Assert.Equal([20, 40], s3_1.ToArray<int>());
    }
}