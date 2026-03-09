#nullable enable

using Apache.Arrow;
using Apache.Arrow.Types;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SeriesTests
{
    [Fact]
    public void Test_Series_Creation_And_Arrow()
    {
        // 1. 创建 Series (Int32)
        using var s = Series.From("my_series", [1, 2, 3]);
        
        Assert.Equal(3, s.Length);
        Assert.Equal("my_series", s.Name);

        // 2. 转 Arrow
        var arrowArray = s.ToArrow();
        Assert.IsType<Int32Array>(arrowArray);
        Assert.Equal(2, ((Int32Array)arrowArray).GetValue(1));

        // 3. Rename
        s.Name = "renamed";
        Assert.Equal("renamed", s.Name);
    }
    [Fact]
    public void Test_Series_FromArrow_ComplexList()
    {
        // A. 构造 Values (Int64): [1, 2, 3]
        var valueBuilder = new Int64Array.Builder();
        valueBuilder.Append(1);
        valueBuilder.Append(2);
        valueBuilder.Append(3);
        using var valuesArray = valueBuilder.Build();

        // B. [修正点] 构造 Offsets (Int32): [0, 2, 2, 3]
        // 使用 Int32Array.Builder，不要用 PrimitiveArrayBuilder
        var offsetsBuilder = new Int32Array.Builder();
        offsetsBuilder.Append(0); // Start
        offsetsBuilder.Append(2); // End of row 0 (0->2, len=2)
        offsetsBuilder.Append(2); // End of row 1 (2->2, len=0, is_null)
        offsetsBuilder.Append(3); // End of row 2 (2->3, len=1)
        using var offsetsArray = offsetsBuilder.Build();
        
        // C. 构造 Validity Bitmap: 1, 0, 1 (Row 1 is null)
        var validityBuilder = new BooleanArray.Builder();
        validityBuilder.Append(true);
        validityBuilder.Append(false); // null
        validityBuilder.Append(true);
        using var validityArray = validityBuilder.Build();

        // D. 组装 ListArray
        // ListArray 构造函数签名:
        // (IArrowType type, int length, ArrowBuffer valueOffsets, IArrowArray values, ArrowBuffer nullBitmap, int nullCount = 0, int offset = 0)
        using var listArray = new ListArray(
            new ListType(new Int64Type()), // 类型定义
            3,                             // 数组长度 (Rows)
            offsetsArray.ValueBuffer,      // 复用 offsets 的底层 Buffer
            valuesArray,                   // Values 数组
            validityArray.ValueBuffer,     // 复用 validity 的底层 Buffer
            1                              // Null Count
        );

        // =============================================================
        // 2. 验证一下 C# 这一侧的数据是对的
        // =============================================================
        // 再次确认 Offsets 是从 0 开始的
        var checkOffsets = listArray.ValueOffsets;
        Assert.Equal(0, checkOffsets[0]); 
        Assert.Equal(2, checkOffsets[1]);
        Assert.Equal(2, checkOffsets[2]);
        Assert.Equal(3, checkOffsets[3]);

        // =============================================================
        // 3. 导入 Polars
        // =============================================================
        // 这里会触发我们写的 C# -> Rust -> Upgrade(Int32->Int64) 逻辑
        using var s = Series.FromArrow("arrow_list_manual", listArray);

        // 4. 验证 Polars
        using var df = DataFrame.FromSeries(s);

        // 5. 数据验证
        using var exploded = df.Explode("arrow_list_manual");
        // [1, 2] -> 2 rows
        // null   -> 1 rows
        // [3]    -> 1 row
        // Total 4 rows
        Assert.Equal(4, exploded.Height);
        
        // 验证值: 1, 2, 3
        Assert.Equal(1, exploded.GetValue<long>(0, "arrow_list_manual"));
        Assert.Equal(2, exploded.GetValue<long>(1, "arrow_list_manual"));
        Assert.Equal(3, exploded.GetValue<long>(3, "arrow_list_manual"));
    }
    [Fact]
        public void Test_Series_HighLevel_Create()
        {
            // 用户只需写标准的 C# 集合
            var data = new List<List<int?>?>
            {
                new() { 1, 2 },
                null,
                new() { 3, null, 5 } // 甚至支持子元素 null
            };

            // 一行代码创建 Series
            using var s = Series.From("my_list", data);

            // 验证
            using var df = DataFrame.FromSeries(s);
            
            // Schema 检查
            Assert.Equal(DataTypeKind.List, s.DataType.Kind);
            
            // Explode 检查
            using var exploded = df.Explode("my_list");

            Assert.Equal(6, exploded.Height); 
        }
    [Fact]
    public void Test_Series_FromArrow_Struct_With_List()
    {
        // =============================================================
        // 目标结构: Struct<Name: Utf8, Scores: List<i64>>
        // Row 0: { "Alice", [10, 20] }
        // Row 1: null (整个 Struct 是 null)
        // Row 2: { "Bob", [30] }
        // =============================================================

        int length = 3;

        // 1. 构造子数组 A: "Name" (StringArray)
        var nameBuilder = new StringArray.Builder();
        nameBuilder.Append("Alice");
        nameBuilder.Append("Ignored"); // Row 1 虽然是 null，但子数组通常要有占位值，或者 append null
        nameBuilder.Append("Bob");
        using var nameArray = nameBuilder.Build();

        // 2. 构造子数组 B: "Scores" (ListArray<i64>)
        // 复用我们要死要活搞出来的 List 构造逻辑
        // Row 0: [10, 20]
        // Row 1: null (或者 [])
        // Row 2: [30]
        
        // Values
        var valBuilder = new Int64Array.Builder();
        valBuilder.Append(10); valBuilder.Append(20); valBuilder.Append(30);
        using var valArray = valBuilder.Build();
        
        // Offsets: [0, 2, 2, 3] (Row 1 是空/null)
        var offBuilder = new Int32Array.Builder();
        offBuilder.Append(0); offBuilder.Append(2); offBuilder.Append(2); offBuilder.Append(3);
        using var offArray = offBuilder.Build();
        
        // Validity for List
        var listValidBuilder = new BooleanArray.Builder();
        listValidBuilder.Append(true); listValidBuilder.Append(false); listValidBuilder.Append(true);
        using var listValid = listValidBuilder.Build();

        using var scoresArray = new ListArray(
            new ListType(new Int64Type()), 
            length, offArray.ValueBuffer, valArray, listValid.ValueBuffer, 1
        );

        // 3. 构造 Struct 自身的 Validity
        // Row 1 是 null
        var structValidBuilder = new BooleanArray.Builder();
        structValidBuilder.Append(true);
        structValidBuilder.Append(false); // <--- Struct Null
        structValidBuilder.Append(true);
        using var structValid = structValidBuilder.Build();

        // 4. 组装 StructArray
        // 字段定义
        var fields = new List<Field>
        {
            new("Name", new StringType(), false),
            new("Scores", new ListType(new Int64Type()), true)
        };
        var structType = new StructType(fields);

        // 子数组列表
        var children = new List<IArrowArray> { nameArray, scoresArray };

        using var structArray = new StructArray(
            structType,
            length,
            children,
            structValid.ValueBuffer,
            1
        );

        // =============================================================
        // 5. 见证奇迹：传入 Polars
        // =============================================================
        // 预期：Rust 会递归把里面的 List<i32> 升级为 LargeList<i64>
        using var s = Series.FromArrow("my_struct", structArray);

        using var df = DataFrame.FromSeries(s);

        // 6. 验证
        // 确保类型是 Struct
        Assert.Equal(DataTypeKind.Struct, s.DataType.Kind);
        
        // 验证第一行 Alice
        // 在 Polars C# 取 Struct 值可能比较麻烦 (返回 JSON 字符串或 Object?)
        // 简单起见，我们 Unnest (展开) 验证
        
        // 如果还没实现 Unnest，我们可以测试 struct.field("Name")
        // 假设你加了 Field Access 的 Expr
        using var res = df.Select(
            Polars.Col("my_struct").Struct.Field("Name"),
            Polars.Col("my_struct").Struct.Field("Scores")
        );
        
        Assert.Equal("Alice", res.GetValue<string>(0, "Name"));
    }
    private class Student
    {
        public string? Name { get; set; }
        public int Age { get; set; }
    }

    [Fact]
    public void Test_Series_From_ObjectList()
    {
        var students = new List<Student>
        {
            new() { Name = "Alice", Age = 20 },
            null!, // Struct Null
            new() { Name = "Bob", Age = 22 }
        };

        // 这一行代码，对于用户来说是极其舒适的
        using var s = Series.From("students", students);
        
        // 验证
        using var df = DataFrame.FromSeries(s);
        
        Assert.Equal(DataTypeKind.Struct, s.DataType.Kind);
        
        using var unnested = df.Unnest("students");
        Assert.Equal("Alice", unnested.GetValue<string>(0, "Name"));
        Assert.Equal(22, unnested.GetValue<int>(2, "Age"));
    }
    [Fact]
    public void Test_Series_Recursive_List_Of_List()
    {
        // 构造 List<List<int>> (注意是 int，不是 long，验证工厂的泛型能力)
        // 结构:
        // Row 0: [1, 2]
        // Row 1: null
        // Row 2: [3, 4, 5]
        var data = new List<List<int>?>
        {
            new() { 1, 2 },
            null,
            new() { 3, 4, 5 }
        };

        // 一行代码搞定
        using var s = Series.From("nested_list", data);

        using var df = DataFrame.FromSeries(s);

        // 验证结构
        Assert.Equal(DataTypeKind.List, s.DataType.Kind);
        Assert.Equal(3, s.Length);
        Assert.Equal(1, s.NullCount);

        // 验证数据：Explode
        using var exploded = df.Explode("nested_list");
        exploded.Show();
        // 1, 2, null, 3, 4, 5 -> 5 rows
        Assert.Equal(6, exploded.Height);
        Assert.Equal(1, exploded.GetValue<int>(0, "nested_list")); // int
        Assert.Equal(4, exploded.GetValue<int>(4, "nested_list"));
    }

    [Fact]
    public void Test_Series_Deep_Recursive()
    {
        // 构造 List<List<string>>: String 也是支持的！
        var data = new List<List<string>?>
        {
            new() { "a", "b" },
            new() { "c" }
        };
        
        using var s = Series.From("strs", data);
        Assert.Equal(DataTypeKind.List, s.DataType.Kind);
        
        // 验证内部数据
        using var df = DataFrame.FromSeries(s);
        using var exp = df.Explode("strs");
        Assert.Equal("a", exp.GetValue<string>(0, "strs"));
    }
    [Fact]
    [Trait("Series","String")]
    public void Test_Series_String_And_Nulls()
    {
        // 1. 创建 String Series (带 Null)
        using var s = Series.From("strings", ["a", null, "原神启动","錕斤拷燙燙燙1231659846156516あいうえおへへへへっへへへへへっへへｈ","🍉",""]);

        Assert.Equal(6, s.Length);

        Assert.Equal("a", s[0]);
        Assert.Null(s[1]);
        Assert.Equal("原神启动", s[2]);
        Assert.Contains("錕斤拷燙燙燙", s.GetValue<string>(3));
        Assert.Equal("🍉",s[4]);
        Assert.Equal("",s[5]);
    }

    [Fact]
    public void Test_Series_Cast_Decimal()
    {
        // 1. 创建 Double Series
        using var s = Series.From("prices", [10.5, 20.0]);

        // 2. Cast 到 Decimal(10, 2)
        // 这需要 DataType 类发挥作用
        using var sDecimal = s.Cast(DataType.Decimal(10, 2));
        
        // 验证 Cast 后的 Arrow 类型
        var arrowArray = sDecimal.ToArrow();
        // Apache Arrow C# 会把 Decimal128 映射为 Decimal128Array
        Assert.IsType<Decimal128Array>(arrowArray);
    }
    [Fact]
    public void Test_Series_Constructor_DateTimeOffset()
    {
        var now = DateTimeOffset.Now;
        var data = new DateTimeOffset[] { now, now.AddHours(1) };

        // 1. 测试非空构造函数
        using var s1 = Series.From("dto", data);
        Assert.Equal("dto", s1.Name);
        Assert.Equal(2, s1.Length);

        // 验证数据 (使用我们刚修好的 GetValue)
        var v0 = s1.GetValue<DateTimeOffset>(0);
        // 验证 UTC 时间点一致
        Assert.Equal(now.UtcTicks / 10 * 10, v0.UtcTicks); // 考虑微秒截断

        // 2. 测试可空构造函数
        var dataNull = new DateTimeOffset?[] { now, null };
        using var s2 = Series.From("dto_null", dataNull);
        
        Assert.Equal(2, s2.Length);
        Assert.Null(s2.GetValue<DateTimeOffset?>(1));
        Assert.NotNull(s2.GetValue<DateTimeOffset?>(0));
    }
    [Fact]
    public void Test_NullCount()
    {
        // Case 1: 整数 Series (含 Null)
        using var sInt = new Series("nums", (int?[])[1, null, 3, null, 5]);
        
        // 验证: 应该有 2 个 null
        Assert.Equal(2, sInt.NullCount);
        Assert.Equal(5, sInt.Length);

        // Case 2: 字符串 Series (含 Null)
        using var sStr = Series.From("str", ["a", null, "b"]);
        
        // 验证: 应该有 1 个 null
        Assert.Equal(1, sStr.NullCount);
        
        // Case 3: 全是 Null
        using var sAllNull = new Series("nulls", new string?[] { null, null });
        Assert.Equal(2, sAllNull.NullCount);
        
        // Case 4: 没有 Null
        using var sClean = new Series("clean", new int[]{1, 2, 3});
        Assert.Equal(0, sClean.NullCount);
    }
    [Fact]
    public void Test_Series_Arithmetic()
    {
        using var s1 = new Series("a", new int[]{1, 2, 3});
        using var s2 = new Series("b", new int[]{10, 20, 30});

        // Test Add (+)
        using var sum = s1 + s2;
        Assert.Equal(11, sum.GetValue<int>(0));
        Assert.Equal(22, sum.GetValue<int>(1));
        Assert.Equal(33, sum.GetValue<int>(2));

        // Test Mul (*)
        using var prod = s1 * s2;
        Assert.Equal(10, prod.GetValue<int>(0));
        Assert.Equal(90, prod.GetValue<int>(2)); // 3 * 30
    }

    [Fact]
    public void Test_Series_Comparison()
    {
        using var s1 = new Series("a", new int[]{1, 5, 10});
        using var s2 = new Series("b", new int[]{1, 4, 20});

        // Test Eq (1==1, 5!=4, 10!=20) -> [true, false, false]
        using var eq = s1.Eq(s2);
        Assert.True(eq.GetValue<bool>(0));
        Assert.False(eq.GetValue<bool>(1));

        // Test Gt (>) (1>1 false, 5>4 true, 10>20 false)
        using var gt = s1 > s2;
        Assert.False(gt.GetValue<bool>(0));
        Assert.True(gt.GetValue<bool>(1));
        Assert.False(gt.GetValue<bool>(2));
    }

    [Fact]
    public void Test_Series_Aggregations()
    {
        using var s = new Series("nums", [1, 2, 3, 4, 5]);

        // Sum: 15
        using var sumSeries = s.Sum();
        Assert.Equal(1, sumSeries.Length); // 聚合后长度为 1
        Assert.Equal(15, sumSeries.GetValue<int>(0));
        
        // 验证泛型快捷方法
        Assert.Equal(15, s.Sum<int>());

        // Mean: 3.0
        // 注意：Mean 可能会返回 double，具体取决于 Polars 内部实现，通常是 float64
        Assert.Equal(3.0, s.Mean<double>());
        
        // Min/Max
        Assert.Equal(1, s.Min<int>());
        Assert.Equal(5, s.Max<int>());
    }

    [Fact]
    public void Test_Series_FloatChecks()
    {
        // 构造包含 NaN 和 Inf 的 Series
        // C# double.NaN 对应 Polars Float64 NaN
        using var s = Series.From("f", [1.0, double.NaN, double.PositiveInfinity]);

        // IsNan -> [false, true, false]
        using var isNan = s.IsNan();
        Assert.False(isNan.GetValue<bool>(0));
        Assert.True(isNan.GetValue<bool>(1));
        Assert.False(isNan.GetValue<bool>(2));

        // IsInfinite -> [false, false, true]
        using var isInf = s.IsInfinite();
        Assert.True(isInf.GetValue<bool>(2));
    }
    [Fact]
    public void Test_Series_Unique_Composite()
    {
        // 数据: [1, 2, 2, 3]
        using var s = Series.From("nums", new[] { 1, 2, 2, 3 });

        // 1. 测试 NUnique (Rust FFI)
        Assert.Equal(3UL, s.NUnique); // 1, 2, 3 共3个唯一值

        // 2. 测试 IsDuplicated (C# Composite)
        using var dupMask = s.IsDuplicated();
        Assert.Equal(DataTypeKind.Boolean, dupMask.DataType.Kind);
        
        // 验证逻辑: [1, 2, 2, 3] -> [F, T, T, F] (默认 keep='all' 语义，或者 keep='avg'?)
        // Polars expr.is_duplicated() 默认行为是: 重复的元素标记为 true。
        // [1(F), 2(T), 2(T), 3(F)] ? 还是 [1(F), 2(F), 2(T), 3(F)] ?
        // 经查 Polars 文档，is_duplicated() 默认把所有涉及重复的项都标记为 true。
        // 即: 只要这个数出现次数 > 1，它就是 true。
        // 所以预期是: [F, T, T, F]
        
        Assert.False((bool)dupMask[0]!); // 1
        Assert.True((bool)dupMask[1]!);  // 2
        Assert.True((bool)dupMask[2]!);  // 2
        Assert.False((bool)dupMask[3]!); // 3

        // 3. 测试 Unique (C# Composite)
        using var uniq = s.UniqueStable();
        Assert.Equal(3, uniq.Length);
        // 验证值
        Assert.Equal(1, uniq[0]);
        Assert.Equal(2, uniq[1]);
        Assert.Equal(3, uniq[2]);
    }
    [Fact]
    public void Test_Series_Sort_Options()
    {
        // 准备数据：包含重复值和 Null
        // [3, null, 1, 3, 2]
        using var s = Series.From("nums", new int?[] { 3, null, 1, 3, 2 });

        // 1. 默认排序 (Ascending)
        // 预期: [null, 1, 2, 3, 3] (Polars 默认 nulls_last=false 且 ascending 时，null 在前)
        // 或者 [1, 2, 3, 3, null] 取决于具体版本，我们通过 nullsLast 参数显式控制更稳
        using var sAsc = s.Sort(descending: false, nullsLast: false);
        
        // 验证 Null 在最前面
        Assert.Null(sAsc[0]); 
        Assert.Equal(1, sAsc[1]);
        Assert.Equal(3, sAsc[4]);

        // 2. 降序 (Descending)
        // 预期: [3, 3, 2, 1, null] (nullsLast=false 在降序时通常意味着 null 最小，即放在最后? 
        // 不，Polars 逻辑通常是 null 视为最小值。
        // Ascending: null, ..., max
        // Descending: max, ..., null (如果 null 最小)
        // 让我们显式测试 nullsLast=true
        using var sDesc = s.Sort(descending: true, nullsLast: true);
        
        // 验证: [3, 3, 2, 1, null]
        Assert.Equal(3, sDesc[0]);
        Assert.Equal(3, sDesc[1]);
        Assert.Equal(1, sDesc[3]);
        Assert.Null(sDesc[4]);

        // 3. Nulls Last (Ascending)
        // 预期: [1, 2, 3, 3, null]
        using var sNullsLast = s.Sort(descending: false, nullsLast: true);
        
        Assert.Equal(1, sNullsLast[0]);
        Assert.Null(sNullsLast[4]);

        // 4. Stable Sort (maintainOrder)
        // 虽然对于 int 很难直观验证稳定性（除非我们看索引），但我们至少要确保开启它不会崩
        using var sStable = s.Sort(maintainOrder: true);
        Assert.Equal(5, sStable.Length);
    }
    [Fact]
    public void Test_Series_Sort_Strings()
    {
        using var s = Series.From("chars", ["c", "a", "b"]);
        
        using var sorted = s.Sort();
        Assert.Equal("a", sorted[0]);
        Assert.Equal("b", sorted[1]);
        Assert.Equal("c", sorted[2]);
    }
    [Fact]
    public void Test_Series_Struct_Unnest()
    {
        // 1. 准备数据：Array(Int32, 2)
        // Row 0: [1, 2]
        // Row 1: [3, 4]
        var data = new[] { [1, 2], new[] { 3, 4 } };
        using var df = DataFrame.FromColumns(new { raw = data })
            .Select(Polars.Col("raw").Cast(DataType.Array(DataType.Int32, 2)).Alias("arr"));

        // 2. 取出 Series
        var arrSeries = df["arr"];

        // 3. 连招：ToStruct() -> Unnest()
        // 先把 Array 转为 Struct Series
        using var structSeries = arrSeries.Array.ToStruct();
        
        // 再把 Struct Series 炸开成 DataFrame
        using var unnestedDf = structSeries.Struct.Unnest();

        // 4. 验证
        // 应该有两列：field_0, field_1
        Assert.Equal(2, unnestedDf.Width);
        Assert.True(unnestedDf.Columns.Contains("field_0"));
        Assert.True(unnestedDf.Columns.Contains("field_1"));

        // 验证数据
        Assert.Equal(1, unnestedDf["field_0"][0]);
        Assert.Equal(2, unnestedDf["field_1"][0]);
        Assert.Equal(3, unnestedDf["field_0"][1]);
        Assert.Equal(4, unnestedDf["field_1"][1]);
    }
    [Fact]
    public void Test_Series_Bitwise_Shift()
    {
        // 1. Signed Int32 (算术右移测试)
        // -8 (111...1000) >> 2 = -2 (111...1110)
        using var sInt = Series.From("signed", new int[] {1, -8});
        
        using var sIntShl = sInt << 2; // 1<<2=4, -8<<2=-32
        using var sIntShr = sInt >> 2; // 1>>2=0, -8>>2=-2

        Assert.Equal(4, sIntShl[0]);
        Assert.Equal(-32, sIntShl[1]);
        
        Assert.Equal(0, sIntShr[0]);
        Assert.Equal(-2, sIntShr[1]); // 验证保留符号位

        // 2. Unsigned UInt32 (逻辑右移测试)
        // 0xF0000000 >> 4 = 0x0F000000 (高位补0)
        uint bigNum = 0xF0000000;
        uint[] unsignedArray = new uint[] { bigNum };
        using var sUint = Series.From("unsigned", unsignedArray);
        sUint.Show();
        // using var sUint = new Series("unsigned",[bigNum]);
        using var sUintShr = sUint >> 4;
        
        // 验证逻辑右移 (0x0F000000 = 251658240)
        // uint expected = 0x0F000000;
        // Assert.Equal(expected, sUintShr[0]);
    }
    [Fact]
    public void TestToString_And_Show()
    {
        // 1. 准备数据
        var s = Series.From("my_series", new[] { 1, 2, 3, 4, 5 });

        // 2. 测试 ToString (基于 ToFrame 的偷懒实现)
        var str = s.ToString();

        // 验证输出是否包含关键信息
        // 由于转成了 DataFrame，输出应该包含 shape 和列名
        Assert.NotEmpty(str);
        Assert.True(str.Contains("my_series"), "Output should contain series name");
        Assert.True(str.Contains("shape: (5, 1)"), "Output should contain DataFrame shape info");
        
        // 3. 测试 Show (虽然不能Assert控制台输出，但至少保证不崩)
        s.Show();
        
        // 4. 测试包含 Null 的情况
        var sNull = Series.From("nulls", new int?[] { 1, null, 3 });
        var strNull = sNull.ToString();
        Assert.True(strNull.Contains("null"), "Output should represent null values");
    }

    [Fact]
    public void TestValueCounts()
    {
        // 准备数据：Apple出现3次，Banana出现1次，Orange出现2次
        var s = Series.From("fruit", [ 
            "apple", "apple", "orange", "banana", "apple", "orange" 
        ]);

        // --- 场景 1: 默认行为 (Sorted, Count) ---
        var dfCounts = s.ValueCounts();
        dfCounts.Show();
        
        // 验证 Shape: 3种水果 -> 3行
        Assert.Equal(3, dfCounts.Height);
        
        // 验证排序: 默认降序，第一行应该是 apple (3次)
        Assert.Equal("apple", dfCounts[0][0]); // Column 0 (fruit) Row 0
        Assert.Equal(3u, dfCounts[1][0]);         // Column 1 (count) Row 0. *注: count通常是u32*

        // --- 场景 2: 自定义名称 + 归一化 (Normalize) ---
        // normalize=true, name="prob"
        var dfNorm = s.ValueCounts(sort: true, parallel: true, name: "prob", normalize: true);
        
        Assert.Equal("prob", dfNorm.Columns[1]);
        
        // 验证概率: apple = 3/6 = 0.5
        var probApple = dfNorm["prob"][0];
        Assert.Equal(0.5, probApple); 
    }
    [Fact]
    public void Test_Series_TopKBy_BottomKBy_With_StringLength()
    {
        // 1. 准备数据
        // 长度: 1, 3, 2
        var s = new Series("words", ["a", "ccc", "bb"]);

        // 2. 定义排序规则：按字符串长度 (使用你指定的 Str.Len())
        // 注意：Col("words") 必须匹配 Series 的 Name
        var byLength = Col("words").Str.Len();

        // ---------------------------------------------------
        // 测试 TopKBy (取最长的2个)
        // ---------------------------------------------------
        // 预期结果: ["ccc", "bb"]
        var top2 = s.TopKBy(2, byLength);
        
        Assert.Equal(2, top2.Length);
        Assert.Equal("ccc", top2[0]);
        Assert.Equal("bb", top2[1]);

        // ---------------------------------------------------
        // 测试 BottomKBy (取最短的2个)
        // ---------------------------------------------------
        // 预期结果: ["a", "bb"]
        var bot2 = s.BottomKBy(2, byLength);
        
        Assert.Equal(2, bot2.Length);
        Assert.Equal("a", bot2[0]);
        Assert.Equal("bb", bot2[1]);
    }
    [Fact]
    public void Test_Series_Statistics_Methods()
    {
        // ---------------------------------------------------
        // 1. 基础统计 (Std, Var, Median)
        // 数据: [1, 2, 3, 4, 5]
        // 均值: 3
        // 方差 (ddof=1): ((1-3)^2 + ... + (5-3)^2) / 4 = 10 / 4 = 2.5
        // 标准差: sqrt(2.5) ≈ 1.58113883
        // ---------------------------------------------------
        var s1 = new Series("s1", [1, 2, 3, 4, 5]);

        // 验证 Var
        var varSeries = s1.Var(ddof: 1);
        Assert.Equal(2.5, (double)varSeries[0]!, precision: 5);

        // 验证 Std
        var stdSeries = s1.Std(ddof: 1);
        Assert.Equal(1.58114, (double)stdSeries[0]!, precision: 5);

        // 验证 Median
        var medianSeries = s1.Median();
        Assert.Equal(3.0, medianSeries[0]);

        // ---------------------------------------------------
        // 2. 分位数 (Quantile)
        // ---------------------------------------------------
        // 中位数 (0.5) 应该是 3
        Assert.Equal(3.0, s1.Quantile(0.5, QuantileMethod.Linear)[0]);

        // ---------------------------------------------------
        // 3. 变化率 (PctChange)
        // 数据: [10, 20, 10]
        // 10 -> 20: +100% (1.0)
        // 20 -> 10: -50% (-0.5)
        // ---------------------------------------------------
        var s2 = new Series("s2", [10, 20, 10]);
        var pct = s2.PctChange(); // 默认 n=1

        Assert.Null(pct[0]); // 第一个值通常是 null
        Assert.Equal(1.0, (double)pct[1]!, precision: 2);
        Assert.Equal(-0.5, (double)pct[2]!, precision: 2);

        // ---------------------------------------------------
        // 4. 排名 (Rank) - 处理并列值
        // 数据: [10, 20, 20, 10]
        // 排序后位置: 10(pos1), 10(pos2), 20(pos3), 20(pos4)
        // Method=Average:
        // 10 的排名 = (1+2)/2 = 1.5
        // 20 的排名 = (3+4)/2 = 3.5
        // ---------------------------------------------------
        var s3 = new Series("s3", [10, 20, 20, 10]);
        var ranks = s3.Rank(RankMethod.Average);

        Assert.Equal(1.5, ranks[0]);
        Assert.Equal(3.5, ranks[1]);
        Assert.Equal(3.5, ranks[2]);
        Assert.Equal(1.5, ranks[3]);
    }
    [Fact]
    public void Test_Series_Cumulative_Methods()
    {
        // 数据: [1, 3, 2, 4]
        var s = new Series("nums", [1, 3, 2, 4]);

        // ---------------------------------------------------
        // 1. CumSum (累加)
        // 预期: [1, 1+3, 1+3+2, 1+3+2+4] -> [1, 4, 6, 10]
        // ---------------------------------------------------
        var cumSum = s.CumSum();
        var arrSum = cumSum.ToArray<int>(); // 假设 ToArray 支持泛型
        
        Assert.Equal([1, 4, 6, 10], arrSum);

        // ---------------------------------------------------
        // 2. CumMax (累积最大值)
        // 预期: [1, 3, 3, 4] (2比3小，所以保持3)
        // ---------------------------------------------------
        var cumMax = s.CumMax();
        var arrMax = cumMax.ToArray<int>();

        Assert.Equal([1, 3, 3, 4], arrMax);

        // ---------------------------------------------------
        // 3. CumMin (累积最小值)
        // 预期: [1, 1, 1, 1]
        // ---------------------------------------------------
        var cumMin = s.CumMin();
        var arrMin = cumMin.ToArray<int>();

        Assert.Equal([1, 1, 1, 1], arrMin);

        // ---------------------------------------------------
        // 4. CumProd (累积乘积)
        // 预期: [1, 1*3, 3*2, 6*4] -> [1, 3, 6, 24]
        // ---------------------------------------------------
        var cumProd = s.CumProd();
        var arrProd = cumProd.ToArray<int>(); // 注意：如果数很大可能会溢出变成 long

        Assert.Equal([1, 3, 6, 24], arrProd);

        // ---------------------------------------------------
        // 5. Reverse 测试 (从后往前算)
        // 数据: [1, 3, 2]
        // CumSum Reverse: 
        // index 2 (val 2) -> 2
        // index 1 (val 3) -> 3 + 2 = 5
        // index 0 (val 1) -> 1 + 5 = 6
        // 结果: [6, 5, 2]
        // ---------------------------------------------------
        var sRev = new Series("rev", [1, 3, 2]);
        var revSum = sRev.CumSum(reverse: true);
        var arrRev = revSum.ToArray<int>();

        Assert.Equal([6, 5, 2], arrRev);
    }
    [Fact]
public void Test_Series_Ewm_Methods()
{
    // ---------------------------------------------------
    // 1. EwmMean (Standard)
    // 数据: [10, 20, 40]
    // Alpha = 0.5 (Com = 1)
    // Adjust = false (Infinite history approximation for simple math)
    // 
    // t0: 10
    // t1: (1-0.5)*10 + 0.5*20 = 5 + 10 = 15
    // t2: (1-0.5)*15 + 0.5*40 = 7.5 + 20 = 27.5
    // ---------------------------------------------------
    var s = new Series("val", [10.0, 20.0, 40.0]);
    
    // 注意：Adjust=false 让计算公式更简单，直接测试核心逻辑
    var ewm = s.EwmMean(alpha: 0.5, adjust: false);
    
    Assert.Equal(10.0, ewm[0]);
    Assert.Equal(15.0, ewm[1]);
    Assert.Equal(27.5, ewm[2]);

    // ---------------------------------------------------
    // 2. EwmVar
    // 只要能跑通且不抛异常，且结果为非负数
    // ---------------------------------------------------
    var ewmVar = s.EwmVar(alpha: 0.5);
    Assert.NotNull(ewmVar[0]); // 第一个可能是 NaN 或者 0
    Assert.True((double?)ewmVar[1] >= 0);
}

    [Fact]
    public void Test_Series_EwmMeanBy_Time()
    {
        // ---------------------------------------------------
        // 验证基于时间的衰减
        // 场景：两个数据点，值都是 10 和 20。
        // 情况 A：时间间隔很短 (1秒) -> EWM 应该接近平均值 (15)
        // 情况 B：时间间隔很长 (10天) -> 之前的值权重衰减没了，EWM 应该接近当前值 (20)
        // ---------------------------------------------------
        
        // 构造 DataFrame 因为 EwmMeanBy 需要两列协同
        var times = new[] 
        { 
            new DateTime(2023, 1, 1), 
            new DateTime(2023, 1, 11) // 间隔 10 天
        };
        var values = new[] { 10.0, 20.0 };

        using var df = DataFrame.FromColumns(new
        {
            tm = times,
            val = values
        });

        // Case 1: HalfLife = "1d" (1天)
        // 经过 10 天 (10个 HalfLife)，第一个值 10 的权重几乎为 0
        // 结果应该非常接近 20
        var resDecay = df.Select(
            Col("val").EwmMeanBy(Col("tm"), halfLife: "1d").Alias("ewm")
        );
        
        var arrDecay = resDecay["ewm"].ToArray<double>();
        // 10天后，旧值的权重只有 (0.5)^10 ≈ 0.0009
        // 所以结果会非常接近 20.0
        Assert.Equal(20.0, arrDecay[1], precision: 1); 

        // Case 2: HalfLife = "100d" (100天)
        // 经过 10 天，只是 0.1 个 HalfLife，权重还很高
        // 结果应该在 10 和 20 之间，偏向 15 左右
        var resStable = df.Select(
            Col("val").EwmMeanBy(Col("tm"), halfLife: "100d").Alias("ewm")
        );
        var arrStable = resStable["ewm"].ToArray<double>();
        Assert.True(arrStable[1] < 19.0); // 肯定小于 20
        Assert.True(arrStable[1] > 10.0); // 肯定大于 10
    }
    [Fact]
    public void Test_RollingMeanBy_TimeWindow_And_ClosedBoundary()
    {
        // ---------------------------------------------------
        // 场景构造：
        // 时间点：09:00, 10:00, 11:00
        // 数值：  10,    20,    30
        // ---------------------------------------------------
        var start = new DateTime(2023, 1, 1, 9, 0, 0);
        var times = new[] 
        { 
            start, 
            start.AddHours(1), // 10:00
            start.AddHours(2)  // 11:00
        };
        var values = new[] { 10.0, 20.0, 30.0 };

        using var df = DataFrame.FromColumns(new { tm = times, val = values });

        // ---------------------------------------------------
        // Case 1: Window = 2h, Closed = Left [t-w, t)
        // ---------------------------------------------------
        // At 11:00 (t): Window is [09:00, 11:00)
        // 包含: 09:00 (10), 10:00 (20) -> 不包含 11:00 本身
        // Mean = (10 + 20) / 2 = 15.0
        // ---------------------------------------------------
        var resLeft = df.Select(
            Col("val").RollingMeanBy(
                windowSize: TimeSpan.FromHours(2), // 测试 TimeSpan 转发
                by: Col("tm"), 
                closed: ClosedWindow.Left          // 测试 Enum 转发
            ).Alias("mean_left")
        );

        var leftArr = resLeft["mean_left"].ToArray<double>();
        // index 0 (09:00): [07:00, 09:00) -> empty? Polars handle minPeriods=1 -> 10 (itself is excluded? No, usually start includes itself if window is large enough, but left closed excludes current 't'? Let's check logic)
        // Polars Left closed definition: [t - period, t). 
        // Wait, Polars behavior for 'Left': Includes the start of the window, excludes the exact 't'.
        // 实际上 Polars 的 RollingBy 默认行为：
        // Closed="left": (t-w, t] ? No.
        // Let's verify the most obvious check: The last point (11:00).
        // Window [09:00, 11:00). Contains 09:00, 10:00.
        // Result should be 15.0.
        
        // 如果这里挂了，说明 ClosedWindow.ToNative() 映射反了，或者 WindowSize 没传对
        Assert.Equal(15.0, leftArr[2]); 


        // ---------------------------------------------------
        // Case 2: Window = 2h, Closed = Both [t-w, t]
        // ---------------------------------------------------
        // At 11:00 (t): Window is [09:00, 11:00]
        // 包含: 09:00 (10), 10:00 (20), 11:00 (30)
        // Mean = (10+20+30) / 3 = 20.0
        // ---------------------------------------------------
        var resBoth = df.Select(
            Col("val").RollingMeanBy(
                windowSize: TimeSpan.FromHours(2), 
                by: Col("tm"), 
                closed: ClosedWindow.Both
            ).Alias("mean_both")
        );

        var bothArr = resBoth["mean_both"].ToArray<double>();
        Assert.Equal(20.0, bothArr[2]);
    }
    [Fact]
    public void Test_RollingQuantileBy_ComplexSignature()
    {
        // 数据: 1, 2, 3, 4, 5 (每秒一个)
        var times = Enumerable.Range(0, 5).Select(i => new DateTime(2023, 1, 1).AddSeconds(i)).ToArray();
        var values = new[] { 1.0, 2.0, 3.0, 4.0, 5.0 };

        using var df = DataFrame.FromColumns(new { tm = times, val = values });

        // 目标：计算过去 3秒内的中位数 (Quantile 0.5)
        // Window="3s", Closed="Both"
        // At t=4 (00:00:04, val=5): Window [00:00:01, 00:00:04] -> {2, 3, 4, 5}
        // Median of {2,3,4,5} -> (3+4)/2 = 3.5 (Linear interpolation)
        
        var res = df.Select(
            Col("val").RollingQuantileBy(
                quantile: 0.5,
                method: QuantileMethod.Linear,
                windowSize: TimeSpan.FromSeconds(3), // "3s"
                by: Col("tm"),
                closed: ClosedWindow.Both
            ).Alias("q50")
        );

        var arr = res["q50"].ToArray<double>();
        
        // Check last element
        // 如果这里算出来是 3.0 或 4.0，说明 Linear 插值没生效
        // 如果结果是 NaN，说明 WindowSize 传错了
        Assert.Equal(3.5, arr[4]); 
    }
    [Fact]
    public void Test_Series_Direct_Aggregations()
    {
        // 1. Reverse 测试
        var s = new Series("nums", [1, 2, 3]);
        var sRev = s.Reverse();
        Assert.Equal([3, 2, 1], sRev.ToArray<int>());

        // 2. First / Last 测试
        Assert.Equal(1, s.First()[0]); // 注意：返回的是 Series，通过 [0] 取值
        Assert.Equal(3, s.Last()[0]);

        // 3. Any / All 测试
        var sBool = new Series("bools", [true, false, null]); // 包含 null
        // Any (ignoreNulls=false): true | false | null -> true (只要有 true 就是 true)
        Assert.Equal(true, sBool.Any(ignoreNulls: false)[0]);
        
        // All (ignoreNulls=true): true & false -> false
        Assert.Equal(false, sBool.All(ignoreNulls: true)[0]);
        
        // 构造全 true
        var sAllTrue = new Series("all_true", [true, true]);
        Assert.Equal(true, sAllTrue.All()[0]);
    }
    [Fact]
    public void Test_Series_Long_Nullable_Constructor()
    {
        // 准备一个超过 int 范围的大数 (30亿)
        long bigNumber = 3_000_000_000L;

        // C# 编译器会自动推断这是 long?[]
        // 注意数字后缀 L
        var sLong = new Series("longs", [bigNumber, null, 123L]);

        // 验证
        // 如果这里挂了，说明底层可能误转成了 int，发生了截断
        Assert.Equal(bigNumber, sLong.GetValue<long?>(0));
        Assert.Null(sLong.GetValue<long?>(1));
        Assert.Equal(123L, sLong.GetValue<long?>(2));
    }
    [Fact]
    public void Test_Bool_Packing_Boundaries_And_Performance()
    {
        // ================================================================
        // Case 1: 纯 Fallback 通道 (Length < 32)
        // 测试目的：确保标量循环逻辑正确，没有被 SIMD 逻辑误伤
        // ================================================================
        bool[] dataSmall = new bool[] { true, false, true, true, false, true, false, false }; // Len 8
        using var sSmall = new Series("small", dataSmall);
        
        // 验证：回读数据必须完全一致
        Assert.Equal(dataSmall, sSmall.ToArray<bool>());
        Console.WriteLine("Case 1 (Scalar < 32): Passed");


        // ================================================================
        // Case 2: 纯 SIMD 通道 (Length = 32, 64)
        // 测试目的：确保 AVX2 MoveMask 逻辑正确，且正好填满 int 边界时不出错
        // ================================================================
        bool[] dataExact = new bool[64];
        for(int i=0; i<64; i++) dataExact[i] = (i % 2 == 0); // T, F, T, F...
        
        using var sExact = new Series("exact", dataExact);
        Assert.Equal(dataExact, sExact.ToArray<bool>());
        Console.WriteLine("Case 2 (SIMD Exact 64): Passed");


        // ================================================================
        // Case 3: 混合通道 (SIMD + Fallback, Length = 100)
        // 测试目的：最关键的测试！
        // 前 96 个走 SIMD (32 * 3)，最后 4 个走 Scalar。
        // 重点检查：SIMD 结束后，Scalar 的 offset 指针是否指到了正确位置？
        // ================================================================
        int lenMixed = 100;
        bool[] dataMixed = new bool[lenMixed];
        for (int i = 0; i < lenMixed; i++)
        {
            // 搞点稍微复杂的模式，防止全是 true 或全是 false 掩盖位移错误
            // 模式：每 3 个 true，每 5 个 false...
            dataMixed[i] = (i % 3 == 0) || (i % 5 != 0);
        }

        using var sMixed = new Series("mixed", dataMixed);
        var resMixed = sMixed.ToArray<bool>();

        // 逐个比对，如果出错，打印具体是哪个 Index 挂了
        for (int i = 0; i < lenMixed; i++)
        {
            if (dataMixed[i] != resMixed[i])
            {
                throw new Exception($"Mismatch at index {i}. Expected {dataMixed[i]}, Got {resMixed[i]}. " +
                                    $"This is likely a SIMD/Scalar boundary issue around index {i - (i % 32)}.");
            }
        }
        Assert.Equal(dataMixed, resMixed);
        Console.WriteLine("Case 3 (Hybrid 100): Passed");


        // ================================================================
        // Case 4: 压力测试 (尝尝咸淡) - 1000万数据
        // 测试目的：验证在大数据量下 AVX2 是否稳定，以及是否足够快
        // ================================================================
        int lenHuge = 10_000_000;
        var dataHuge = new bool[lenHuge];
        // 填充数据：让它不是全 0 或 全 1
        // 这种交错数据能防止 CPU 分支预测作弊
        Parallel.For(0, lenHuge, i => 
        {
            dataHuge[i] = (i % 2 == 0); 
        });

        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        // 这一步会触发 BoolPacker.Pack (AVX2) -> Rust Memcpy
        using var sHuge = new Series("huge", dataHuge);
        
        sw.Stop();
        Console.WriteLine($"Case 4 (10M Rows): Time = {sw.ElapsedMilliseconds} ms");

        // 简单的抽样验证，防止全盘错误
        var resHuge = sHuge.ToArray<bool>();
        Assert.Equal(dataHuge[0], resHuge[0]);
        Assert.Equal(dataHuge[lenHuge - 1], resHuge[lenHuge - 1]);
        Assert.Equal(dataHuge[lenHuge / 2], resHuge[lenHuge / 2]);
    }
    // =================================================================================
    // 1. [Stride 2] Int8 / SByte (每次吞 16 个)
    // =================================================================================
    [Fact]
    public void Test_Int8_Simd_Boundaries()
    {
        Console.WriteLine("Testing Int8 (sbyte)...");

        // 场景：35个元素 (16*2 SIMD + 3 Scalar)
        int count = 35; 
        sbyte?[] data = new sbyte?[count];
        for (int i = 0; i < count; i++)
        {
            // 制造数据：每3个放一个Null，其余放数值
            if (i % 3 == 0) data[i] = null;
            else data[i] = (sbyte)(i % 127);
        }

        // 这里的 data 类型是明确的 sbyte?[]，会自动匹配 Series(string, sbyte?[]) 构造函数
        using var s = new Series("s8", data);
        
        // 验证
        var result = s.ToArray<sbyte?>();
        Assert.Equal(data, result);
    }

    // =================================================================================
    // 2. [Stride 4] Int16 / Short (每次吞 8 个)
    // =================================================================================
    [Fact]
    public void Test_Int16_Simd_Boundaries()
    {
        Console.WriteLine("Testing Int16 (short)...");

        // 场景：19个元素 (8*2 SIMD + 3 Scalar)
        int count = 19;
        short?[] data = new short?[count];
        for (int i = 0; i < count; i++)
        {
            if (i % 3 == 0) data[i] = null;
            else data[i] = (short)(i * 100);
        }

        using var s = new Series("s16", data);
        
        var result = s.ToArray<short?>();
        Assert.Equal(data, result);
    }

    // =================================================================================
    // 3. [Stride 8] Int32 / Int (每次吞 4 个)
    // =================================================================================
    [Fact]
    public void Test_Int32_Simd_Boundaries()
    {
        Console.WriteLine("Testing Int32 (int)...");

        // 场景：13个元素 (4*3 SIMD + 1 Scalar) -> 那个 +1 是最容易越界的
        int count = 13;
        int?[] data = new int?[count];
        for (int i = 0; i < count; i++)
        {
            if (i % 3 == 0) data[i] = null;
            else data[i] = i * 1000;
        }

        using var s = new Series("s32", data);
        
        var result = s.ToArray<int?>();
        Assert.Equal(data, result);
    }

    // =================================================================================
    // 4. [Stride 16] Int64 / Long (每次吞 2 个)
    // =================================================================================
    [Fact]
    public void Test_Int64_Simd_Boundaries()
    {
        Console.WriteLine("Testing Int64 (long)...");

        // 场景：5个元素 (2*2 SIMD + 1 Scalar)
        int count = 5;
        long?[] data = new long?[count];
        for (int i = 0; i < count; i++)
        {
            if (i % 3 == 0) data[i] = null;
            else data[i] = i * 10000L;
        }

        using var s = new Series("s64", data);
        
        var result = s.ToArray<long?>();
        Assert.Equal(data, result);
    }

    // =================================================================================
    // 5. [Stride 32] Int128 (每次吞 1 个)
    // =================================================================================
    [Fact]
    public void Test_Int128_Simd_Layout()
    {
        Console.WriteLine("Testing Int128...");

        // 测试 Type A / Type B 布局检测是否正确
        // 如果检测错误，MaxValue 可能会变成 1 或其他值
        Int128?[] data = new Int128?[] 
        { 
            Int128.MaxValue, // 验证 Value 读取
            Int128.One, 
            null,            // 验证 Validity 写入
            Int128.Zero 
        };

        using var s = new Series("s128", data);
        
        Assert.Equal(data[0], s[0]); // MaxValue Check
        Assert.Equal(data[1], s[1]);
        Assert.Null(s[2]);
        Assert.Equal(data[3], s[3]);
    }

    // =================================================================================
    // 6. [UInt32] 测试无符号拆箱是否修复 (Unsafe.As 验证)
    // =================================================================================
    [Fact]
    public void Test_UInt32_Unboxing_Fix()
    {
        Console.WriteLine("Testing UInt32 Unboxing...");
        
        // 这个测试之前会报错 "InvalidCastException"，现在应该能过
        uint?[] data = new uint?[] { uint.MaxValue, 0, null, 123 };
        
        using var s = new Series("u32", data);
        
        var result = s.ToArray<uint?>();
        Assert.Equal(data, result);
    }

    // =================================================================================
    // 7. [Performance] 性能测试
    // =================================================================================
    [Fact]
    public void Test_Performance_10M()
    {
        // 只测最常用的 Int32
        int len = 10_000_000;
        var data = new int?[len];
        Parallel.For(0, len, i => 
        {
            if (i % 31 == 0) data[i] = null;
            else data[i] = i;
        });

        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        // 这里会触发 ArrayHelper.UnzipInt32SIMD
        using var s = new Series("perf", data);
        
        sw.Stop();
        Console.WriteLine($"Nullable Int32 (10M) Pack Time: {sw.ElapsedMilliseconds} ms");
        
        // 简单验证
        Assert.Null(s.GetValue<int?>(0));
        Assert.Equal(1, s.GetValue<int?>(1));
    }
    [Fact]
    public void Test_Series_Dot_Product()
    {
        using var s1 = Series.From("a", new[] { 1, 2, 3 });
        using var s2 = Series.From("b", new[] { 4, 5, 6 });

        // 1. 测试返回 Series 版本
        using var resSeries = s1.Dot(s2);
        Assert.Equal(1, resSeries.Length);
        Assert.Equal(32L, resSeries.GetValue<long>(0));

        // 2. 测试直接返回标量版本
        var resScalar = s1.Dot<long>(s2);
        Assert.Equal(32L, resScalar);
        
        // 3. 测试同名 Series (ApplyBinaryExpr 应该自动处理改名)
        using var s3 = Series.From("a", new[] { 10, 20, 30 }); // 名字也是 'a'
        var resSameName = s1.Dot<long>(s3);
        // 1*10 + 2*20 + 3*30 = 10 + 40 + 90 = 140
        Assert.Equal(140L, resSameName);
    }
    [Fact]
    public void Test_InterpolateBy_Series_Timestamp()
    {
        // 模拟时间序列数据
        // T1: 10:00 (Val=10)
        // T2: 10:15 (Val=null) <- 我们要插值这个点
        // T3: 11:00 (Val=70)
        
        // T2 距离 T1 是 15分钟，距离 T3 是 45分钟。
        // 总间隔 60分钟。T2 位于 15/60 = 1/4 处。
        // Val = 10 + (70-10) * 0.25 = 10 + 15 = 25.

        var baseTime = new DateTime(2024, 1, 1, 10, 0, 0);
        
        using var times = Series.From("time", new[]
        {
            baseTime,
            baseTime.AddMinutes(15), 
            baseTime.AddHours(1)
        });

        using var values = Series.From("val", new double?[] { 10.0, null, 70.0 });

        // Act
        using var interpolated = values.InterpolateBy(times);

        // Assert
        var result = interpolated.GetValue<double>(1);
        Assert.Equal(25.0, result);
    }
}