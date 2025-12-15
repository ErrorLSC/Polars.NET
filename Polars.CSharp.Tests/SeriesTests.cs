#nullable enable

using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.CSharp.Tests;

public class SeriesTests
{
    [Fact]
    public void Test_Series_Creation_And_Arrow()
    {
        // 1. 创建 Series (Int32)
        using var s = new Series("my_series", [1, 2, 3]);
        
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
        using var df = new DataFrame(s);

        // 5. 数据验证
        using var exploded = df.Explode(Polars.Col("arrow_list_manual"));
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
                new List<int?> { 1, 2 },
                null,
                new List<int?> { 3, null, 5 } // 甚至支持子元素 null
            };

            // 一行代码创建 Series
            using var s = Series.From("my_list", data);

            // 验证
            using var df = new DataFrame(s);
            
            // Schema 检查
            Assert.Equal(DataTypeKind.List, s.DataType.Kind);
            
            // Explode 检查
            using var exploded = df.Explode(Polars.Col("my_list"));

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
        var fields = new List<Apache.Arrow.Field>
        {
            new Apache.Arrow.Field("Name", new StringType(), false),
            new Apache.Arrow.Field("Scores", new ListType(new Int64Type()), true)
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

        using var df = new DataFrame(s);

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
            new Student { Name = "Alice", Age = 20 },
            null!, // Struct Null
            new Student { Name = "Bob", Age = 22 }
        };

        // 这一行代码，对于用户来说是极其舒适的
        using var s = Series.From("students", students);
        
        // 验证
        using var df = new DataFrame(s);
        
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

        using var df = new DataFrame(s);

        // 验证结构
        Assert.Equal(DataTypeKind.List, s.DataType.Kind);
        Assert.Equal(3, s.Length);
        Assert.Equal(1, s.NullCount);

        // 验证数据：Explode
        using var exploded = df.Explode(Polars.Col("nested_list"));
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
            new List<string> { "a", "b" },
            new List<string> { "c" }
        };
        
        using var s = Series.From("strs", data);
        Assert.Equal(DataTypeKind.List, s.DataType.Kind);
        
        // 验证内部数据
        using var df = new DataFrame(s);
        using var exp = df.Explode(Polars.Col("strs"));
        Assert.Equal("a", exp.GetValue<string>(0, "strs"));
    }
    [Fact]
    public void Test_Series_String_And_Nulls()
    {
        // 1. 创建 String Series (带 Null)
        using var s = new Series("strings", ["a", null, "c"]);
        
        Assert.Equal(3, s.Length);
        
        // Polars 0.50 默认可能是 StringViewArray 或 LargeStringArray
        // 我们用之前的扩展方法来验证
        Assert.Equal("a", s.GetValue<string>(0));
        Assert.Null(s.GetValue<string>(1));
        Assert.Equal("c", s.GetValue<string>(2));
    }

    [Fact]
    public void Test_Series_Cast_Decimal()
    {
        // 1. 创建 Double Series
        using var s = new Series("prices", [10.5, 20.0]);

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
        using var s1 = new Series("dto", data);
        Assert.Equal("dto", s1.Name);
        Assert.Equal(2, s1.Length);

        // 验证数据 (使用我们刚修好的 GetValue)
        var v0 = s1.GetValue<DateTimeOffset>(0);
        // 验证 UTC 时间点一致
        Assert.Equal(now.UtcTicks / 10 * 10, v0.UtcTicks); // 考虑微秒截断

        // 2. 测试可空构造函数
        var dataNull = new DateTimeOffset?[] { now, null };
        using var s2 = new Series("dto_null", dataNull);
        
        Assert.Equal(2, s2.Length);
        Assert.Null(s2.GetValue<DateTimeOffset?>(1));
        Assert.NotNull(s2.GetValue<DateTimeOffset?>(0));
    }
    [Fact]
    public void Test_NullCount()
    {
        // Case 1: 整数 Series (含 Null)
        using var sInt = new Series("nums", [1, null, 3, null, 5]);
        
        // 验证: 应该有 2 个 null
        Assert.Equal(2, sInt.NullCount);
        Assert.Equal(5, sInt.Length);

        // Case 2: 字符串 Series (含 Null)
        using var sStr = new Series("str", ["a", null, "b"]);
        
        // 验证: 应该有 1 个 null
        Assert.Equal(1, sStr.NullCount);
        
        // Case 3: 全是 Null
        using var sAllNull = new Series("nulls", new string?[] { null, null });
        Assert.Equal(2, sAllNull.NullCount);
        
        // Case 4: 没有 Null
        using var sClean = new Series("clean", [1, 2, 3]);
        Assert.Equal(0, sClean.NullCount);
    }
    [Fact]
    public void Test_Series_Arithmetic()
    {
        using var s1 = new Series("a", [1, 2, 3]);
        using var s2 = new Series("b", [10, 20, 30]);

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
        using var s1 = new Series("a", [1, 5, 10]);
        using var s2 = new Series("b", [1, 4, 20]);

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
        using var s = new Series("f", [1.0, double.NaN, double.PositiveInfinity]);

        // IsNan -> [false, true, false]
        using var isNan = s.IsNan();
        Assert.False(isNan.GetValue<bool>(0));
        Assert.True(isNan.GetValue<bool>(1));
        Assert.False(isNan.GetValue<bool>(2));

        // IsInfinite -> [false, false, true]
        using var isInf = s.IsInfinite();
        Assert.True(isInf.GetValue<bool>(2));
    }
}