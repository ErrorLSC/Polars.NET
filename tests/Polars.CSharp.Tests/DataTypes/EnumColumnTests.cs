using Pl = Polars.CSharp.Polars;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Helpers;
using Polars.NET.Linq.Backend.Polars;
using Polars.NET.Linq.IR;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;

namespace Polars.CSharp.Tests;

/// <summary>
/// .NET enums stored as Polars Enum columns: categories in underlying-value order, [Flags] enums as their
/// underlying integer, undefined values rejected.
/// </summary>
public class EnumColumnTests
{
    // Declaration order (Red, Green, Blue, Black) differs from value order (Black, Green, Blue, Red).
    public enum Shade { Red = 5, Green = 1, Blue = 3, Black = -2 }

    public enum Tiny : byte { High = 200, Low = 1 }

    public enum Aliased { First = 1, Primary = 1, Second = 2 }

    [Flags]
    public enum Access { None = 0, Read = 1, Write = 2 }

    public record ShadeRow(int Id, Shade Shade);
    public record OptionalShadeRow(int Id, Shade? Shade);
    public record TinyRow(Tiny Size);
    public record AccessRow(Access Access);

    private static readonly ShadeRow[] Rows =
    [
        new(1, Shade.Red), new(2, Shade.Blue), new(3, Shade.Black), new(4, Shade.Green), new(5, Shade.Blue)
    ];

    [Fact]
    public void Categories_Follow_Underlying_Value_Order_Including_Negative_Values()
    {
        Assert.Equal(["Black", "Green", "Blue", "Red"], EnumCategories.GetCategories(typeof(Shade)));
        Assert.Equal(["Low", "High"], EnumCategories.GetCategories(typeof(Tiny)));
    }

    [Fact]
    public void DataType_Enum_Uses_The_Same_Categories()
    {
        var dtype = DataType.Enum<Shade>();
        Assert.Equal(["Black", "Green", "Blue", "Red"], dtype.EnumCategories!.GetCategories());
    }

    [Fact]
    public void Members_Sharing_A_Value_Contribute_One_Category()
    {
        var categories = EnumCategories.GetCategories(typeof(Aliased));
        Assert.Equal(2, categories.Length);
        Assert.Equal(Enum.GetName(Aliased.Primary), categories[0]);
        Assert.Equal("Second", categories[1]);
    }

    [Fact]
    public void FromRows_Stores_An_Enum_Column_And_Reads_It_Back()
    {
        using var df = DataFrame.FromRows(Rows);

        var dtype = df.Schema["Shade"];
        Assert.True(dtype.IsEnum);
        Assert.Equal(["Black", "Green", "Blue", "Red"], dtype.EnumCategories!.GetCategories());
        Assert.Equal(Rows, df.Rows<ShadeRow>().ToList());
    }

    [Fact]
    public void FromRows_Streaming_Path_Stores_An_Enum_Column()
    {
        using var df = DataFrame.FromRows(Rows.Where(_ => true));

        Assert.True(df.Schema["Shade"].IsEnum);
        Assert.Equal(Rows, df.Rows<ShadeRow>().ToList());
    }

    [Fact]
    public void FromRows_Without_Rows_Still_Has_An_Enum_Column()
    {
        using var df = DataFrame.FromRows(Array.Empty<ShadeRow>());

        Assert.True(df.Schema["Shade"].IsEnum);
        Assert.Equal(0, df.Height);
    }

    [Fact]
    public void Nullable_Enum_Keeps_Nulls()
    {
        OptionalShadeRow[] rows = [new(1, Shade.Blue), new(2, null), new(3, Shade.Black)];
        using var df = DataFrame.FromRows(rows);

        Assert.True(df.Schema["Shade"].IsEnum);
        Assert.Equal(1, df["Shade"].NullCount);
        Assert.Equal(rows, df.Rows<OptionalShadeRow>().ToList());
    }

    [Fact]
    public void Series_From_Enum_Array_And_Sequence()
    {
        using var fromArray = Series.From("s", new[] { Shade.Green, Shade.Red });
        using var fromSequence = Series.From("s", new List<Shade> { Shade.Green, Shade.Red }.Select(s => s));

        Assert.True(fromArray.DataType.IsEnum);
        Assert.True(fromSequence.DataType.IsEnum);
        Assert.Equal(["Green", "Red"], fromArray.Cast(DataType.String).ToArray<string>());
        Assert.Equal(["Green", "Red"], fromSequence.Cast(DataType.String).ToArray<string>());
    }

    [Fact]
    public void Byte_Backed_Enum_Uses_Unsigned_Value_Order()
    {
        TinyRow[] rows = [new(Tiny.High), new(Tiny.Low)];
        using var df = DataFrame.FromRows(rows);
        using var sorted = df.Sort("Size");

        Assert.Equal(rows.OrderBy(r => r.Size).ToList(), sorted.Rows<TinyRow>().ToList());
    }

    [Fact]
    public void Sorting_An_Enum_Column_Agrees_With_Enum_Comparison()
    {
        using var df = DataFrame.FromRows(Rows);
        using var sorted = df.Sort("Shade");

        Assert.Equal(
            Rows.OrderBy(r => r.Shade).Select(r => r.Shade).ToList(),
            sorted.Rows<ShadeRow>().ToList().Select(r => r.Shade).ToList());
    }

    [Fact]
    public void Flags_Enum_Is_Stored_As_Its_Underlying_Integer()
    {
        AccessRow[] rows = [new(Access.Read | Access.Write), new(Access.None), new(Access.Write)];
        using var df = DataFrame.FromRows(rows);

        Assert.Equal(DataType.Int32, df.Schema["Access"]);
        Assert.Equal(rows, df.Rows<AccessRow>().ToList());
    }

    [Fact]
    public void Undefined_Enum_Value_Is_Rejected()
    {
        ShadeRow[] rows = [new(1, (Shade)42)];

        var error = Assert.Throws<ArgumentException>(() => DataFrame.FromRows(rows));
        Assert.Contains("42", error.Message);
        Assert.Contains(nameof(Shade), error.Message);
    }

    [Fact]
    public void Logical_Enum_Type_Lowers_To_A_Polars_Enum()
    {
        var logical = LogicalTypes.tryEnumOf(typeof(Shade));
        Assert.True(FSharpOption<LogicalType>.get_IsSome(logical));
        Assert.Equal(LogicalType.NewEnum(ListModule.OfArray(new[] { "Black", "Green", "Blue", "Red" })), logical!.Value);

        var dtype = new DataType(PolarsVocabulary.toDataTypeHandle(logical.Value), DataTypeKind.Enum);
        Assert.Equal(["Black", "Green", "Blue", "Red"], dtype.EnumCategories!.GetCategories());
    }

    [Fact]
    public void Logical_Enum_Type_Is_Not_Defined_For_Flags_Or_Non_Enum_Types()
    {
        Assert.True(FSharpOption<LogicalType>.get_IsNone(LogicalTypes.tryEnumOf(typeof(Access))));
        Assert.True(FSharpOption<LogicalType>.get_IsNone(LogicalTypes.tryEnumOf(typeof(int))));
    }

    // ---------------------------------------------------------------------------------------------------------
    // Arrow round trips: Polars writes an Enum's categories into the Arrow field metadata (_PL_ENUM_VALUES2);
    // importing honors it, so an Enum stays an Enum with the same categories, also nested in structs and lists.
    // ---------------------------------------------------------------------------------------------------------

    private static DataFrame NestedEnumFrame()
    {
        using var flat = DataFrame.FromRows(Rows);
        return flat.Select(
            Pl.Col("Id"),
            Pl.Col("Shade"),
            Pl.Struct(Pl.Col("Id"), Pl.Col("Shade")).Alias("Pair"),
            Pl.Col("Shade").Implode().Over("Id").Alias("Shades"));
    }

    private static void AssertSameEnumColumns(DataFrame expected, DataFrame actual)
    {
        Assert.Equal(expected.Schema.ToString(), actual.Schema.ToString());
        Assert.True(actual.Schema["Shade"].IsEnum);
        Assert.Equal(["Black", "Green", "Blue", "Red"], actual.Schema["Shade"].EnumCategories!.GetCategories());
        Assert.True(actual.Schema["Pair"].StructFields!.Single(f => f.Name == "Shade").DataType.IsEnum);
        Assert.True(actual.Schema["Shades"].InnerType!.IsEnum);
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Arrow_Record_Batch_Round_Trip_Keeps_Enum_Columns()
    {
        using var df = NestedEnumFrame();
        using var back = DataFrame.FromArrow(df.ToArrow());

        AssertSameEnumColumns(df, back);
    }

    [Fact]
    public void Arrow_Stream_Import_Keeps_Enum_Columns()
    {
        using var df = NestedEnumFrame();
        var batch = df.ToArrow();
        using var back = new DataFrame(ArrowStreamInterop.ImportEager([batch], batch.Schema));

        AssertSameEnumColumns(df, back);
    }

    [Fact]
    public void Foreign_Arrow_Stream_Import_Keeps_Enum_Columns()
    {
        using var df = NestedEnumFrame();
        var batch = df.ToArrow();

        using var ipc = new MemoryStream();
        using (var writer = new Apache.Arrow.Ipc.ArrowStreamWriter(ipc, batch.Schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }
        ipc.Position = 0;
        using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(ipc);

        // A foreign stream arrives through the Arrow C stream interface.
        unsafe
        {
            var cStream = Apache.Arrow.C.CArrowArrayStream.Create();
            try
            {
                Apache.Arrow.C.CArrowArrayStreamExporter.ExportArrayStream(reader, cStream);
                using var foreign = Apache.Arrow.C.CArrowArrayStreamImporter.ImportArrayStream(cStream);
                using var back = DataFrame.FromArrowStream(foreign);

                AssertSameEnumColumns(df, back);
            }
            finally
            {
                Apache.Arrow.C.CArrowArrayStream.Free(cStream);
            }
        }
    }
}
