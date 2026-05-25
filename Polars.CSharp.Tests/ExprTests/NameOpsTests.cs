using Cs = Polars.CSharp.Polars.Selectors;
using Pl = Polars.CSharp.Polars;
namespace Polars.CSharp.Tests;

public class ExprNameOpsTests
{
    [Fact]
    [Trait("Expr", "NameMap")]
    public void Test_Expr_Name_Map()
    {
        int[] data1 = [1, 2, 3];
        int[] data2 = [4, 5, 6];
        
        using Series s1 = Pl.CreateSeries("Old_Col_A", data1);
        using Series s2 = Pl.CreateSeries("Old_Col_B", data2);
        using DataFrame df = Pl.CreateDataFrame(s1, s2);

        using DataFrame result = df.Select(
            Pl.All().Name.Map(name => name.ToLower().Replace("old_", "new_"))
        );

        Assert.Equal(["new_col_a", "new_col_b"], result.Columns);

        Assert.Equal([1, 2, 3], result["new_col_a"].ToArray<int>());
        Assert.Equal([4, 5, 6], result["new_col_b"].ToArray<int>());
    }
    [Fact]
    [Trait("Expr", "NameMapFields")]
    public void Test_Expr_Name_MapFields()
    {
        int[] dataA = [10, 20];
        int[] dataB = [30, 40];

        using Series sA = Pl.CreateSeries("field_a", dataA);
        using Series sB = Pl.CreateSeries("field_b", dataB);
        using DataFrame df = Pl.CreateDataFrame(sA, sB);

        using DataFrame result = df.Select(
            Pl.Struct(Pl.Col("field_a"), Pl.Col("field_b"))
              .Name.MapFields(fieldName => fieldName.Replace("field_", "struct_val_").ToUpper())
              .Alias("my_struct")
        ).Unnest("my_struct");

        Assert.Equal(["STRUCT_VAL_A", "STRUCT_VAL_B"], result.Columns);

        Assert.Equal([10, 20], result["STRUCT_VAL_A"].ToArray<int>());
        Assert.Equal([30, 40], result["STRUCT_VAL_B"].ToArray<int>());
    }
    [Fact]
    [Trait("Expr", "NameCasing")]
    public void Test_Expr_Name_Casing_And_Keep()
    {
        int[] data = [1, 2, 3];
        using Series s = Pl.CreateSeries("mIxEd_CaSe_CoL", data);
        using DataFrame df = Pl.CreateDataFrame(s);

        using DataFrame upperResult = df.Select(
            Pl.Col("mIxEd_CaSe_CoL").Name.ToUppercase()
        );
        Assert.Equal(["MIXED_CASE_COL"], upperResult.Columns);
        Assert.Equal([1, 2, 3], upperResult["MIXED_CASE_COL"].ToArray<int>());

        using DataFrame lowerResult = df.Select(
            Pl.Col("mIxEd_CaSe_CoL").Name.ToLowercase()
        );
        Assert.Equal(["mixed_case_col"], lowerResult.Columns);
        Assert.Equal([1, 2, 3], lowerResult["mixed_case_col"].ToArray<int>());

        using DataFrame keepResult = df.Select(
            Pl.Col("mIxEd_CaSe_CoL").Alias("temp_name").Name.Keep()
        );

        Assert.Equal(["mIxEd_CaSe_CoL"], keepResult.Columns);
    }
    [Fact]
    [Trait("Expr", "StructFieldAffixes")]
    public void Test_Expr_Struct_Prefix_And_Suffix_Fields()
    {
        // 1. 构造基础数据
        int[] data1 = [10, 20];
        int[] data2 = [30, 40];

        using Series sA = Pl.CreateSeries("val_x", data1);
        using Series sB = Pl.CreateSeries("val_y", data2);
        using DataFrame df = Pl.CreateDataFrame(sA, sB);

        using DataFrame prefixResult = df.Select(
            Pl.Struct(Pl.Col("val_x"), Pl.Col("val_y"))
              .Name.PrefixFields("pre_")
              .Alias("my_struct")
        ).Unnest("my_struct");

        Assert.Equal(["pre_val_x", "pre_val_y"], prefixResult.Columns);

        Assert.Equal([10, 20], prefixResult["pre_val_x"].ToArray<int>());
        Assert.Equal([30, 40], prefixResult["pre_val_y"].ToArray<int>());

        using DataFrame suffixResult = df.Select(
            Pl.Struct(Pl.Col("val_x"), Pl.Col("val_y"))
              .Name.SuffixFields("_suf")
              .Alias("my_struct")
        ).Unnest("my_struct");

        Assert.Equal(["val_x_suf", "val_y_suf"], suffixResult.Columns);
        
        Assert.Equal([10, 20], suffixResult["val_x_suf"].ToArray<int>());
        Assert.Equal([30, 40], suffixResult["val_y_suf"].ToArray<int>());
    }
    [Fact]
    [Trait("Expr", "NameReplace")]
    public void Test_Expr_Name_Replace()
    {
        int[] data = [1, 2, 3];
        using Series s1 = Pl.CreateSeries("user_name_123", data);
        using Series s2 = Pl.CreateSeries("user_age_45", data);
        using DataFrame df = Pl.CreateDataFrame(s1, s2);

        using DataFrame literalResult = df.Select(
            Pl.All().Name.Replace("user", "client", literal: true)
        );
        
        Assert.Equal(["client_name_123", "client_age_45"], literalResult.Columns);
        Assert.Equal([1, 2, 3], literalResult["client_name_123"].ToArray<int>());

        using DataFrame regexResult = df.Select(
            Pl.All().Name.Replace(@"_\d+", "_num", literal: false)
        );
        
        Assert.Equal(["user_name_num", "user_age_num"], regexResult.Columns);
        Assert.Equal([1, 2, 3], regexResult["user_name_num"].ToArray<int>());
    }
}