using Apache.Arrow;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Helpers;
using static Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
namespace Polars.CSharp.Tests;

public class ExprTests
{
    [Fact]
    public void Select_Inline_Style_Pythonic()
    {
        using var csv = new DisposableFile("name,birthdate,weight,height\nQinglei,2025-11-25,70,1.80",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df.Select(
            Col("name"),
            
            Col("birthdate").Alias("b_date"),
            
            Col("birthdate").Dt.Year().Alias("year"),
            
            (Col("weight") / (Col("height") * Col("height"))).Alias("bmi")
        );

        Assert.Equal(4, res.Width);
        
        Assert.Equal("Qinglei", res.GetValue<string>(0, "name"));

        Assert.Equal(2025, res.GetValue<int>(0, "year"));

        Assert.True(res.GetValue<double>(0, "bmi") > 21.6);
        Assert.True(res.GetValue<double>(0, "bmi") < 21.7);
    }

    [Fact]
    public void Filter_By_Numeric_Value_Gt()
    {
        using var csv = new DisposableFile("val\n10\n20\n30",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df.Filter(Col("val") > 15);
        
        Assert.Equal(2, res.Height); // 20, 30
        
        Assert.Equal(20, res.GetValue<int>(0, "val"));
        Assert.Equal(30, res.GetValue<int>(1, "val"));
    }

    [Fact]
    public void Filter_By_Date_Year_Lt()
    {
        var content = @"name,birthdate,weight,height
Ben Brown,1985-02-15,72.5,1.77
Qinglei,2025-11-25,70.0,1.80
Zhang,2025-10-31,55,1.75";
        
        using var csv = new DisposableFile(content,".csv");

        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df.Filter(Col("birthdate").Dt.Year() < 1990);

        Assert.Equal(1, res.Height);
        
        Assert.Equal("Ben Brown", res.GetValue<string>(0, "name"));
    }

    [Fact]
    public void Filter_By_String_Value_Eq()
    {
        using var csv = new DisposableFile("name\nAlice\nBob\nAlice",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);
        
        using var res = df.Filter(Col("name") == Lit("Alice"));
        
        Assert.Equal(2, res.Height);
    }

    [Fact]
    public void Filter_By_Double_Value_Eq()
    {
        using var csv = new DisposableFile("value\n3.36\n4.2\n5\n3.36",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);
        
        using var res = df.Filter(Col("value") == 3.36);
        
        Assert.Equal(2, res.Height);
    }

    [Fact]
    public void Null_Handling_Works()
    {
        using var csv = new DisposableFile("age\n10\n\n30",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var filled = df
            .WithColumns(
                Col("age").FillNull(0).Alias("age_filled")
            )
            .Filter(Col("age_filled") >= 0);
            
        Assert.Equal(3, filled.Height);

        Assert.Equal(0, filled.GetValue<int>(1, "age_filled"));

        using var nulls = df.Filter(Col("age").IsNull());

        Assert.Equal(1, nulls.Height);
    }
    [Fact]
    public void IsBetween_With_DateTime_Literals()
    {
        var content = @"name,birthdate,height
Qinglei,1990-05-20,1.80
TooOld,1980-01-01,1.80
TooShort,1990-05-20,1.60";

        using var csv = new DisposableFile(content,".csv");
        
        using var df = DataFrame.ReadCsv(csv.Path, tryParseDates: true);
        
        var startDt = new DateTime(1982, 12, 31);
        var endDt = new DateTime(1996, 1, 1);

        using var res = df.Filter(

            Col("birthdate").IsBetween(Lit(startDt), Lit(endDt))
            & 
            (Col("height") > 1.7)
        );

        Assert.Equal(1, res.Height);
        
        Assert.Equal("Qinglei", res.GetValue<string>(0, "name"));
    }
    [Fact]
    public void Math_Ops_BMI_Calculation_With_Pow()
    {
        using var csv = new DisposableFile("name,height,weight\nAlice,1.65,60\nBob,1.80,80",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        var bmiExpr = (Col("weight") / Col("height").Pow(2))
            .Alias("bmi");

        using var res = df.Select(
            Col("name"),
            bmiExpr,
            Col("height").Sqrt().Alias("sqrt_h")
        );

        Assert.True(res.GetValue<double>(1, "bmi") > 24.69 && res.GetValue<double>(1, "bmi") < 24.70);

        Assert.True(res.GetValue<double>(0, "sqrt_h") > 1.28 && res.GetValue<double>(0, "sqrt_h") < 1.29);
    }
    [Fact]
    public void Test_Trigonometry_Basic()
    {
        var data = new[] { 0.0, Math.PI / 2.0, Math.PI };
        using var df = DataFrame.FromColumns(new { theta = data });

        using var res = df.Select(
            Col("theta").Sin().Alias("sin"),
            Col("theta").Cos().Alias("cos"),
            Col("theta").Tan().Alias("tan")
        );

        // Sin(0)=0, Cos(0)=1, Tan(0)=0
        Assert.Equal(0.0, (double)res["sin"][0], 1e-6);
        Assert.Equal(1.0, (double)res["cos"][0], 1e-6);
        Assert.Equal(0.0, (double)res["tan"][0], 1e-6);

        // Sin(PI/2)=1, Cos(PI/2)=0 (approx)
        Assert.Equal(1.0, (double)res["sin"][1], 1e-6);
        Assert.Equal(0.0, (double)res["cos"][1], 1e-6);
    }

    [Fact]
    public void Test_Trigonometry_Inverse()
    {
        var data = new[] { -1.0, 0.0, 1.0 };
        using var df = DataFrame.FromColumns(new { val = data });

        using var res = df.Select(
            Col("val").ArcSin().Alias("asin"),
            Col("val").ArcCos().Alias("acos")
        );

        // ArcSin(-1) = -PI/2
        Assert.Equal(-Math.PI / 2, (double)res["asin"][0], 1e-6);
        // ArcCos(1) = 0
        Assert.Equal(0.0, (double)res["acos"][2], 1e-6);
    }

    [Fact]
    public void Test_Rounding_And_Sign()
    {
        var data = new[] { -1.5, 1.5, 2.0 };
        using var df = DataFrame.FromColumns(new { val = data });

        using var res = df.Select(
            Col("val").Ceil().Alias("ceil"),   // [-1, 2, 2]
            Col("val").Floor().Alias("floor"), // [-2, 1, 2]
            Col("val").Sign().Alias("sign")    // [-1, 1, 1]
        );

        // Ceil
        Assert.Equal(-1.0, (double)res["ceil"][0]);
        Assert.Equal(2.0, (double)res["ceil"][1]);

        // Floor
        Assert.Equal(-2.0, (double)res["floor"][0]);
        Assert.Equal(1.0, (double)res["floor"][1]);

        // Sign
        Assert.Equal(-1, Convert.ToInt32(res["sign"][0])); 
        Assert.Equal(1, Convert.ToInt32(res["sign"][1]));
    }  
    // ==========================================
    // String Operations
    // ==========================================

    [Fact]
    public void String_Operations_Case_Slice_Replace()
    {
        using var csv = new DisposableFile("text\nHello World\nfoo BAR",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df.Select(
            Col("text"),
            
            Col("text").Str.ToUpper().Alias("upper"),
            
            Col("text").Str.Slice(0, 3).Alias("slice"),
            
            Col("text").Str.ReplaceAll("o", "0").Alias("replaced"),
            
            Col("text").Str.Len().Alias("len")
        );

        Assert.Equal("HELLO WORLD", res.GetValue<string>(0, "upper"));
        Assert.Equal("Hel", res.GetValue<string>(0, "slice"));
        Assert.Equal("Hell0 W0rld", res.GetValue<string>(0, "replaced"));
        
        Assert.Equal(11, res.GetValue<int>(0, "len")); 

        Assert.Equal("FOO BAR", res.GetValue<string>(1, "upper"));
        Assert.Equal("foo", res.GetValue<string>(1, "slice"));
    }

    [Fact]
    public void String_Regex_Replace_And_Extract()
    {
        using var csv = new DisposableFile("text\nUser: 12345\nID: 999",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df.Select(

            Col("text").Str.ReplaceAll(@"\d+", "#", useRegex: true).Alias("masked"),

            Col("text").Str.Extract(@"(\d+)", 1).Alias("extracted_id")
        );

        Assert.Equal("User: #", res.GetValue<string>(0, "masked"));
        
        Assert.Equal("12345", res.GetValue<string>(0, "extracted_id"));
        Assert.Equal("999", res.GetValue<string>(1, "extracted_id"));
    }
    // ==========================================
    // Temporal Ops (Components, Format, Cast)
    // ==========================================
    [Fact]
    public void Temporal_Ops_Components_Format_Cast()
    {
        var csvContent = "ts\n2023-12-25 15:30:00\n2024-01-01 00:00:00";
        using var csv = new DisposableFile(csvContent,".csv");

        using var df = DataFrame.ReadCsv(csv.Path, tryParseDates: true);

        using var res = df.Select(
            Col("ts"),
            Col("ts").Dt.Year().Alias("y"),
            Col("ts").Dt.Month().Alias("m"),
            Col("ts").Dt.Day().Alias("d"),
            Col("ts").Dt.Hour().Alias("h"),
            Col("ts").Dt.Weekday().Alias("w_day"),
            
            Col("ts").Dt.ToString("%Y/%m/%d").Alias("fmt_custom"),
            
            Col("ts").Dt.Date().Alias("date_only")
        );

        // --- Row 0: 2023-12-25 15:30:00 ---

        Assert.Equal(2023, res.GetValue<int>(0, "y"));
        Assert.Equal(12, res.GetValue<int>(0, "m"));
        Assert.Equal(25, res.GetValue<int>(0, "d"));
        Assert.Equal(15, res.GetValue<int>(0, "h"));
        Assert.Equal(1, res.GetValue<int>(0, "w_day")); // 周一

        Assert.Equal("2023/12/25", res.GetValue<string>(0, "fmt_custom"));


        var expectedDate = new DateTime(2023, 12, 25);
        var actualDate = res.GetValue<DateTime>(0, "date_only");

        Assert.Equal(expectedDate, actualDate); 

        Assert.Equal(2024, res.GetValue<int>(1, "y"));
        Assert.Equal(1, res.GetValue<int>(1, "m"));
        Assert.Equal(0, res.GetValue<int>(1, "h"));
    }
    [Fact]
    public void Test_Dt_Ops_Advanced()
    {
        // Row 0: 10:30:55
        // Row 1: 10:45:10
        using var s = new Series("ts", ["2023-01-01 10:30:55", "2023-01-01 10:45:10"]);
        using var df = DataFrame.FromSeries(s);

        using var dfDt = df.Select(
            Col("ts").Str.ToDatetime("%Y-%m-%d %H:%M:%S").Alias("ts")
        );

        using var res = dfDt.Select(
            Col("ts"),

            // 1. Truncate "1h" -> 10:00:00
            Col("ts").Dt.Truncate(new TimeSpan(0,1,0,0)).Alias("trunc_1h"),
            
            // 2. Round "30m" -> 
            // 10:30:55 -> 10:30:00
            // 10:45:10 -> 10:30:00 (
            Col("ts").Dt.Round(new TimeSpan(0,0,30,0)).Alias("round_30m"),

            // 3. OffsetBy "1d" -> +1 day
            Col("ts").Dt.OffsetBy(TimeSpan.FromDays(1)).Alias("offset_1d"),

            // 4. Timestamp (转 Int64)
            Col("ts").Dt.Timestamp(TimeUnit.Milliseconds).Alias("ts_ms")
        );

        var t0 = res.GetValue<DateTime>(0, "trunc_1h");
        Assert.Equal(10, t0.Hour);
        Assert.Equal(0, t0.Minute);
        Assert.Equal(0, t0.Second);

        var original = res.GetValue<DateTime>(0, "ts");
        var offset = res.GetValue<DateTime>(0, "offset_1d");
        Assert.Equal(original.AddDays(1), offset);

        var tsVal = res.GetValue<long>(0, "ts_ms");
        Assert.True(tsVal > 1672531000000L); 

        Assert.Equal(DataTypeKind.Int64, res.Schema["ts_ms"].Kind);
    }
    [Fact]
    public void Test_Duration_Formatter_HighPrecision()
    {
        var us = TimeSpan.FromMicroseconds(10);
        Assert.Equal("10us", DurationFormatter.ToPolarsString(us));

        var ns = TimeSpan.FromTicks(1); 
        Assert.Equal("100ns", DurationFormatter.ToPolarsString(ns));

        var complex = new TimeSpan(0, 0, 0, 1, 500)
                      + TimeSpan.FromMicroseconds(30)
                      + TimeSpan.FromTicks(2); // 200ns
        
        Assert.Equal("1s500ms30us200ns", DurationFormatter.ToPolarsString(complex));
    }
    // ==========================================
    // Cast Ops: Int to Float, String to Int
    // ==========================================
    [Fact]
    public void Cast_Ops_Int_To_Float_String_To_Int()
    {
        using var csv = new DisposableFile("val_str,val_int\n100,10\n200,20",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df.Select(
            // 1. String -> Int64
            Col("val_str").Cast(DataType.Int64).Alias("str_to_int"),
            
            // 2. Int64 -> Float64
            Col("val_int").Cast(DataType.Float64).Alias("int_to_float")
        );

        long v1 = res.Column("str_to_int").GetValue<long>(0);
        Assert.Equal(100L, v1);

        var floatCol = res.Column("int_to_float").ToArrow() as DoubleArray; // Float64 -> DoubleArray
        Assert.NotNull(floatCol);
        
        double v2 = floatCol.GetValue(1) ?? 0.0;
        Assert.Equal(20.0, v2);
    }
    // ==========================================
    // Control Flow: IfElse (When/Then/Otherwise)
    // ==========================================
    [Fact]
    public void Control_Flow_IfElse()
    {
        using var csv = new DisposableFile("student,score\nAlice,95\nBob,70\nCharlie,50",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // if score >= 90 then "A"
        // else if score >= 60 then "Pass"
        // else "Fail"
        
        var gradeExpr = IfElse(
            Col("score") >= 90,
            Lit("A"),
            IfElse(
                Col("score") >= 60,
                Lit("Pass"),
                Lit("Fail")
            )
        ).Alias("grade");

        using var res = df
            .WithColumns(gradeExpr)
            .Sort("score", descending: true); 

        using var batch = res.ToArrow();
        var gradeCol = batch.Column("grade");

        // Alice (95) -> A
        Assert.Equal("A", gradeCol.GetStringValue(0));
        
        // Bob (70) -> Pass
        Assert.Equal("Pass", gradeCol.GetStringValue(1));
        
        // Charlie (50) -> Fail
        Assert.Equal("Fail", gradeCol.GetStringValue(2));
    }
    // ==========================================
    // Struct and Advanced List Ops
    // ==========================================
    [Fact]
    public void Struct_And_Advanced_List_Ops()
    {
        using var csv = new DisposableFile("name,score1,score2\nAlice,80,90\nBob,60,70",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // "1 5 2" -> Split -> Sort(Desc) -> First
        var maxCharExpr = Col("raw_nums").Str.Split(" ")
            .List.Sort(descending: true)
            .List.First()
            .Alias("max_char");

        using var res = df
            .WithColumns(
                AsStruct(Col("score1"), Col("score2"))
                .Alias("scores_struct")
            )
            .WithColumns(
                Col("scores_struct").Struct.Field("score1").Alias("s1_extracted")
            )
            .WithColumns(
                Lit("1 5 2").Alias("raw_nums")
            )
            .WithColumns(maxCharExpr);

        using var batch = res.ToArrow();

        // Alice score1 = 80
        Assert.Equal(80, batch.Column("s1_extracted").GetInt64Value(0));

        // List Sort + First
        // "1 5 2" -> ["1", "5", "2"] -> Sort Desc -> ["5", "2", "1"] -> First -> "5"
        Assert.Equal("5", batch.Column("max_char").GetStringValue(0));
    }
    [Fact]
    public void Test_List_Sort_Options()
    {
        using var s = Series.From("vals", [3, 1]); 
        
        using var df = DataFrame.FromColumns(new 
        {
            dummy = new[] { 1 }
        }).Select(
            Lit(3).Implode().List.Concat(LitNull().Implode()).List.Concat(Lit(1).Implode())
            .Alias("list_col")
        );
        
        // Sort (Ascending) -> null first
        // [null, 1, 3]
        using var df1 = df.Select(Col("list_col").List.Sort(descending: false, nullsLast: false));
        // 验证逻辑...

        // 2. Sort (Ascending + NullsLast)
        // [1, 3, null]
        using var df2 = df.Select(Col("list_col").List.Sort(descending: false, nullsLast: true));
        
        using var lastItem = df2.Select(Col("list_col").List.Get(-1));
        Assert.Null(lastItem["list_col"][0]);
        
        using var firstItem = df2.Select(Col("list_col").List.Get(0));
        Assert.Equal(1, (int)firstItem["list_col"][0]);
    }
    [Fact]
    public void Test_Expr_Explode_In_Select()
    {
        using var s = new Series("data", ["x,y"]);
        using var df = DataFrame.FromSeries(s);

        using var res = df.Select(
            Col("data").Str.Split(",").Explode().Alias("flat")
        );

        Assert.Equal(2, res.Height);
        Assert.Equal("x", res.GetValue<string>(0, "flat"));
        Assert.Equal("y", res.GetValue<string>(1, "flat"));
    }
    [Fact]
    public void Test_String_Strip_And_Checks()
    {

        using var s = new Series("s", ["  hello  ", "__world__", "prefix_val_suffix"]);
        using var df = DataFrame.FromSeries(s);

        using var res = df.Select(
            // Strip Whitespace
            Col("s").Str.StripChars().Alias("strip_ws"), 
            
            // Strip Specific Chars ('_')
            Col("s").Str.StripChars("_").Alias("strip_custom"),

            // Strip Start/End
            Col("s").Str.StripCharsStart(" _").Alias("strip_start"), 
            
            // Strip Prefix/Suffix
            Col("s").Str.StripPrefix("prefix_").Str.StripSuffix("_suffix").Alias("strip_affix"),

            // StartsWith / EndsWith
            Col("s").Str.StartsWith("  h").Alias("starts_h"),
            Col("s").Str.EndsWith("__").Alias("ends_underscore")
        );

        // Strip WS ("  hello  " -> "hello")
        Assert.Equal("hello", res.GetValue<string>(0, "strip_ws"));
        
        // Strip Custom ("__world__" -> "world")
        Assert.Equal("world", res.GetValue<string>(1, "strip_custom"));

        // Strip Start ("  hello  " -> "hello  ", "__world__" -> "world__")
        Assert.Equal("hello  ", res.GetValue<string>(0, "strip_start"));
        Assert.Equal("world__", res.GetValue<string>(1, "strip_start"));

        // Strip Affix ("prefix_val_suffix" -> "val")
        Assert.Equal("val", res.GetValue<string>(2, "strip_affix"));

        // Boolean Checks
        Assert.True(res.GetValue<bool>(0, "starts_h"));     // "  hello  " starts with "  h"
        Assert.True(res.GetValue<bool>(1, "ends_underscore")); // "__world__" ends with "__"
    }

    [Fact]
    public void Test_String_To_Date_Parsing()
    {
        using var s = new Series("dates", ["2023-01-01", "2023/12/31"]);
        using var df = DataFrame.FromSeries(s);

        using var res = df.Select(
            Col("dates").Str.ToDate("%Y-%m-%d").Alias("parsed_date"),      
            Col("dates").Str.ToDatetime("%Y/%m/%d").Alias("parsed_dt")     
        );
        
        Assert.Equal(DataTypeKind.Date, res.Schema["parsed_date"].Kind);
        

        Assert.NotNull(res.GetValue<DateTime?>(0, "parsed_date")); 

        Assert.Null(res.GetValue<DateTime?>(0, "parsed_dt"));

        Assert.NotNull(res.GetValue<DateTime?>(1, "parsed_dt"));
    }
    [Fact]
    public void Test_Struct_Operations()
    {
        var df = DataFrame.From(
        [
            new { A = 1, B = 2 },
            new { A = 3, B = 4 }
        ]);

        var q = df.Lazy()
            .Select(
                AsStruct(Col("A","B"))
                    .Struct.RenameFields("First", "Second")
                    .Alias("MyStruct")
            );

        using var result = q.Collect();

        var q2 = result.Select(
            Col("MyStruct").Struct.Field(0).Alias("F0"), 
            Col("MyStruct").Struct.Field(1).Alias("F1")  
        );
        
        Assert.Equal(1, q2.GetValue<int>(0, "F0"));
        Assert.Equal(2, q2.GetValue<int>(0, "F1"));
    }
    [Fact]
    public void Test_Struct_JsonEncode()
    {
        var df = DataFrame.From(
        [
            new { Id = 1, Info = new { Name = "Alice", Age = 18 } },
            new { Id = 2, Info = new { Name = "Bob", Age = 20 } }
        ]);

        var q = df.Lazy()
            .Select(
                Col("Id"),
                Col("Info").Struct.JsonEncode().Alias("InfoJson")
            );

        using var res = q.Collect();

        var jsonSeries = res["InfoJson"];
        Assert.Equal(DataTypeKind.String, jsonSeries.DataType.Kind); 

        var jsonStr = res.GetValue<string>(0, "InfoJson");

        Assert.Contains("\"Name\":\"Alice\"", jsonStr);
        Assert.Contains("\"Age\":18", jsonStr);
        
        // Console.WriteLine(jsonStr); 
        // {"Name":"Alice","Age":18}
    }
    [Fact]
    public void Test_Window_Shift_Diff_Len()
    {
        var df = DataFrame.FromColumns(new 
        {
            Group = new[] { "A", "A", "A", "B", "B" },
            Value = new[] { 10, 20, 30, 100, 200 },
        });

        var count = df.Select(Len()).Row(0)[0];
        Assert.Equal(5u, count);

        var result = df.Select(
            Col("Group"),
            Col("Value"),
            
            Col("Value").Sum().Over("Group").Alias("GroupSum"),
            
            Col("Value").Shift(1).Alias("PrevValue"),
            
            Col("Value").Diff(1).Alias("DiffValue")
        );

        // Group A (10, 20, 30) -> Sum = 60
        Assert.Equal(60, result[0, "GroupSum"]); // A1
        Assert.Equal(60, result[2, "GroupSum"]); // A3

        // Group B (100, 200) -> Sum = 300
        Assert.Equal(300, result[3, "GroupSum"]); // B1

        Assert.Null(result[0, "PrevValue"]); 
        Assert.Equal(10, result[1, "PrevValue"]); 

        Assert.Null(result[0, "DiffValue"]);
        Assert.Equal(10, result[1, "DiffValue"]); // 20 - 10 = 10
        Assert.Equal(100, result[4, "DiffValue"]); // 200 - 100 = 100
    }
    [Fact]
    public void Test_AddBusinessDays_SupplyChain_Scenario()
    {
        // 2024-01-05 is Friday
        var startDate = new DateOnly(2024, 1, 5); 
        
        using var df = DataFrame.FromColumns(new 
        {
            OrderDate = new[] { startDate }
        });

        // Friday -> (Skip Saturday, Sunday) -> Monday(+1) -> Tuesday(+2)
        using var res1 = df.Select(
            Col("OrderDate").Dt
                .AddBusinessDays(2) 
                .Alias("Delivery")
        );

        var delivery1 = (DateOnly)res1["Delivery"][0];
        Assert.Equal(new DateOnly(2024, 1, 9), delivery1); 

        // Friday -> (Skip Saturday, Sunday) -> (Skip Monday as Holiday) -> Tuesday(+1) -> Wednesday(+2)
        var holidays = new[] { new DateOnly(2024, 1, 8) };
        
        using var res2 = df.Select(
            Col("OrderDate").Dt
                .AddBusinessDays(2, holidays: holidays)
                .Alias("Delivery")
        );

        var delivery2 = (DateOnly)res2["Delivery"][0];
        Assert.Equal(new DateOnly(2024, 1, 10), delivery2); 
    }

    [Fact]
    public void Test_IsBusinessDay()
    {
        // 2024-01-05 (Fri), 01-06 (Sat), 01-07 (Sun), 01-08 (Mon)
        var dates = new[] 
        { 
            new DateOnly(2024, 1, 5), 
            new DateOnly(2024, 1, 6),
            new DateOnly(2024, 1, 8) 
        };

        using var df = DataFrame.FromColumns(new { Date = dates });

        // Set 01-08 as holiday
        var holidays = new[] { new DateOnly(2024, 1, 8) };

        using var res = df.Select(
            Col("Date"),
            Col("Date").Dt.IsBusinessDay(holidays: holidays).Alias("IsBiz")
        );

        // Fri -> True
        Assert.True((bool)res["IsBiz"][0]);
        // Sat -> False (Weekend)
        Assert.False((bool)res["IsBiz"][1]);
        // Mon -> False (Holiday)
        Assert.False((bool)res["IsBiz"][2]);
    }
    public class ArrayExprTests
    {
        private static DataFrame CreateArrayDataFrame()
        {
            // 原始数据: List<int>
            var lists = new[] 
            {
                [1, 2, 3],
                [10, 20, 30],
                new[] { 5, 0, 5 },
                null
            };

            using var df = DataFrame.FromColumns(new { data = lists });
            
            var targetType = DataType.Array(DataType.Int32, 3);
            
            var dfArray = df.Select(
                Col("data").Cast(targetType).Alias("arr")
            );
            
            return dfArray;
        }

        [Fact]
        public void Test_Array_Aggregations()
        {
            using var df = CreateArrayDataFrame();

            // Sum, Min, Max, Mean
            // Row 0: [1, 2, 3] -> Sum=6, Max=3
            // Row 1: [10, 20, 30] -> Sum=60, Max=30
            using var res = df.Select(
                Col("arr").Array.Sum().Alias("sum"),
                Col("arr").Array.Max().Alias("max"),
                Col("arr").Array.Min().Alias("min"),
                Col("arr").Array.Mean().Alias("mean")
            );

            Assert.Equal(6, (int)res["sum"][0]);
            Assert.Equal(3, (int)res["max"][0]);
            Assert.Equal(1, (int)res["min"][0]);
            Assert.Equal(2.0, (double)res["mean"][0]);

            Assert.Equal(60, (int)res["sum"][1]);
        }

        [Fact]
        public void Test_Array_ToStruct()
        {
            using var df = CreateArrayDataFrame();

            // Array(3) -> Struct { field_0, field_1, field_2 }
            using var res = df.Select(
                Col("arr").Array.ToStruct().Alias("struct_col")
            );

            var structCol = res["struct_col"];
            Assert.Equal(DataTypeKind.Struct, structCol.DataType.Kind);

            using var unnested = res.Unnest("struct_col");
            
            Assert.True(unnested.Columns.Contains("field_0"));
            Assert.True(unnested.Columns.Contains("field_1"));
            Assert.True(unnested.Columns.Contains("field_2"));

            Assert.Equal(1, (int)unnested["field_0"][0]);
            Assert.Equal(2, (int)unnested["field_1"][0]);
            Assert.Equal(3, (int)unnested["field_2"][0]);
        }

        [Fact]
        public void Test_Array_Get_And_Join()
        {
            using var df = CreateArrayDataFrame();

            using var res = df.Select(
                Col("arr").Array.Get(0).Alias("first"), 
                Col("arr").Array.Get(-1).Alias("last")   
            );

            Assert.Equal(1, (int)res["first"][0]);
            Assert.Equal(3, (int)res["last"][0]);
            
            Assert.Equal(10, (int)res["first"][1]);
            Assert.Equal(30, (int)res["last"][1]);
        }

        [Fact]
        public void Test_Array_Join_String()
        {
            var data = new[] { new[] { "a", "b" }, ["c", "d"] };
            using var df = DataFrame.FromColumns(new { strs = data });
            
            using var res = df.Select(
                Col("strs")
                    .Cast(DataType.Array(DataType.String, 2)) 
                    .Array.Join("-")
                    .Alias("joined")
            );

            Assert.Equal("a-b", (string)res["joined"][0]);
            Assert.Equal("c-d", (string)res["joined"][1]);
        }

        [Fact]
        public void Test_Array_Sort_And_Reverse()
        {
            // 数据: [3, 1, 2]
            var data = new[] { new[] { 3, 1, 2 } };
            using var df = DataFrame.FromColumns(new { data })
                .Select(Col("data").Cast(DataType.Array(DataType.Int32, 3)).Alias("arr"));

            using var res = df.Select(
                Col("arr").Array.Sort().Alias("sorted"),       // [1, 2, 3]
                Col("arr").Array.Reverse().Alias("reversed"),  // [2, 1, 3] 
                Col("arr").Array.ArgMin().Alias("argmin"),     // Index of 1 is 1
                Col("arr").Array.ArgMax().Alias("argmax")      // Index of 3 is 0
            );

            var sortedList = res["sorted"].GetValue<List<int>>(0); 

            Assert.NotNull(sortedList);
            Assert.Equal(1, sortedList[0]);
            Assert.Equal(2, sortedList[1]);
            Assert.Equal(3, sortedList[2]);

            var reversedList = res["reversed"].GetValue<List<int>>(0);
            Assert.NotNull(reversedList);
            Assert.Equal(2, reversedList[0]);
            Assert.Equal(1, reversedList[1]);
            Assert.Equal(3, reversedList[2]);
            // ArgMin/ArgMax
            Assert.Equal(1U, res["argmin"][0]); 
            Assert.Equal(0U, res["argmax"][0]);
        }

        [Fact]
        public void Test_Array_Boolean_Ops()
        {
            var data = new[] 
            { 
                [true, false], 
                [true, true],
                new[] { false, false }
            };
            
            using var df = DataFrame.FromColumns(new { vals = data })
                .Select(Col("vals").Cast(DataType.Array(DataType.Boolean, 2)).Alias("arr"));

            using var res = df.Select(
                Col("arr").Array.Any().Alias("any"),
                Col("arr").Array.All().Alias("all")
            );

            // Row 0: [T, F] -> Any=T, All=F
            Assert.True((bool)res["any"][0]);
            Assert.False((bool)res["all"][0]);

            // Row 1: [T, T] -> Any=T, All=T
            Assert.True((bool)res["any"][1]);
            Assert.True((bool)res["all"][1]);

            // Row 2: [F, F] -> Any=F, All=F
            Assert.False((bool)res["any"][2]);
            Assert.False((bool)res["all"][2]);
        }
        
        [Fact]
        public void Test_Array_Contains()
        {
             // [3, 1, 2]
            var data = new[] { new[] { 3, 1, 2 } };
            using var df = DataFrame.FromColumns(new { data })
                .Select(Col("data").Cast(DataType.Array(DataType.Int32, 3)).Alias("arr"));
            
            using var res = df.Select(
                Col("arr").Array.Contains(1).Alias("has_1"),
                Col("arr").Array.Contains(99).Alias("has_99")
            );
            
            Assert.True((bool)res["has_1"][0]);
            Assert.False((bool)res["has_99"][0]);
        }
        
        [Fact]
        public void Test_Array_InnerType_Property()
        {
            var arrType = DataType.Array(DataType.Float64, 5);
            
            Assert.Equal(DataTypeKind.Array, arrType.Kind);

            Assert.Equal(DataTypeKind.Float64, arrType.InnerType.Kind);

            Assert.Equal(5UL, arrType.ArrayWidth);
        }
        [Fact]
        public void Test_Series_Array_ReadItem()
        {
            // Array(Int32, 3)
            // Row 0: [1, 2, 3]
            // Row 1: [10, 20, 30]
            // Row 2: [5, 0, 5]
            // Row 3: null
            using var df = CreateArrayDataFrame(); 
            var series = df["arr"]; 

            var listVal = series.GetValue<List<int>>(0);
            Assert.NotNull(listVal);
            Assert.Equal(3, listVal.Count);
            Assert.Equal(1, listVal[0]);
            Assert.Equal(2, listVal[1]);
            Assert.Equal(3, listVal[2]);

            var arrayVal = series.GetValue<int[]>(1);
            Assert.NotNull(arrayVal);
            Assert.Equal(3, arrayVal.Length);
            Assert.Equal(10, arrayVal[0]);
            Assert.Equal(20, arrayVal[1]);
            Assert.Equal(30, arrayVal[2]);
            
            var nullVal = series.GetValue<List<int>>(3);
            Assert.Null(nullVal);
            
            using var dfFloat = DataFrame.FromColumns(new 
            { 
                v = new[] { [1.1, 2.2], new[] { 3.3, 4.4 } } 
            }).Select(Col("v").Cast(DataType.Array(DataType.Float64, 2)).Alias("vec"));
            
            var vec = dfFloat["vec"].GetValue<double[]>(0);
            Assert.Equal(1.1, vec[0], 1);
            Assert.Equal(2.2, vec[1], 1);
        }
        [Fact]
        public void Test_Expr_TopK()
        {
            var data = new[] { 10, 5, 8, 100, 1, 99 };
            using var df = DataFrame.FromColumns(new { val = data });

            using var top = df.Select(Col("val").TopK(2).Alias("top"));
            
            using var bottom = df.Select(Col("val").BottomK(2).Alias("bottom"));

            Assert.Equal(2, top.Height);

            var topList = top["top"].ToArray<int>();
            Assert.Contains(100, topList);
            Assert.Contains(99, topList);

            var bottomList = bottom["bottom"].ToArray<int>();
            Assert.Contains(1, bottomList);
            Assert.Contains(5, bottomList);
        }
        [Fact]
        public void Test_Expr_TopKBy_MultiColumn()
        {
            // Group: [A, A, B, B]
            // Value: [1, 2, 3, 4]
            // Score: [10, 20, 10, 20]
            
            var group = new[] { "A", "A", "B", "B" };
            var val = new[] { 1, 2, 3, 4 };
            var score = new[] { 10, 20, 10, 20 };

            using var df = DataFrame.FromColumns(new { group, val, score });
            
            using var res_topk = df.Select(
                Col("val").TopKBy(
                    k: 2, 
                    by: [Col("group"), Col("score")], 
                    reverse: [true, false] // Group descending, Score ascending
                ).Alias("top_k")
            );

            Assert.Equal(2, res_topk.Height);
            var list_top = res_topk["top_k"].ToArray<int>();
            
            Assert.Contains(1, list_top);
            Assert.Contains(2, list_top);

            using var res_bottomk = df.Select(
                Col("val").BottomKBy(
                    k: 2,
                    by: [Col("group"), Col("score")], 
                    reverse: [true, false] //
                ).Alias("bottom_k")
            );

            Assert.Equal(2, res_bottomk.Height);
            var list_bottom = res_bottomk["bottom_k"].ToArray<int>();
            
            Assert.Contains(3, list_bottom);
            Assert.Contains(4, list_bottom);
        }
    }
    [Fact]
    public void Test_Bitwise_Shift_Operators()
    {
        // Signed: 1, 2, -4, null
        // Unsigned: 1, 2, 0xFFFFFFFF, null
        var iData = new int?[] { 1, 2, -4, null };
        var uData = new uint?[] { 1, 2, 0xFFFFFFFF, null };

        using var df = DataFrame.FromColumns(new 
        { 
            i_val = iData,
            u_val = uData
        });

        using var res = df.Select(
            // 1 << 1 = 2
            (Col("i_val") << 1).Alias("i_shl"),
            
            // -4 >> 1 = -2 
            (Col("i_val") >> 1).Alias("i_shr"),

            // 0xFFFFFFFF >> 4 = 0x0FFFFFFF 
            (Col("u_val") >> 4).Alias("u_shr")
        );

        // 1 << 1 = 2
        Assert.Equal(2, res.GetValue<int>(0, "i_shl"));
        // null << 1 = null
        Assert.Null(res.GetValue<int?>(3, "i_shl"));

        // -4 (111...100) >> 1 = -2 (111...110)
        Assert.Equal(-2, res.GetValue<int>(2, "i_shr"));

        // UInt Max >> 4
        uint expected = 0xFFFFFFFF >> 4;
        Assert.Equal(expected, res.GetValue<uint>(2, "u_shr"));
    }
    [Fact]
    public void Test_Product()
    {
        // 1 * 2 * 3 * 4 = 24
        using var df = DataFrame.FromColumns(new { nums = new[] { 1, 2, 3, 4 } });

        var result = df.Select(Col("nums").Product());
        
        var val = result["nums"][0];
        Assert.Equal(24L, val);
    }
    [Fact]
    public void Test_Skew()
    {
        // [1, 2, 3] -> Skew 0
        // [1, 1, 10] -> Skew > 0
        using var df = DataFrame.FromColumns(new { 
            sym = new[] { 1.0, 2.0, 3.0 }, 
            skewed = new[] { 1.0, 1.0, 10.0 } 
        });

        // Test 1: Symmetric -> 0
        using var res1 = df.Select(Col("sym").Skew(bias: true));
        Assert.Equal(0.0, (double)res1["sym"][0], precision: 6);

        // Test 2: Skewed -> Positive
        using var res2 = df.Select(Col("skewed").Skew(bias: false));
        Assert.True((double)res2["skewed"][0] > 0);
    }
    [Fact]
    public void Test_Kurtosis()
    {
        using var df = DataFrame.FromColumns(new { data = new[] { 1.0, 2.0, 3.0, 4.0, 5.0 } });

        // Fisher = true (Normal = 0)
        // Fisher = false (Pearson, Normal = 3)
        using var q = df.Select(
            Col("data").Kurtosis(fisher: true, bias: false).Alias("k_fisher"),
            Col("data").Kurtosis(fisher: false, bias: false).Alias("k_pearson")
        );

        var kFisher = (double)q["k_fisher"][0];
        var kPearson = (double)q["k_pearson"][0];

        // Pearson = Fisher + 3
        Assert.Equal(3.0, kPearson - kFisher, precision: 6);
    }
    [Fact]
    public void Test_PctChange()
    {
        // Data: [10, 20, 40, 40]
        // n=1: [null, 1.0, 1.0, 0.0]
        using var df = DataFrame.FromColumns(new { a = new[] { 10.0, 20.0, 40.0, 40.0 } });

        using var res = df.Select(Col("a").PctChange(n: 1));
        
        Assert.Null(res["a"][0]); 
        Assert.Equal(1.0, (double)res["a"][1]);
        Assert.Equal(1.0, (double)res["a"][2]);
        Assert.Equal(0.0, (double)res["a"][3]);
    }

    [Fact]
    public void Test_Rank_Methods()
    {
        // Data with ties: [10, 20, 20, 30]
        // Indices:         0   1   2   3
        using var df = DataFrame.FromColumns(new { v = new[] { 10, 20, 20, 30 } });

        // Average (Default): 20->2.5
        using var qAvg = df.Select(Col("v").Rank(RankMethod.Average).Alias("rank"));
        Assert.Equal(2.5, (double)qAvg["rank"][1]);

        // Min: 20->2
        using var qMin = df.Select(Col("v").Rank(RankMethod.Min).Alias("rank"));

        Assert.Equal(2.0, Convert.ToDouble(qMin["rank"][1]));
        Assert.Equal(4.0, Convert.ToDouble(qMin["rank"][3])); 

        // Dense: 20->2, 30->3
        using var qDense = df.Select(Col("v").Rank(RankMethod.Dense).Alias("rank"));
        Assert.Equal(2.0, Convert.ToDouble(qDense["rank"][1]));
        Assert.Equal(3.0, Convert.ToDouble(qDense["rank"][3])); // 紧接3
        
        // Descending
        using var qDesc = df.Select(Col("v").Rank(RankMethod.Min, descending: true));
        // 30->1, 20->2, 10->4
        Assert.Equal(1.0, Convert.ToDouble(qDesc["v"][3])); // value 30 is rank 1
    }
    [Fact]
    public void Test_CumSum_Prod()
    {
        // Data: [1, 2, 3, 4]
        using var df = DataFrame.FromColumns(new { v = new[] { 1, 2, 3, 4 } });

        // --- CumSum ---
        // Forward: [1, 3, 6, 10]
        // Reverse: [10, 9, 7, 4]  (4, 4+3, 4+3+2, ...)
        using var qSum = df.Select(
            Col("v").CumSum().Alias("fwd"),
            Col("v").CumSum(reverse: true).Alias("rev")
        );

        Assert.Equal(6, qSum["fwd"][2]); // 1+2+3
        Assert.Equal(9, qSum["rev"][1]); // 4+3+2

        // --- CumProd ---
        // Forward: [1, 2, 6, 24]
        // Reverse: [24, 24, 12, 4] (1*2*3*4, 2*3*4, 3*4, 4)
        using var qProd = df.Select(
            Col("v").CumProd().Alias("fwd"),
            Col("v").CumProd(reverse: true).Alias("rev")
        );
        Assert.Equal(6L, qProd["fwd"][2]); // 1*2*3
        Assert.Equal(12L, qProd["rev"][2]); // 3*4
    }

    [Fact]
    public void Test_CumMin_Max()
    {
        // Data: [1, 5, 2, 4, 3]
        using var df = DataFrame.FromColumns(new { v = new[] { 1, 5, 2, 4, 3 } });

        // --- CumMax ---
        // Forward: [1, 5, 5, 5, 5]
        // Reverse: [5, 5, 4, 4, 3] (Max of 1..3 is 5, ..., 2..3 is 4, 4..3 is 4, 3 is 3)
        using var qMax = df.Select(
            Col("v").CumMax().Alias("fwd"),
            Col("v").CumMax(reverse: true).Alias("rev")
        );
        
        Assert.Equal(5, (int)qMax["fwd"][2]); // Max(1,5,2) -> 5
        Assert.Equal(4, (int)qMax["rev"][2]); // Max(2,4,3) -> 4

        // --- CumMin ---
        // Forward: [1, 1, 1, 1, 1]
        // Reverse: [1, 2, 2, 3, 3] (Min of 1..3 is 1, ..., 2..3 is 2, ..., 3 is 3)
        using var qMin = df.Select(
            Col("v").CumMin().Alias("fwd"),
            Col("v").CumMin(reverse: true).Alias("rev")
        );

        Assert.Equal(1, (int)qMin["fwd"][4]); 
        Assert.Equal(2, (int)qMin["rev"][2]); // Min(2,4,3) -> 2
    }

    [Fact]
    public void Test_CumCount()
    {
        // Data: [10, 20, 30]
        using var df = DataFrame.FromColumns(new { v = new[] { 10, 20, 30 } });

        using var res = df.Select(
            Col("v").CumCount(reverse: false).Alias("fwd"), // [1, 2, 3]
            Col("v").CumCount(reverse: true).Alias("rev")   // [3, 2, 1] 
        );

        // CumCount returns UInt32 
        Assert.Equal(1u, res["fwd"][0]); 
        Assert.Equal(2u, res["fwd"][1]);
        Assert.Equal(3u, res["fwd"][2]);

        // Reverse Check
        Assert.Equal(3u, res["rev"][0]); 
        Assert.Equal(1u, res["rev"][2]);
    }
    [Fact]
    public void Test_Ewm_Mean_Std_Var()
    {
        // Data: [10, 20, 40]
        using var df = DataFrame.FromColumns(new { v = new[] { 10.0, 20.0, 40.0 } });

        // --- 1. EWM Mean (alpha=0.5) ---
        // Formula (adjust=true):
        // y0 = x0 = 10
        // y1 = (x1 + (1-a)*x0) / (1 + (1-a)) = (20 + 0.5*10) / 1.5 = 25/1.5 = 16.666...
        using var resMean = df.Select(
            Col("v").EwmMean(alpha: 0.5, adjust: true).Alias("mean")
        );
        
        Assert.Equal(10.0, (double)resMean["mean"][0]);
        Assert.Equal(16.666666, (double)resMean["mean"][1], precision: 4);

        // --- 2. EWM Var & Std (bias=false, unbiased) ---
        using var resVar = df.Select(
            Col("v").EwmVar(alpha: 0.5, bias: false).Alias("var"),
            Col("v").EwmStd(alpha: 0.5, bias: false).Alias("std")
        );

        var var0 = resVar["var"][0];
        if (var0 == null) 
            Assert.Null(var0); 
        else 
            Assert.Equal(0.0, Convert.ToDouble(var0));

        Assert.True((double)resVar["var"][1] > 0);
        Assert.True((double)resVar["std"][1] > 0);
        
        // Std = Sqrt(Var)
        var v = (double)resVar["var"][2];
        var s = (double)resVar["std"][2];
        Assert.Equal(s, Math.Sqrt(v), precision: 6);
    }

    [Fact]
    public void Test_EwmMeanBy_Time()
    {
        // t0: 00:00, val=10
        // t1: 01:00, val=20 
        // t2: 03:00, val=40
        var dates = new[] {
            new DateTime(2023, 1, 1, 0, 0, 0),
            new DateTime(2023, 1, 1, 1, 0, 0),
            new DateTime(2023, 1, 1, 3, 0, 0)
        };
        
        using var df = DataFrame.FromColumns(new 
        { 
            ts = dates, 
            v = new[] { 10.0, 20.0, 40.0 } 
        });
        df.Show();

        using var res = df.Select(
            Col("v").EwmMeanBy(by: Col("ts"), halfLife: "1h").Alias("ewm_by")
        );

        // Row 0: 10
        Assert.Equal(10.0, (double)res["ewm_by"][0]);

        // Row 1 (dt=1h):
        // weight_current = 1
        // weight_prev = 0.5^1 = 0.5
        // mean = (20*1 + 10*0.5) / (1 + 0.5) = 25 / 1.5 = 16.666...
        Assert.Equal(15.0, (double)res["ewm_by"][1], precision: 4);
        
        // Row 2 (dt=2h from t1):
        // weight_current = 1
        // weight_t1 = 0.5^(2/1) = 0.25
        // weight_t0 = 0.5^(3/1) = 0.125
        // mean = (40*1 + 20*0.25 + 10*0.125) / (1 + 0.25 + 0.125) 
        //      = (40 + 5 + 1.25) / 1.375 = 46.25 / 1.375 = 33.6363...
        Assert.Equal(33.75, (double)res["ewm_by"][2], precision: 4);
    }

    [Fact]
    public void Test_EwmMeanBy_Index()
    {
        using var df = DataFrame.FromColumns(new 
        { 
            idx = new[] { 0, 1, 2 },
            v = new[] { 10.0, 20.0, 40.0 } 
        });

        using var res = df.Select(
            Col("v").EwmMeanBy(by: Col("idx"), halfLife: "1i").Alias("ewm_idx")
        );
        
        Assert.Equal(10.0, (double)res["ewm_idx"][0]);
        Assert.True((double)res["ewm_idx"][1] > 10);
    }
    [Fact]
    public void Test_Rolling_Fixed_Advanced()
    {
        // Data: [1, 2, 3, 4, 100]
        using var df = DataFrame.FromColumns(new { v = new[] { 1.0, 2.0, 3.0, 4.0, 100.0 } });

        // --- Rolling Var (Window=3) ---
        // [1, 2, 3] -> Mean=2, Var = ((1-2)^2 + (2-2)^2 + (3-2)^2) / (3-1) = 2/2 = 1.0
        // [2, 3, 4] -> Var = 1.0
        using var resVar = df.Select(
            Col("v").RollingVar(windowSize: "3i").Alias("var")
        );
        // min_periods default is usually window size or 1 depending on setup, Polars usually nulls until window full
        Assert.Null(resVar["var"][0]); 
        Assert.Equal(1.0, (double)resVar["var"][2]);
        Assert.Equal(1.0, (double)resVar["var"][3]);

        // --- Rolling Quantile (Median) with Weights ---
        // Median (0.5 quantile) is at weight 1.0 -> Value 2.
        
        double[] w = [0.5, 1.0, 0.5];
        
        using var resQuant = df.Select(
            Col("v").RollingQuantile(
                quantile: 0.5, 
                method: QuantileMethod.Linear, 
                windowSize: "3i", 
                weights: w
            ).Alias("q_weighted")
        );
        // Window [1, 2, 3] -> 1*0.25 + 2*0.75 = 1.75
        Assert.Equal(1.75, (double)resQuant["q_weighted"][2], precision: 4);
        
        // Window [3, 4, 100] -> 3*0.25 + 4*0.75 = 3.75
        Assert.Equal(3.75, (double)resQuant["q_weighted"][4], precision: 4);
        
        // --- Rolling Kurtosis ---
        using var resKurt = df.Select(
            Col("v").RollingKurtosis(windowSize: "4i").Alias("kurt")
        );
        Assert.NotNull(resKurt["kurt"][3]); // index 3 is 4th element
    }
    [Fact]
    public void Test_Rolling_Rank_Fixed()
    {
        // [10, 20, 20, 30]
        using var df = DataFrame.FromColumns(new { v = new[] { 10.0, 20.0, 20.0, 30.0 } });

        // Index 2 (Window: 10, 20, 20): Last val is 20. Rank(Average) = (2+3)/2 = 2.5
        using var res = df.Select(
            Col("v").RollingRank(windowSize: "3i", minPeriods: 3).Alias("rank")
        );

        Assert.Equal(2.5, (double)res["rank"][2]);
    }
    [Fact]
    public void Test_Rolling_By_Time_Std_Rank()
    {
        // t0: 00:00, v=0
        // t1: 00:01, v=10
        // t2: 00:02, v=10 (Diff from t0 is 2m)
        // t3: 00:05, v=20 (Diff from t2 is 3m)
        var dates = new[] {
            new DateTime(2023, 1, 1, 0, 0, 0),
            new DateTime(2023, 1, 1, 0, 1, 0),
            new DateTime(2023, 1, 1, 0, 2, 0),
            new DateTime(2023, 1, 1, 0, 5, 0)
        };

        using var df = DataFrame.FromColumns(new 
        { 
            ts = dates, 
            v = new[] { 0.0, 10.0, 10.0, 20.0 } 
        });

        // 必须转为 Polars Datetime
        var tsCol = Col("ts").Cast(DataType.Datetime(TimeUnit.Microseconds));

        // --- 1. Rolling Std By "2m" ---
        // At t2 (00:02): Window [00:00, 00:02] (Default closed="left" -> [t-w, t))
        // Closed=Left: [00:00, 00:02) -> 包含 00:00, 00:01. Values: [0, 10]. Std(0, 10) ≈ 7.07
        // Closed=Both: [00:00, 00:02] -> 包含 00:00, 00:01, 00:02. Values: [0, 10, 10]. Std(0, 10, 10) ≈ 5.77
        
        using var resStd = df.Select(
            Col("v").RollingStdBy(
                windowSize: TimeSpan.FromMinutes(2), 
                by: tsCol, 
                closed: ClosedWindow.Left
            ).Alias("std_left"),
            
            Col("v").RollingStdBy(
                windowSize: TimeSpan.FromMinutes(2), 
                by: tsCol, 
                closed: ClosedWindow.Both 
            ).Alias("std_both")
        );

        // Check t2 (Index 2)
        // Left: [0, 10]
        Assert.Equal(7.07106, (double)resStd["std_left"][2], precision: 4);
        // Both: [0, 10, 10]
        Assert.Equal(5.77350, (double)resStd["std_both"][2], precision: 4);

        // --- 2. Rolling Rank By ---
        using var resRank = df.Select(
            Col("v").RollingRankBy(
                windowSize: "3m",
                by: tsCol,
                closed: ClosedWindow.Both
            ).Alias("rank")
        );
        
        // At t2 (00:02): Window [23:59, 00:02]. Includes t0, t1, t2 -> [0, 10, 10]
        // Current value is 10 (at t2).
        // Rank logic: 0->1, 10->2.5 (Average), 10->2.5
        // Polars rolling rank typically ranks the *current row's value* within the window.
        Assert.Equal(2.5, (double)resRank["rank"][2]);
    }
    [Fact]
    public void Test_Rolling_QuantileBy_Method()
    {
        var dates = new[] {
            new DateTime(2023, 1, 1, 0, 0, 0),
            new DateTime(2023, 1, 1, 0, 1, 0)
        };
        
        // Values: [1, 2]
        using var df = DataFrame.FromColumns(new 
        { 
            ts = dates, 
            v = new[] { 1.0, 2.0 } 
        });
        var tsCol = Col("ts").Cast(DataType.Datetime(TimeUnit.Microseconds));

        // Window "2m" covers both points.
        // Compute Median (0.5) of [1, 2].
        // Method: Linear -> 1.5
        // Method: Lower -> 1
        // Method: Higher -> 2
        
        using var res = df.Select(
            Col("v").RollingQuantileBy(
                quantile: 0.5, 
                method: QuantileMethod.Linear, 
                windowSize: "2m", 
                by: tsCol,
                closed: ClosedWindow.Both
            ).Alias("q_linear"),
            
            Col("v").RollingQuantileBy(
                quantile: 0.5, 
                method: QuantileMethod.Lower, 
                windowSize: "2m", 
                by: tsCol,
                closed: ClosedWindow.Both
            ).Alias("q_lower")
        );

        Assert.Equal(1.5, (double)res["q_linear"][1]);
        Assert.Equal(1.0, (double)res["q_lower"][1]);
    }
    [Fact]
    public void Test_Temporal_Literals()
    {
        using var df = DataFrame.FromColumns(new { id = new[] { 1 } });

        // DateOnly (2024-01-01)
        var d = new DateOnly(2024, 1, 1);
        
        // TimeOnly (12:30:00)
        var t = new TimeOnly(12, 30, 0);
        
        // TimeSpan (1 hour)
        var dur = TimeSpan.FromHours(1);

        // DateTimeOffset (2024-01-01 12:00 +08:00) -> UTC 04:00
        var dto = new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.FromHours(8));

        using var res = df.Select(
            Lit(d).Alias("date"),
            Lit(t).Alias("time"),
            Lit(dur).Alias("duration"),
            Lit(dto).Alias("dto")
        );

        Assert.Equal(1, res.Height);

        var val = res.GetValue<DateTime>(0, "dto");
        Assert.Equal(new DateTime(2024, 1, 1, 4, 0, 0), val);
    }
    [Fact]
    public void Test_Decimal_Lit()
    {
        using var df = DataFrame.FromColumns(new { id = new[] { 1 } });

        decimal d1 = 12.34m;     // Unscaled: 1234, Scale: 2
        
        decimal d2 = -99.8765m;  // Unscaled: -998765, Scale: 4
        
        decimal d3 = 100m;       // Unscaled: 100, Scale: 0 

        var res = df.Select(
            Lit(d1).Alias("d1"),
            Lit(d2).Alias("d2"),
            Lit(d3).Alias("d3")
        );

        Assert.Equal(1, res.Height);
        Assert.Equal(12.34m, res.GetValue<decimal>(0, "d1"));
        Assert.Equal(-99.8765m, res.GetValue<decimal>(0, "d2"));
        Assert.Equal(100m, res.GetValue<decimal>(0, "d3"));
        
        Assert.Equal(DataType.Decimal(38,2),res[0].DataType);
        Assert.Equal(DataType.Decimal(38,4),res[1].DataType);
    }
    [Fact]
    public void Test_Expr_Lit_List()
    {
        using var df = DataFrame.FromColumns(new { id = new[] { 1, 2, 3, 4, 5 } });

        // int[]
        var listInt = new[] { 1, 3, 5 };
        var res1 = df.Select(
            Lit(listInt).Implode().List.Contains(Col("id")).Alias("is_in_int")
        );
        Assert.True(res1.GetValue<bool>(0, "is_in_int")); // 1 in [1,3,5] -> True
        Assert.False(res1.GetValue<bool>(1, "is_in_int")); // 2 in [1,3,5] -> False
        
        // string[]
        string[] listStr =["A", "B"];
        var res2 = df.Select(
            Lit(listStr).Alias("lit_str_list")
        );

        Assert.Equal("A",res2[0][0]);
        Assert.Equal("B",res2[0][1]);
    }
    [Fact]
    public void Test_Expr_IsIn()
    {
        using var df = DataFrame.FromColumns(new
        {
            id = new[] { 1, 2, 3, 4, 5 },
            name = new[] { "Alice", "Bob", "Charlie", "David", "Eve" }
        });

        var validIds = new[] { 1, 3, 5 };       
        var validNames = new[] { "Bob", "Eve" }; 

        var res = df.Select(
            Col("id").IsIn(Lit(validIds).Implode()).Alias("id_in_whitelist"),
            Col("name").IsIn(Lit(validNames).Implode()).Alias("name_in_whitelist")
        );

        Assert.Equal(5, res.Height);

        // Row 0: id=1 (In [1,3,5]) -> True
        Assert.True(res.GetValue<bool>(0, "id_in_whitelist"));
        // Row 1: id=2 (Not In [1,3,5]) -> False
        Assert.False(res.GetValue<bool>(1, "id_in_whitelist"));
        // Row 2: id=3 (In [1,3,5]) -> True
        Assert.True(res.GetValue<bool>(2, "id_in_whitelist"));

        // Row 0: Alice -> False
        Assert.False(res.GetValue<bool>(0, "name_in_whitelist"));
        // Row 1: Bob -> True
        Assert.True(res.GetValue<bool>(1, "name_in_whitelist"));
    }
    [Fact]
    public void Test_Lit_Primitives_And_Nullables()
    {
        using var df = DataFrame.FromColumns(new { _ = new[] { 0, 0, 0 } }); // Height=3

        var ints = new[] { 1, 2, 3 };
        var nullDoubles = new double?[] { 1.1, null, 3.3 };
        var bools = new[] { true, false, true };

        var res = df.Select(
            Lit(ints).Alias("i"),           
            Lit(nullDoubles).Alias("f"),    
            Lit(bools).Alias("b")           
        );

        Assert.Equal(3, res.Height);
        
        // Int Check
        Assert.Equal(1, res.GetValue<int>(0, "i"));
        
        // Nullable Double Check
        Assert.Equal(1.1, res.GetValue<double?>(0, "f"));
        Assert.Null(res.GetValue<double?>(1, "f"));
        
        // Bool Check
        Assert.False(res.GetValue<bool>(1, "b"));
    }

    [Fact]
    public void Test_Lit_Struct_Implicit()
    {
        
        using var df = DataFrame.FromColumns(new { id = new[] { 1, 2 } });

        var users = new[]
        {
            new { Name = "Alice", Age = 30 },
            new { Name = "Bob",   Age = 25 }
        };

        var res = df.Select(
            Lit(users).Alias("user_info")
        );

        /*
         Expected:
         shape: (2, 1)
         ┌───────────────┐
         │ user_info     │
         │ ---           │
         │ struct[2]     │
         ╞═══════════════╡
         │ {"Alice",30}  │
         │ {"Bob",25}    │
         └───────────────┘
        */
        
        Assert.Equal(2, res.Height);

        var dtype = res.Schema["user_info"];

        Assert.Equal(res.Schema["user_info"],DataType.Struct(["Name","Age"],[DataType.String,DataType.Int32]));
    }
    [Fact]
    public void Test_Complex_Struct_Filter()
    {

        using var df = DataFrame.FromColumns(new
        {
            id = new[] { 1, 2, 3, 4 },
            region = new[] { "US", "US", "EU", "EU" },
            dept   = new[] { "Sales", "IT", "Sales", "IT" }
        });

        var validCombinations = new[] 
        {
            new { region = "US", dept = "Sales" }, // Row 1
            new { region = "EU", dept = "IT" }     // Row 4
        };

        // AsStruct: Pack two columns into struct
        // Lit(validCombinations): Convert C# array into Series<Struct>
        // .Implode(): Pack Series<Struct> into scalar List<Struct> 
        // .IsIn(): Check whether struct is in the validCombinations
        var res = df.Select(
            Col("id"),
            AsStruct(Col("region"), Col("dept"))
                .IsIn(Lit(validCombinations).Implode()) 
                .Alias("is_valid")
        ).Filter(Col("is_valid")); 

        
        Assert.Equal(2, res.Height);
        Assert.Equal(1, res.GetValue<int>(0, "id"));
        Assert.Equal(4, res.GetValue<int>(1, "id"));
    }
   [Fact]
    public void Test_The_Grand_Loop_Read_Write()
    {
        using var df = DataFrame.FromColumns(new { _ = new[] { 0 } });

        var logs = new[]
        {
            new 
            { 
                Machine = "Server-A",
                Events = new[] 
                {
                    new { Code = 200, Msg = "OK" },
                    new { Code = 500, Msg = "Error" }
                }
            },
            new 
            { 
                Machine = "Server-B",
                Events = new[] 
                {
                    new { Code = 404, Msg = "Not Found" }
                }
            }
        };

        var dfComplex = df.Select(
            Lit(logs).Alias("log_entry")
        );
    
        // log_entry (Struct) -> Events (List) -> Get(0) (Struct) -> Msg (String)
        var res = dfComplex.Select(
            Col("log_entry").Struct.Field("Machine").Alias("host"),
            
            Col("log_entry")
                .Struct.Field("Events")
                .List.Get(0) 
                .Struct.Field("Msg") 
                .Alias("first_msg")
        );
        
        Assert.Equal(2, res.Height);

        // Row 0: Server-A, First Event is OK
        Assert.Equal("Server-A", res.GetValue<string>(0, "host"));
        Assert.Equal("OK",       res.GetValue<string>(0, "first_msg"));

        // Row 1: Server-B, First Event is Not Found
        Assert.Equal("Server-B", res.GetValue<string>(1, "host"));
        Assert.Equal("Not Found", res.GetValue<string>(1, "first_msg"));
    }

    [Fact]
    public void Test_Level2_Struct_With_List()
    {
        using var df = DataFrame.FromColumns(new { _ = new[] { 0 } });

        var users = new[]
        {
            new 
            { 
                Name = "Alice", 
                Tags = new[] { 1, 2, 3 } // <--- List<int>
            },
            new 
            { 
                Name = "Bob", 
                Tags = new int[] { }     // <--- Empty List
            }
        };

        var res = df.Select(
            Lit(users).Alias("user_tags")
        );

        /*
         shape: (2, 1)
         ┌─────────────────────┐
         │ user_tags           │
         │ ---                 │
         │ struct[2]           │
         ╞═════════════════════╡
         │ {"Alice",[1, 2, 3]} │
         │ {"Bob",[]}          │
         └─────────────────────┘
        */

        Assert.Equal(2, res.Height);
        
    }

    [Fact]
    public void Test_Level3_The_Ultimate_Nest()
    {
        // Struct 
        //  -> List 
        //      -> Struct
        using var df = DataFrame.FromColumns(new { _ = new[] { 0 } });

        var complexData = new[]
        {
            new 
            { 
                Id = 1,
                History = new[] // <--- List of Structs
                {
                    new { Date = "2023-01-01", Action = "Login" },
                    new { Date = "2023-01-02", Action = "Logout" }
                }
            },
            new 
            { 
                Id = 2,
                History = new[] 
                {
                    new { Date = "2023-01-05", Action = "Purchase" }
                }
            }
        };

        var res = df.Select(
            Lit(complexData).Alias("audit_log")
        );

        res.Show();
        
        var exploded = res.Select(
            Col("audit_log").Struct.Field("History").Explode().Alias("flat_log")
        );
        
        Assert.Equal(3, exploded.Height);
    }
    [Fact]
    public void Filter_Expression_Works()
    {
        var df = new DataFrame(
            new Series("a", Enumerable.Range(1, 10).ToArray()),
            new Series("group", [1, 1, 1, 1, 1, 2, 2, 2, 2, 2])
        );
        
        var res = df.Select(
            Col("a").Filter(Col("a") > 5).Alias("filtered_a")
        );

        var series = res["filtered_a"];

        Assert.Equal(5, series.Len());
        Assert.Equal(6, (int)series[0]);
        Assert.Equal(10, (int)series[4]);
    }

    [Fact]
    public void Filter_In_GroupBy_Works()
    {
        var df = new DataFrame(
            new Series("id", [1, 1, 1, 2, 2]),
            new Series("val", [10, 20, 30, 40, 50])
        );

        // GroupBy id
        var res = df.GroupBy("id").Agg(
            Col("val").Filter(Col("val") > 15).Mean().Alias("conditional_mean")
        ).Sort("id", false);

        // Group 1: [10, 20, 30] -> Filter(>15) -> [20, 30] -> Mean -> 25
        // Group 2: [40, 50] -> Filter(>15) -> [40, 50] -> Mean -> 45
        
        var meanCol = res["conditional_mean"];
        Assert.Equal(25.0, (double)meanCol[0]);
        Assert.Equal(45.0, (double)meanCol[1]);
    }
    [Fact]
    public void Test_Expr_Dot_Product()
    {
        // a = [1, 2, 3]
        // b = [4, 5, 6]
        using var df = DataFrame.FromColumns(new
        {
            a = new[] { 1, 2, 3 },
            b = new[] { 4, 5, 6 }
        });

        // Calculation: (1*4) + (2*5) + (3*6) = 4 + 10 + 18 = 32
        using var result = df.Select(
            Col("a").Dot(Col("b")).Alias("dot_res")
        );

        var val = result["dot_res"][0];
        Assert.Equal(32L, Convert.ToInt64(val));
    }

    [Fact]
    public void Test_Expr_Cosine_Similarity()
    {
        // Case 1: [1, 0] vs [0, 1] -> Orthogonal -> Dot=0, Cos=0
        // Case 2: [1, 1] vs [2, 2] -> Parallel -> Cos=1
        
        using var df = DataFrame.FromColumns(new
        {
            // Vector A
            a1 = new[] { 1.0, 0.0 }, 
            a2 = new[] { 0.0, 1.0 },
            
            // Vector B
            b1 = new[] { 1.0, 1.0 },
            b2 = new[] { 2.0, 2.0 }
        });

        // Cos(A, B) = (A . B) / (|A| * |B|)
        // |A| = Sqrt(Sum(A^2))
        static Expr L2Norm(Expr e) => e.Pow(2).Sum().Sqrt();
        static Expr Cosine(Expr a, Expr b) => a.Dot(b) / (L2Norm(a) * L2Norm(b));

        using var res = df.Select(
            Cosine(Col("a1"), Col("a2")).Alias("ortho"), // [1,0] . [0,1]
            Cosine(Col("b1"), Col("b2")).Alias("parallel") // [1,1] . [2,2]
        );

        var ortho = (double?)res["ortho"][0];
        var parallel = (double?)res["parallel"][0];

        Assert.True(Math.Abs(ortho.Value - 0.0) < 1e-6, $"Expected 0.0, got {ortho}");
        Assert.True(Math.Abs(parallel.Value - 1.0) < 1e-6, $"Expected 1.0, got {parallel}");
    }

    [Fact]
    [Trait("Expr","DotProduct")]
    public void Test_Dot_Product_With_Nulls()
    {
        using var df = DataFrame.FromColumns(new
        {
            a = new[] { 1, 2, null as int? },
            b = new[] { 4, 5, 10 }
        });

        // (1*4) + (2*5) + (null*10) = 4 + 10 + ? 
        
        using var result = df.Select(Col("a").Dot(Col("b")));

        var val = result[0][0];
        
        Assert.Equal(14L, Convert.ToInt64(val));
    }
    [Fact]
    public void Test_Interpolate()
    {
        using var df = DataFrame.FromColumns(new
        {
            // 1, null, null, 4, 5
            val = new double?[] { 1.0, null, null, 4.0, 5.0 } 
        });

        // Linear Interpolation (Default)
        // 1.0 -> (2.0) -> (3.0) -> 4.0 -> 5.0
        using var linear = df.Select(Col("val").Interpolate().Alias("linear"));
        var linearArr = linear["linear"].ToArray<double?>();
        
        Assert.Equal(2.0, linearArr[1]);
        Assert.Equal(3.0, linearArr[2]);

        // Nearest Interpolation
        // 1.0 -> (1.0) -> (4.0) -> 4.0 -> 5.0  (depends on strategy, usually rounds to nearest valid)
        using var nearest = df.Select(Col("val").Interpolate(InterpolationMethod.Nearest).Alias("nearest"));
        var nearestArr = nearest["nearest"].ToArray<double?>();
        
        Assert.NotNull(nearestArr[1]);
        Assert.NotNull(nearestArr[2]);
    }
    [Fact]
    public void Test_InterpolateBy_Expr()
    {
        // Time: 0, 10, 20 
        // Position: 0,      2,          10
        // Value:    0,      ?,          100
        //
        // Linear (Index based): Index 1 is exactly between Index 0 and Index 2.
        //      Result = (0 + 100) / 2 = 50
        //
        // InterpolateBy (Position based):
        //      Total dist = 10 - 0 = 10
        //      Current dist = 2 - 0 = 2
        //      Ratio = 2 / 10 = 0.2
        //      Result = 0 + (100 - 0) * 0.2 = 20
        
        using var df = DataFrame.FromColumns(new
        {
            pos = new double[] { 0, 2, 10 },
            val = new double?[] { 0, null, 100 }
        });

        using var res = df.Select(
            Col("val").Interpolate().Alias("linear_index"),
            Col("val").InterpolateBy(Col("pos")).Alias("linear_pos")
        );

        var linearIndex = res["linear_index"][1];
        Assert.Equal(50.0, (double)linearIndex!);

        var linearPos = res["linear_pos"][1];
        Assert.Equal(20.0, (double)linearPos!);
    }
    [Fact]
    [Trait("Expr","SqlExpr")]
    public void Test_SqlExpr_MixedWithNativeExpr()
    {
        using var df = DataFrame.FromColumns(new
        {
            a = new int[] { 1, 2, 3 } 
        });

        using var resultDf = df.Select(
            Col("a").Alias("original_a"),                  
            SqlExpr("POWER(a, a) AS a_a"),                 
            SqlExpr("CAST(a AS VARCHAR) AS a_txt")         
        );

        var aaArr = resultDf["a_a"].ToArray<double>();
        Assert.Equal(1.0, aaArr[0]);
        Assert.Equal(4.0, aaArr[1]);
        Assert.Equal(27.0, aaArr[2]);

        var txtArr = resultDf["a_txt"].ToArray<string>();
        Assert.Equal("1", txtArr[0]);
        Assert.Equal("2", txtArr[1]);
        Assert.Equal("3", txtArr[2]);

        var origArr = resultDf["original_a"].ToArray<int>();
        Assert.Equal(1, origArr[0]);
        Assert.Equal(3, origArr[2]);
    }
    [Fact]
    [Trait("Expr","Get")]
    public void Test_Expr_Get()
    {
        // Index: 0,  1,  2,  3,  4
        // Value: 10, 20, 30, 40, 50
        using var df = DataFrame.FromColumns(new
        {
            val = new int[] { 10, 20, 30, 40, 50 }
        });

        using var res = df.Select(
            Col("val").Get(2).Alias("get_valid"),

            Col("val").Get(10, nullOnOutOfBounds: true).Alias("get_oob")
        );

        Assert.Equal(30, (int)res["get_valid"][0]!);

        Assert.Null(res["get_oob"][0]);
    }

    [Fact]
    [Trait("Expr","GatherTake")]
    public void Test_Expr_Gather_And_Take()
    {

        using var df = DataFrame.FromColumns(new
        {
            name = new string[] { "Alice", "Bob", "Charlie", "David" },
            score = new int[]   { 85,      92,    78,        95      }
        });

        using var dfIndices = DataFrame.FromColumns(new
        {
            idx_take = new int[] { 0, 2, 0 } // Alice(0), Charlie(2), Alice(0)
        });

        using var res = df.Select(
            Col("name").Gather(Col("score").ArgSort(descending:true)).Alias("ranked_names"),
            
            Col("name").Take(Lit([0, 2, 0,1])).Alias("taken_names") 
        );

        var rankedNames = res["ranked_names"];
        Assert.Equal(4, rankedNames.Length);
        Assert.Equal("David",   (string)rankedNames[0]!); 
        Assert.Equal("Bob",     (string)rankedNames[1]!); 
        Assert.Equal("Alice",   (string)rankedNames[2]!); 
        Assert.Equal("Charlie", (string)rankedNames[3]!); 

        var takenNames = res["taken_names"];
        Assert.Equal(4, takenNames.Length);
        Assert.Equal("Alice",   (string)takenNames[0]!);
        Assert.Equal("Charlie", (string)takenNames[1]!);
        Assert.Equal("Alice",   (string)takenNames[2]!);
        Assert.Equal("Bob",     (string)takenNames[3]!);
    }

    [Fact]
    [Trait("Expr","GatherEvery")]
    public void Test_Expr_GatherEvery()
    {
        // Index: 0,  1,  2,  3,  4,  5,  6,  7,  8
        // Value: 0, 10, 20, 30, 40, 50, 60, 70, 80
        using var df = DataFrame.FromColumns(new
        {
            val = new int[] { 0, 10, 20, 30, 40, 50, 60, 70, 80 }
        });

        using var res = df.Select(
            Col("val").GatherEvery(n: 3, offset: 2).Alias("every_3_offset_2")
        );

        var resultCol = res["every_3_offset_2"];
        
        Assert.Equal(3, resultCol.Length);
        Assert.Equal(20, (int)resultCol[0]!);
        Assert.Equal(50, (int)resultCol[1]!);
        Assert.Equal(80, (int)resultCol[2]!);
    }
    [Fact]
    [Trait("Expr","Arg")]
    public void Test_Expr_Arg_Extremes_And_Unique()
    {
        // Index: 0, 1,  2,  3, 4
        // Value: 5, 5, 10, 10, 1
        using var df = DataFrame.FromColumns(new
        {
            val = new int[] { 5, 5, 10, 10, 1 }
        });
        // shape: (5, 1)
        // ┌─────┐
        // │ val │
        // │ --- │
        // │ i32 │
        // ╞═════╡
        // │ 5   │
        // │ 5   │
        // │ 10  │
        // │ 10  │
        // │ 1   │
        // └─────┘
        using var res = df.Select(
            Col("val").ArgMin().Alias("min_idx"),
            Col("val").ArgMax().Alias("max_idx"),
            Col("val").ArgUnique().Alias("unique_idx")
        );
        // shape: (3, 3)
        // ┌─────────┬─────────┬────────────┐
        // │ min_idx ┆ max_idx ┆ unique_idx │
        // │ ---     ┆ ---     ┆ ---        │
        // │ u32     ┆ u32     ┆ u32        │
        // ╞═════════╪═════════╪════════════╡
        // │ 4       ┆ 2       ┆ 0          │
        // │ 4       ┆ 2       ┆ 2          │
        // │ 4       ┆ 2       ┆ 4          │
        // └─────────┴─────────┴────────────┘
        Assert.Equal(4u, (uint)res["min_idx"][0]!);

        Assert.Equal(2u, (uint)res["max_idx"][0]!);

        var uniqueIdx = res["unique_idx"];
        Assert.Equal(3, uniqueIdx.Length);
        Assert.Equal(0u, (uint)uniqueIdx[0]!); 
        Assert.Equal(2u, (uint)uniqueIdx[1]!); 
        Assert.Equal(4u, (uint)uniqueIdx[2]!); 
    }
    [Fact]
    [Trait("Expr","IndexSearch")]
    public void Test_Expr_IndexOf_And_SearchSorted()
    {
        // Index:  0,  1,  2,  3,  4,  5
        // Value: 10, 20, 20, 30, 40, 50
        using var df = DataFrame.FromColumns(new
        {
            val = new int[] { 10, 20, 20, 30, 40, 50 }
        });
        // shape: (6, 1)
        // ┌─────┐
        // │ val │
        // │ --- │
        // │ i32 │
        // ╞═════╡
        // │ 10  │
        // │ 20  │
        // │ 20  │
        // │ 30  │
        // │ 40  │
        // │ 50  │
        // └─────┘
        using var res = df.Select(
            // --- IndexOf  ---
            Col("val").IndexOf(Lit(20)).Alias("idx_of_20"),
            Col("val").IndexOf(Lit(99)).Alias("idx_of_99"), 
            
            // --- SearchSorted ---
            Col("val").SearchSorted(Lit(25)).Alias("search_25"),
            
            Col("val").SearchSorted(Lit(20), side: SearchSortedSide.Left).Alias("search_20_left"),

            Col("val").SearchSorted(Lit(20), side: SearchSortedSide.Right).Alias("search_20_right")
        );
        // shape: (1, 5)
        // ┌───────────┬───────────┬───────────┬────────────────┬─────────────────┐
        // │ idx_of_20 ┆ idx_of_99 ┆ search_25 ┆ search_20_left ┆ search_20_right │
        // │ ---       ┆ ---       ┆ ---       ┆ ---            ┆ ---             │
        // │ u32       ┆ u32       ┆ u32       ┆ u32            ┆ u32             │
        // ╞═══════════╪═══════════╪═══════════╪════════════════╪═════════════════╡
        // │ 1         ┆ null      ┆ 3         ┆ 1              ┆ 3               │
        // └───────────┴───────────┴───────────┴────────────────┴─────────────────┘
        Assert.Equal(1u, (uint)res["idx_of_20"][0]!); 
        Assert.Null(res["idx_of_99"][0]);

        Assert.Equal(3u, (uint)res["search_25"][0]!); 

        Assert.Equal(1u, (uint)res["search_20_left"][0]!);
        Assert.Equal(3u, (uint)res["search_20_right"][0]!);
    }
    [Fact]
    [Trait("Expr", "StringManipulation")]
    public void Test_Expr_ConcatString_And_FormatString()
    {
        using var df = DataFrame.FromColumns(new
        {
            WordA = new string[] { "apple", "banana", null, "hello" },
            WordB = new string[] { "pie", null, "split", "world" }
        });
        // shape: (4, 2)
        // ┌────────┬───────┐
        // │ WordA  ┆ WordB │
        // │ ---    ┆ ---   │
        // │ str    ┆ str   │
        // ╞════════╪═══════╡
        // │ apple  ┆ pie   │
        // │ banana ┆ null  │
        // │ null   ┆ split │
        // │ hello  ┆ world │
        // └────────┴───────┘

        using var res = df.Select(
            // --- ConcatString (Strict: any null makes result null) ---
            Polars.ConcatString("-", false, Col("WordA"), Col("WordB")).Alias("concat_strict"),
            
            // --- ConcatString (Ignore Nulls: skips nulls seamlessly) ---
            Polars.ConcatString("-", true, Col("WordA"), Col("WordB")).Alias("concat_ignore"),
            
            // --- FormatString (Template formatting) ---
            Polars.FormatString("[{}] + [{}] = ❤️", Col("WordA"), Col("WordB")).Alias("format_str")
        );

        // shape: (4, 3)
        // ┌───────────────┬───────────────┬────────────────────────┐
        // │ concat_strict ┆ concat_ignore ┆ format_str             │
        // │ ---           ┆ ---           ┆ ---                    │
        // │ str           ┆ str           ┆ str                    │
        // ╞═══════════════╪═══════════════╪════════════════════════╡
        // │ apple-pie     ┆ apple-pie     ┆ [apple] + [pie] = ❤️   │
        // │ null          ┆ banana        ┆ null                   │
        // │ null          ┆ split         ┆ null                   │
        // │ hello-world   ┆ hello-world   ┆ [hello] + [world] = ❤️ │
        // └───────────────┴───────────────┴────────────────────────┘

        // Row 0: Full data
        Assert.Equal("apple-pie", (string)res["concat_strict"][0]!);
        Assert.Equal("apple-pie", (string)res["concat_ignore"][0]!);
        Assert.Equal("[apple] + [pie] = ❤️", (string)res["format_str"][0]!);

        // Row 1: WordB is null
        Assert.Null(res["concat_strict"][1]);
        Assert.Equal("banana", (string)res["concat_ignore"][1]!); // ignoreNulls skipped WordB
        Assert.Null(res["format_str"][1]);

        // Row 2: WordA is null
        Assert.Null(res["concat_strict"][2]);
        Assert.Equal("split", (string)res["concat_ignore"][2]!); // ignoreNulls skipped WordA
        Assert.Null(res["format_str"][2]);
        
        // Row 3: Normal strings
        Assert.Equal("hello-world", (string)res["concat_strict"][3]!);
        Assert.Equal("[hello] + [world] = ❤️", (string)res["format_str"][3]!);
    }

    [Fact]
    [Trait("Expr", "ConcatExpr")]
    public void Test_Expr_ConcatExpr()
    {
        using var df = DataFrame.FromColumns(new
        {
            Col1 = new int[] { 10, 20 },
            Col2 = new int[] { 30, 40 }
        });
        // shape: (2, 2)
        // ┌──────┬──────┐
        // │ Col1 ┆ Col2 │
        // │ ---  ┆ ---  │
        // │ i32  ┆ i32  │
        // ╞══════╪══════╡
        // │ 10   ┆ 30   │
        // │ 20   ┆ 40   │
        // └──────┴──────┘

        // ConcatExpr appends expressions/series vertically.
        // It will combine Col1 and Col2 into a single column of length 4.
        using var res = df.Select(
            ConcatExpr(rechunk: true, Col("Col1"), Col("Col2")).Alias("vertical_concat")
        );

        // shape: (4, 1)
        // ┌─────────────────┐
        // │ vertical_concat │
        // │ ---             │
        // │ i32             │
        // ╞═════════════════╡
        // │ 10              │
        // │ 20              │
        // │ 30              │
        // │ 40              │
        // └─────────────────┘

        Assert.Equal(4L, res.Height);
        
        Assert.Equal(10, (int)res["vertical_concat"][0]!);
        Assert.Equal(20, (int)res["vertical_concat"][1]!);
        Assert.Equal(30, (int)res["vertical_concat"][2]!);
        Assert.Equal(40, (int)res["vertical_concat"][3]!);
    }
    [Fact]
    [Trait("Expr", "NUnique")]
    public void Test_Expr_NUnique()
    {
        var df = DataFrame.FromSeries(
            Series.From("col1", ["Genshin","Genshin",null,null,"Chiikawa"]),
            Series.From("col2", new int?[] {12341,432123,12341,99999,null})
        );

        var result = df.Select(Cs.All().ToExpr().NUnique());
        Assert.Equal(3u,result[0][0]);
        Assert.Equal(4u,result[1][0]);
    }
}   