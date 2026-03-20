#nullable enable
using MiniExcelLibs;

namespace Polars.CSharp.Tests;

public class ExcelTests
{
    [Fact]
    public void Test_ReadExcel_RoundTrip_With_MiniExcel()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.xlsx");

        var data = new[]
        {
            new { Id = 1, Name = "Alice", JoinDate = new DateTime(2022, 1, 1), Score = 99.5, IsActive = true,  Phone = 13800138000 },
            new { Id = 2, Name = "Bob",   JoinDate = new DateTime(2023, 5, 20),Score = 88.0, IsActive = false, Phone = 13900139000 },
            new { Id = 3, Name = "Charlie",JoinDate= new DateTime(2024, 12, 31),Score= 60.0, IsActive = true,  Phone = 13700137000 },
        };

        try
        {
            MiniExcel.SaveAs(tempFile, data);

            {
                using var df = DataFrame.ReadExcel(tempFile);

                Assert.Equal(3, df.Height);
                
                // Rust OADate -> Polars Datetime
                var dateVal = df.GetValue<DateTime>(0, "JoinDate");
                Assert.Equal(new DateTime(2022, 1, 1), dateVal);

                Assert.Equal(99.5, df.GetValue<double>(0, "Score"));
                
                Assert.True(df.GetValue<bool>(0, "IsActive"));

                Assert.Equal(13800138000, df.GetValue<double>(0, "Phone"));
            }

            {

                using var schema = new PolarsSchema();
                schema.Add("Phone", DataType.String); 

                using var df = DataFrame.ReadExcel(tempFile, schema: schema);

                Assert.Equal(DataType.String, df.Schema["Phone"]);
                
                Assert.Equal("13800138000", df.GetValue<string>(0, "Phone"));
                
                Assert.Equal(DataType.Float64, df.Schema["Score"]);
            }
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }

    [Fact]
    public void Test_ReadExcel_EmptyRows_And_Nulls()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.xlsx");

        var data = new[]
        {
            new { Col1 = (string?)"A", Col2 = (int?)1 },
            new { Col1 = (string?)null, Col2 = (int?)null },
            new { Col1 = (string?)"C", Col2 = (int?)3 },
        };

        try
        {
            MiniExcel.SaveAs(tempFile, data);

            // dropEmptyRows = true
            using var dfDropped = DataFrame.ReadExcel(tempFile, dropEmptyRows: true);
            Assert.Equal(2, dfDropped.Height); // 中间那行应该没了
            Assert.Equal("A", dfDropped.GetValue<string>(0, "Col1"));
            Assert.Equal("C", dfDropped.GetValue<string>(1, "Col1"));

            // 测试 dropEmptyRows = false
            using var dfKept = DataFrame.ReadExcel(tempFile, dropEmptyRows: false);
            Assert.Equal(3, dfKept.Height);
            Assert.Null(dfKept.GetValue<string>(1, "Col1")); 
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }
    [Fact]
    public void Test_WriteExcel_Precision_And_Formats()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.xlsx");

        try
        {
            // UInt64.MaxValue 18446744073709551615
            using var df = DataFrame.From(
            [
                new 
                { 
                    Id = 1, 
                    BigId = ulong.MaxValue, 
                    JoinDate = new DateTime(2023, 10, 1),
                    LogTime = new DateTime(2023, 10, 1, 14, 30, 0)
                }
            ]);

            df.WriteExcel(
                tempFile, 
                sheetName: "Data", 
                dateFormat: "dd/mm/yyyy",          
                datetimeFormat: "hh:mm AM/PM"      
            );

            using var dfRead = DataFrame.ReadExcel(
                tempFile, 
                schema: new PolarsSchema().Add("BigId", DataType.String) 
            );

            var bigIdStr = dfRead.GetValue<string>(0, "BigId");
            Assert.Equal(ulong.MaxValue.ToString(), bigIdStr);

            var joinDate = dfRead.GetValue<DateTime>(0, "JoinDate");
            Assert.Equal(new DateTime(2023, 10, 1), joinDate);
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }
}