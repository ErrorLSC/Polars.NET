using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class ConfigTests
{
    [Fact]
    [Trait("Config","ThreadPoolSize")]
    public void Test_Config_ThreadPoolSize()
    {
        Pl.Config.RestoreDefaults();
        ulong threads = Pl.ThreadPoolSize();
        Assert.True(threads>0);
        Pl.Config["POLARS_MAX_THREADS"] = "8";
        Assert.Equal(8UL,Pl.ThreadPoolSize());
        Pl.Config.RestoreDefaults();
    }
    [Fact]
    [Trait("Config", "UsingBlock")]
    public void Test_Config_TableFormat()
    {
        Pl.Config.RestoreDefaults();
        using var df = Pl.CreateSeries("nihao",["123","321"]).ToFrame();
        Pl.Config.SetTableFormatting(TableFormatting.Nothing);
        string dfS1 = df.ToString();
        using (Pl.Config.SetTableHideDataFrameShape(true).BeginScope())
        {
            Assert.Equal("1", Pl.Config["POLARS_FMT_TABLE_HIDE_DATAFRAME_SHAPE_INFORMATION"]);
            using var s = Pl.CreateSeries("byebye",[114514,1919810]);
            Pl.Config.SetTableFormatting(TableFormatting.AsciiFull);
            string dfString = s.ToFrame().ToString(); 
            Assert.DoesNotContain("shape",dfString); 
            Assert.Contains("+",dfString); 
        }
        
        Assert.Equal(dfS1,df.ToString());
        Pl.Config.RestoreDefaults();
        Assert.Contains("┘",df.ToString()); 
    }
    [Fact]
    [Trait("Config", "SaveLoad")]
    public void Test_Config_SaveLoad()
    {
        Pl.Config.RestoreDefaults();

        try
        {
            Pl.Config
                .SetThousandsSeparator(true) 
                .SetFloatPrecision(5)
                
                .SetVerbose(true)
                .SetStreamingChunkSize(65536)
                .SetEngineAffinity(Engine.Streaming);

            var dirtyStatus = Pl.Config.Status(ifSet: true);
            Assert.Contains("float_precision", dirtyStatus.Keys);
            Assert.Equal("5", dirtyStatus["float_precision"]);
            Assert.Equal("1", Pl.Config["POLARS_VERBOSE"]);

            string savedConfigPayload = Pl.Config.Save(ifSet: true);
            
            Assert.Contains("float_precision", savedConfigPayload);
            Assert.Contains("POLARS_VERBOSE", savedConfigPayload);

            Pl.Config.RestoreDefaults();

            var resetStatus = Pl.Config.Status(ifSet: true);
            Assert.DoesNotContain("float_precision", resetStatus.Keys);
            Assert.Null(Pl.Config["POLARS_VERBOSE"]);
            Assert.Null(Pl.Config["POLARS_IDEAL_MORSEL_SIZE"]);
            Assert.Null(Pl.Config["POLARS_ENGINE_AFFINITY"]);
            Assert.Null(Pl.Config["POLARS_VERBOSE"]);
            Assert.Null(Pl.Config["float_precision"]);

            Pl.Config.Load(savedConfigPayload);
            
            var restoredStatus = Pl.Config.Status(ifSet: true);
            Assert.Equal(".", restoredStatus["decimal_separator"]);
            Assert.Equal(",", restoredStatus["thousands_separator"]);
            Assert.Equal("5", restoredStatus["float_precision"]);

            Assert.Equal("1", Pl.Config["POLARS_VERBOSE"]);
            Assert.Equal("65536", Pl.Config["POLARS_IDEAL_MORSEL_SIZE"]);
            Assert.Equal("streaming", Pl.Config["POLARS_ENGINE_AFFINITY"]);
        }
        finally
        {
            Pl.Config.RestoreDefaults();
        }
    }
    [Fact]
    [Trait("Config", "SaveLoadFile")]
    public void Test_Config_SaveLoadFile()
    {
        Pl.Config.RestoreDefaults();

        string tempFilePath = Path.Combine(Path.GetTempPath(), $"polars_config_{Guid.NewGuid():N}.json");

        try
        {
            Pl.Config
                .SetThousandsSeparator(true) 
                .SetFloatPrecision(7)
                .SetVerbose(true)
                .SetStreamingChunkSize(102400);

            Pl.Config.SaveToFile(tempFilePath, ifSet: true);

            Assert.True(File.Exists(tempFilePath));
            string fileContent = File.ReadAllText(tempFilePath);
            Assert.Contains("float_precision", fileContent);
            Assert.Contains("POLARS_VERBOSE", fileContent);

            Pl.Config.RestoreDefaults();

            var resetStatus = Pl.Config.Status(ifSet: true);
            Assert.DoesNotContain("float_precision", resetStatus.Keys);
            Assert.Null(Pl.Config["POLARS_VERBOSE"]);

            Pl.Config.LoadFromFile(tempFilePath);

            var restoredStatus = Pl.Config.Status(ifSet: true);
            Assert.Equal(".", restoredStatus["decimal_separator"]);
            Assert.Equal(",", restoredStatus["thousands_separator"]);
            Assert.Equal("7", restoredStatus["float_precision"]);
            Assert.Equal("1", Pl.Config["POLARS_VERBOSE"]);
            Assert.Equal("102400", Pl.Config["POLARS_IDEAL_MORSEL_SIZE"]);
        }
        finally
        {
            var configDf = DataFrame.ReadJson(tempFilePath);

            Assert.Contains("environment", configDf.Columns);
            Assert.Contains("direct", configDf.Columns);

            var precision = configDf
                .Select(Pl.Col("direct").Struct.Field("float_precision"));
            Assert.Equal("7", precision[0][0]);

            Pl.Config.RestoreDefaults();

            if (File.Exists(tempFilePath))
            {
                File.Delete(tempFilePath);
            }
        }
    }
    [Fact]
    [Trait("Config", "Format")]
    public void Test_Config_FormatSeparators_Coverage()
    {
        Pl.Config.RestoreDefaults();

        var df = DataFrame.FromColumns(new
        {
            FloatVal = new[] { 114514.1919810 },
            IntVal   = new[] { 123456789 }
        });
        df.Show();
        using (Pl.Config.SetAsciiTables(true).BeginScope())
        {
            Assert.Equal("ASCII_FULL_CONDENSED", Pl.Config["POLARS_FMT_TABLE_FORMATTING"]); 

            string asciiString = df.ToString();

            Assert.Contains("+", asciiString);
            Assert.DoesNotContain("┌", asciiString);
        }

        using (Pl.Config.SetDecimalSeparator(',').SetThousandsSeparator('.').BeginScope())
        {
            var deStatus = Pl.Config.Status(ifSet: true);
            Assert.Equal(",", deStatus["decimal_separator"]);
            Assert.Equal(".", deStatus["thousands_separator"]);

            string deString = df.ToString();

            Assert.Contains("114.514,", deString); 
            Assert.Contains("123.456.789", deString);
        }

        using (Pl.Config.SetThousandsSeparator(true).BeginScope())
        {
            var enStatus = Pl.Config.Status(ifSet: true);
            Assert.Equal(".", enStatus["decimal_separator"]);
            Assert.Equal(",", enStatus["thousands_separator"]);

            string enString = df.ToString();
            Assert.Contains("114,514.", enString);
            Assert.Contains("123,456,789", enString);
        }

        Pl.Config.SetDecimalSeparator('$');

        Assert.Equal("$", Pl.Config["decimal_separator"]);
        string weirdString = df.ToString();
        df.Show();
        Assert.Contains("114514$19", weirdString); 

        Pl.Config.RestoreDefaults();
    }
    [Fact]
    [Trait("Config", "DecimalAndFloatFormatting")]
    public void Test_Config_DecimalAndFloatFormatting()
    {
        Pl.Config.RestoreDefaults();

        var s = Pl.CreateSeries("DecimalVal", [12.3400m]);
        var df = s.ToFrame();

        using (Pl.Config.SetTrimDecimalZeros(true).BeginScope())
        {
            Assert.Equal("1", Pl.Config["trim_decimal_zeros"]);

            string trimmedString = df.ToString();

            Assert.Contains("12.34", trimmedString);
            Assert.DoesNotContain("12.3400", trimmedString);
        }

        using (Pl.Config.SetTrimDecimalZeros(false).BeginScope())
        {
            Assert.Equal("0", Pl.Config["trim_decimal_zeros"]);

            string untrimmedString = df.ToString();

            Assert.Contains("12.3400", untrimmedString);
        }

        var dfFloat = DataFrame.FromColumns(new { BigFloat = new[] { 123456789.123 } });
        
        using (Pl.Config.SetFormatFloat(FloatFormat.Mixed).BeginScope())
        {
            var status = Pl.Config.Status(ifSet: true);
            Assert.Equal("Mixed", status["float_format"]);

            string mixedString = dfFloat.ToString();
            Assert.Contains("e8", mixedString);
        }

        using (Pl.Config.SetFormatFloat(FloatFormat.Full).BeginScope())
        {
            var status = Pl.Config.Status(ifSet: true);
            Assert.Equal("Full", status["float_format"]);

            string fullString = dfFloat.ToString();
            Assert.DoesNotContain("e+", fullString);
            Assert.Contains("123456789.", fullString);
        }

        Pl.Config.SetFormatFloat(FloatFormat.Full);
        Assert.Equal("Full", Pl.Config["float_format"]);

        Pl.Config.RestoreDefaults();
    }
    [Fact]
    [Trait("Config", "LengthFormatting")]
    public void Test_Config_LengthFormatting_Coverage()
    {
        Pl.Config.RestoreDefaults();
        
        string longStr = "原神启动";
        var dfStr = DataFrame.FromColumns(new { Text = new[] { longStr } });

        using (Pl.Config.SetFormatStringLength(2).BeginScope())
        {
            Assert.Equal("2", Pl.Config["POLARS_FMT_STR_LEN"]);

            string strOutput = dfStr.ToString();
            Assert.DoesNotContain(longStr, strOutput);
            Assert.Contains("…", strOutput); 
        }

        using (Pl.Config.SetFormatStringLength(999).BeginScope())
        {
            Assert.Equal("999", Pl.Config["POLARS_FMT_STR_LEN"]);
            string strOutput = dfStr.ToString();
            Assert.Contains(longStr, strOutput);
        }

        using var listSeries = Pl.IntRangesAsSeries(start: 0, end: 20, name: "MyList");
        var dfList = listSeries.ToFrame();

        using (Pl.Config.SetFormatTableCellListLength(0).BeginScope())
        {
            Assert.Equal("0", Pl.Config["POLARS_FMT_TABLE_CELL_LIST_LEN"]);

            string listOutput0 = dfList.ToString();
            Assert.Contains("[…]", listOutput0);
            Assert.DoesNotContain("0", listOutput0);
        }

        using (Pl.Config
            .SetFormatTableCellListLength(-1)
            .SetTableWidthChars(-1) 
            .BeginScope())
        {
            Assert.Equal("-1", Pl.Config["POLARS_FMT_TABLE_CELL_LIST_LEN"]);
            Assert.Equal("-1", Pl.Config["POLARS_TABLE_WIDTH"]);

            string listOutputAll = dfList.ToString();
            Assert.Contains("0, 1, 2, 3, 4", listOutputAll);
        }

        Pl.Config.SetTableWidthChars(120);
        Assert.Equal("120", Pl.Config["POLARS_TABLE_WIDTH"]);

        Pl.Config.RestoreDefaults();
    }
    [Fact]
    [Trait("Config", "AlignmentFormatting")]
    public void Test_Config_AlignmentFormatting_Coverage()
    {
        Pl.Config.RestoreDefaults();

        var df = DataFrame.FromColumns(new
        {
            TextCol = new[] { "abc" },
            NumCol  = new[] { 123 }
        });

        using (Pl.Config.SetTableWidthChars(-1).BeginScope())
        {
            using (Pl.Config.SetTableCellAlignment(Alignment.Right).BeginScope())
            {
                Assert.Equal("RIGHT", Pl.Config["POLARS_FMT_TABLE_CELL_ALIGNMENT"]);

                string rightOutput = df.ToString();
            }

            using (Pl.Config.SetTableCellNumericAlignment(Alignment.Left).BeginScope())
            {
                Assert.Equal("LEFT", Pl.Config["POLARS_FMT_TABLE_CELL_NUMERIC_ALIGNMENT"]);
            }
        }

        Pl.Config.SetTableCellAlignment(Alignment.Center);
        Assert.Equal("CENTER", Pl.Config["POLARS_FMT_TABLE_CELL_ALIGNMENT"]);

        Pl.Config.RestoreDefaults();
    }
    [Fact]
    [Trait("Config", "DimensionsFormatting")]
    public void Test_Config_DimensionsFormatting_Coverage()
    {
        Pl.Config.RestoreDefaults();

        var df = DataFrame.FromColumns(new
        {
            ColA = new[] { 1, 2, 3 },
            ColB = new[] { 4, 5, 6 },
            ColC = new[] { 7, 8, 9 }
        });

        using (Pl.Config.SetTableWidthChars(-1).BeginScope())
        {
 
            using (Pl.Config.SetTableRows(1).BeginScope())
            {
                Assert.Equal("1", Pl.Config["POLARS_FMT_MAX_ROWS"]);

                string rowFoldOutput = df.ToString();

                Assert.DoesNotContain("3 │ 6 │ 9", rowFoldOutput);

                Assert.Contains("…", rowFoldOutput);
            }

            using (Pl.Config.SetTableRows(-1).BeginScope())
            {
                Assert.Equal("-1", Pl.Config["POLARS_FMT_MAX_ROWS"]);

                string rowFullOutput = df.ToString();

                Assert.DoesNotContain("…", rowFullOutput);
            }

            using (Pl.Config.SetTableCols(1).BeginScope())
            {
                Assert.Equal("1", Pl.Config["POLARS_FMT_MAX_COLS"]);

                string colFoldOutput = df.ToString();

                Assert.DoesNotContain("ColC", colFoldOutput);
                Assert.Contains("…", colFoldOutput);
            }

            using (Pl.Config.SetTableCols(-1).BeginScope())
            {
                Assert.Equal("-1", Pl.Config["POLARS_FMT_MAX_COLS"]);

                string colFullOutput = df.ToString();
                Assert.Contains("ColC", colFullOutput);
            }

            using (Pl.Config.SetTableColumnDataTypeInline(true).BeginScope())
            {
                Assert.Equal("1", Pl.Config["POLARS_FMT_TABLE_INLINE_COLUMN_DATA_TYPE"]);

                string inlineOutput = df.ToString();

                Assert.Contains("ColA (i32)", inlineOutput);
            }

            using (Pl.Config.SetTableColumnDataTypeInline(false).BeginScope())
            {
                Assert.Equal("0", Pl.Config["POLARS_FMT_TABLE_INLINE_COLUMN_DATA_TYPE"]);

                string noInlineOutput = df.ToString();

                Assert.Contains("---", noInlineOutput);
                Assert.DoesNotContain("ColA (i32)", noInlineOutput);
            }
        }

        Pl.Config.SetTableRows(50);
        Assert.Equal("50", Pl.Config["POLARS_FMT_MAX_ROWS"]);

        Pl.Config.RestoreDefaults();
    }
    [Fact]
    [Trait("Config", "VisualDetailsFormatting")]
    public void Test_Config_VisualDetailsFormatting_Coverage()
    {
        Pl.Config.RestoreDefaults();

        var df = DataFrame.FromColumns(new
        {
            Name = new[] { "a", "b", "c" },
            Age  = new[] { 20, 25, 30 }
        });

        using (Pl.Config.SetTableWidthChars(-1).BeginScope())
        {

            using (Pl.Config.SetTableDataFrameShapeBelow(true).BeginScope())
            {
                Assert.Equal("1", Pl.Config["POLARS_FMT_TABLE_DATAFRAME_SHAPE_BELOW"]);

                string shapeBelowOutput = df.ToString().Trim();

                Assert.False(shapeBelowOutput.StartsWith("shape:"));

                Assert.EndsWith("(3, 2)", shapeBelowOutput);
            }

            using (Pl.Config.SetTableDataFrameShapeBelow(false).BeginScope())
            {
                Assert.Equal("0", Pl.Config["POLARS_FMT_TABLE_DATAFRAME_SHAPE_BELOW"]);

                string shapeAboveOutput = df.ToString().Trim();
                Assert.StartsWith("shape:", shapeAboveOutput);
            }

            using (Pl.Config.SetTableHideColumnDataTypes(true).BeginScope())
            {
                Assert.Equal("1", Pl.Config["POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES"]);

                string noTypesOutput = df.ToString();

                Assert.DoesNotContain("str", noTypesOutput);
                Assert.DoesNotContain("i64", noTypesOutput);
            }

            using (Pl.Config.SetTableHideColumnNames(true).BeginScope())
            {
                Assert.Equal("1", Pl.Config["POLARS_FMT_TABLE_HIDE_COLUMN_NAMES"]);

                string noNamesOutput = df.ToString();

                Assert.DoesNotContain("Name", noNamesOutput);
                Assert.DoesNotContain("Age", noNamesOutput);
            }

            using (Pl.Config.SetTableHideDataTypeSeparator(true).BeginScope())
            {
                Assert.Equal("1", Pl.Config["POLARS_FMT_TABLE_HIDE_COLUMN_SEPARATOR"]);

                string noSepOutput = df.ToString();

                Assert.DoesNotContain("---", noSepOutput);
            }

            using (Pl.Config.SetTableHideDataTypeSeparator(false).BeginScope())
            {
                Assert.Equal("0", Pl.Config["POLARS_FMT_TABLE_HIDE_COLUMN_SEPARATOR"]);
                string hasSepOutput = df.ToString();
                Assert.Contains("---", hasSepOutput);
            }
        }

        Pl.Config.SetTableHideColumnNames(false);
        Assert.Equal("0", Pl.Config["POLARS_FMT_TABLE_HIDE_COLUMN_NAMES"]);

        Pl.Config.RestoreDefaults();
    }
    [Fact]
    [Trait("Config", "SingletonGhostContamination")]
    public void Test_Config_Singleton_Snapshot_Isolation()
    {
        Pl.Config.RestoreDefaults();

        try
        {
            Pl.Config.SetFloatPrecision(9);

            Assert.Equal("9", Pl.Config["float_precision"]);

            using (Pl.Config.SetTableRows(3).BeginScope())
            {
                var currentStatus = Pl.Config.Status(ifSet: true);
                
                Assert.Equal("3", currentStatus["POLARS_FMT_MAX_ROWS"]);
                Assert.Equal("9", currentStatus["float_precision"]); 
            } 

            var postStatus = Pl.Config.Status(ifSet: true);

            Assert.DoesNotContain("POLARS_FMT_MAX_ROWS", postStatus.Keys);

            Assert.Equal("9", Pl.Config["float_precision"]);
        }
        finally
        {
            Pl.Config.RestoreDefaults();
        }
    }
}