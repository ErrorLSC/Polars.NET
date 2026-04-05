using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Helpers;
using Pl =  Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
namespace Polars.CSharp.Tests;

public class DataTypeExprTests
{
    [Fact]
    [Trait("DataTypeExpr","CollectDataType")]
    public void Test_CollectDtype_ResolvesTypeFromSchema()
    {
        using var schema = new PolarsSchema().Add("name",typeof(string));

        using var expr = Pl.Col("name");
        using var dTypeExpr = Pl.DataTypeOf(expr);

        using var resolvedType = dTypeExpr.CollectDataType(schema);
        Assert.NotNull(resolvedType);
        
        Assert.Equal(DataType.String, resolvedType);
    }
    [Fact]
    [Trait("DataTypeExpr", "CollectDataType")]
    public void Test_CollectDataType_ResolvesTypeFromSchemaContext()
    {
        using var schema = new PolarsSchema().Add("a", DataType.String);

        using var expr = Pl.Col("a");
        using var dTypeExpr = Pl.DataTypeOf(expr);

        using var resolvedType = dTypeExpr.CollectDataType(schema);

        Assert.NotNull(resolvedType);
        Assert.Equal(DataType.String, resolvedType);
    }

    [Fact]
    [Trait("DataTypeExpr", "CollectDataType")]
    public void Test_CollectDataType_ThrowsWhenContextIsMissingField()
    {
        using var schema = new PolarsSchema().Add("b", DataType.Int32);

        using var dTypeExpr = Pl.DataTypeOf(Pl.Col("a"));

        Assert.Throws<PolarsException>(() => 
        {
            dTypeExpr.CollectDataType(schema);
        });
    }
    [Fact]
    [Trait("DataTypeExpr","ToLiteral")]
    public void Test_ToLiteral_ReturnsValidDataType()
    {
        using var expectedType = DataType.Float64;
        using var dTypeExpr = expectedType.ToDataTypeExpr();

        using var actualType = dTypeExpr.ToLiteral();


        Assert.NotNull(actualType);
        Assert.Equal(expectedType, actualType);
    }

    [Fact]
    [Trait("DataTypeExpr","SelfDataType")]
    public void Test_ToLiteral_ReturnsNullForDynamicPlaceholder()
    {
        using var dynamicDTypeExpr = Pl.SelfDataType();

        var actualType = dynamicDTypeExpr.ToLiteral();

        Assert.Null(actualType);
    }
    [Fact]
    [Trait("DataTypeExpr", "DefaultValueNumeric")]
    public void Test_DefaultValue_Numeric_ActuallyProducesZero()
    {
        using var expectedType = DataType.Int32;
        using var dTypeExpr = expectedType.ToDataTypeExpr();
        
        using var expr = dTypeExpr.DefaultValue();

        using var series = Series.FromExpr(expr);

        Assert.NotNull(series);
        Assert.Equal(1, series.Length); 
        
        var val = series.First(); 
        Assert.Equal(0, val[0]);
    }
    [Fact]
    [Trait("DataTypeExpr", "DefaultValueNumericToOne")]
    public void Test_DefaultValue_NumericToOne_ReturnsOneExpression()
    {
        using var expectedType = DataType.Float64;
        using var dTypeExpr = expectedType.ToDataTypeExpr();

        using var expr = dTypeExpr.DefaultValue(numericToOne: true);

        Assert.NotNull(expr);
    }
    [Fact]
    [Trait("DataTypeExpr", "DefaultValueString")]
    public void Test_DefaultValue_String_ReturnsEmptyStringExpression()
    {
        using var expectedType = DataType.String;
        using var dTypeExpr = expectedType.ToDataTypeExpr();

        using var expr = dTypeExpr.DefaultValue();

        Assert.NotNull(expr);
    }
    [Fact]
    [Trait("DataTypeExpr", "DefaultValueList")]
    public void Test_DefaultValue_List_UsesNumListValues()
    {
        using var expectedType = DataType.List(typeof(int)); 
        using var dTypeExpr = expectedType.ToDataTypeExpr().List.InnerDataType();

        using var expr = dTypeExpr.DefaultValue(n: 1, numericToOne: false, numListValues: 3);

        Assert.NotNull(expr);
    }
    [Fact]
    [Trait("DataTypeExpr", "Display")]
    public void Test_Display_ReturnsValidFormattedExpression()
    {
        using var expectedType = DataType.Float64;
        using var dTypeExpr = expectedType.ToDataTypeExpr();

        using var displayExpr = dTypeExpr.Display();

        Assert.NotNull(displayExpr);
        
        var exprString = displayExpr.ToString();
        Assert.False(string.IsNullOrWhiteSpace(exprString));
    }

    [Fact]
    [Trait("DataTypeExpr", "InnerDataType")]
    public void Test_InnerDataType_ExtractsCorrectTypeFromList()
    {
        using var listType = DataType.List(typeof(int)); 
        using var dTypeExpr = listType.ToDataTypeExpr();

        using var innerDTypeExpr = dTypeExpr.InnerDataType();
        
        Assert.Null(innerDTypeExpr.ToLiteral());
    }

    [Fact]
    [Trait("DataTypeExpr", "Matches")]
    public void Test_Matches_ReturnsBooleanExpressionWithSelector()
    {
        using var expectedType = DataType.Float64;
        using var dTypeExpr = expectedType.ToDataTypeExpr();

        using var numericSelector = Cs.Numeric(); 

        using var matchExpr = dTypeExpr.Matches(numericSelector);

        Assert.NotNull(matchExpr);
        
        var exprString = matchExpr.ToString();
        Assert.Contains("matches", exprString);
    }
    [Fact]
    [Trait("DataTypeExpr", "WrapInList")]
    public void Test_WrapInList_WrapsTypeCorrectly()
    {
        using var baseType = DataType.Int32;
        using var dTypeExpr = baseType.ToDataTypeExpr();

        using var listExpr = dTypeExpr.WrapInList();

        using var schema = new PolarsSchema().Add("dummy", DataType.Int32);
        using var resolvedType = listExpr.CollectDataType(schema);

        using var expectedType = DataType.List(DataType.Int32); 
        Assert.NotNull(resolvedType);
        Assert.Equal(expectedType, resolvedType); 
    }

    [Fact]
    [Trait("DataTypeExpr", "WrapInArray")]
    public void Test_WrapInArray_WrapsTypeWithCorrectWidth()
    {
        using var baseType = DataType.Float64;
        using var dTypeExpr = baseType.ToDataTypeExpr();

        int arrayWidth = 5;
        using var arrayExpr = dTypeExpr.WrapInArray(arrayWidth);

        using var schema = new PolarsSchema().Add("dummy", DataType.Int32);
        using var resolvedType = arrayExpr.CollectDataType(schema);

        using var expectedType = DataType.Array(DataType.Float64, arrayWidth); 
        Assert.NotNull(resolvedType);
        Assert.Equal(expectedType, resolvedType);
    }

    [Fact]
    [Trait("DataTypeExpr", "ToSignedInteger")]
    public void Test_ToSignedInteger_ConvertsUnsignedToSigned()
    {
        using var baseType = DataType.UInt32;
        using var dTypeExpr = baseType.ToDataTypeExpr();

        using var signedExpr = dTypeExpr.ToSignedInteger();

        using var schema = new PolarsSchema().Add("dummy", DataType.Int32);
        using var resolvedType = signedExpr.CollectDataType(schema);

        Assert.NotNull(resolvedType);
        Assert.Equal(DataType.Int32, resolvedType);
    }

    [Fact]
    [Trait("DataTypeExpr", "ToUnsignedInteger")]
    public void Test_ToUnsignedInteger_ConvertsSignedToUnsigned()
    {
        using var baseType = DataType.Int64;
        using var dTypeExpr = baseType.ToDataTypeExpr();

        using var unsignedExpr = dTypeExpr.ToUnsignedInteger();

        using var schema = new PolarsSchema().Add("dummy", DataType.Int32);
        using var resolvedType = unsignedExpr.CollectDataType(schema);

        Assert.NotNull(resolvedType);
        Assert.Equal(DataType.UInt64, resolvedType);
    }
    [Fact]
    [Trait("DataTypeExpr", "ListNamespace")]
    public void Test_List_InnerDataType_ExtractsTypeCorrectly()
    {
        using var listType = DataType.List(DataType.Int64); 
        using var dTypeExpr = listType.ToDataTypeExpr();


        using var innerDTypeExpr = dTypeExpr.List.InnerDataType();

        using var schema = new PolarsSchema().Add("dummy", DataType.Int32);
        using var resolvedType = innerDTypeExpr.CollectDataType(schema);

        Assert.NotNull(resolvedType);
        Assert.Equal(DataType.Int64, resolvedType);
    }

    [Fact]
    [Trait("DataTypeExpr", "ArrayNamespace")]
    public void Test_Array_InnerDataType_ExtractsTypeCorrectly()
    {
        int arrayWidth = 4;
        using var arrayType = DataType.Array(DataType.Float32, arrayWidth);
        using var dTypeExpr = arrayType.ToDataTypeExpr();

        using var innerDTypeExpr = dTypeExpr.Array.InnerDataType();

        using var schema = new PolarsSchema().Add("dummy", DataType.Int32);
        using var resolvedType = innerDTypeExpr.CollectDataType(schema);

        Assert.NotNull(resolvedType);
        Assert.Equal(DataType.Float32, resolvedType);
    }

    [Fact]
    [Trait("DataTypeExpr", "ArrayNamespace")]
    public void Test_Array_Width_ReturnsValidExpression()
    {
        using var arrayType = DataType.Array(DataType.String, 5);
        using var dTypeExpr = arrayType.ToDataTypeExpr();

        using var widthExpr = dTypeExpr.Array.Width();

        Assert.NotNull(widthExpr);
        
        var exprString = widthExpr.ToString();
        Assert.Contains("Array(String, 5).arr.width()", exprString);
    }

    [Fact]
    [Trait("DataTypeExpr", "ArrayNamespace")]
    public void Test_Array_Shape_ReturnsValidExpression()
    {
        using var arrayType = DataType.Array(DataType.String, 2);
        using var dTypeExpr = arrayType.ToDataTypeExpr();

        using var shapeExpr = dTypeExpr.Array.Shape();

        Assert.NotNull(shapeExpr);
        
        var exprString = shapeExpr.ToString();
        Assert.Contains("Array(String, 2).arr.shape()", exprString);
    }
    [Fact]
    [Trait("DataTypeExpr", "StructNamespace")]
    public void Test_Struct_FieldNames_ReturnsValidExpression()
    {
        using var structType = DataType.Struct(
            ("Id", DataType.Int32),
            ("Name", DataType.String)
        );
        using var dTypeExpr = structType.ToDataTypeExpr();

        using var fieldNamesExpr = dTypeExpr.Struct.FieldNames();

        Assert.NotNull(fieldNamesExpr);
        
        var exprString = fieldNamesExpr.ToString();
        Assert.Contains("Struct({'Id': Int32, 'Name': String}).str", exprString);
    }

    [Fact]
    [Trait("DataTypeExpr", "StructNamespace")]
    public void Test_Struct_FieldDataTypeByName_ExtractsCorrectType()
    {
        using var structType = DataType.Struct(
            ("Id", DataType.Int32),
            ("Name", DataType.String),
            ("Score", DataType.Float64),
            ("IsActive", DataType.Boolean)
        );
        using var dTypeExpr = structType.ToDataTypeExpr();

        using var scoreFieldDTypeExpr = dTypeExpr.Struct["Score"];

        using var schema = new PolarsSchema().Add("dummy", DataType.Int32);
        using var resolvedType = scoreFieldDTypeExpr.CollectDataType(schema);

        Assert.NotNull(resolvedType);
        Assert.Equal(DataType.Float64, resolvedType);
    }

    [Fact]
    [Trait("DataTypeExpr", "StructNamespace")]
    public void Test_Struct_FieldDataTypeByIndex_ExtractsCorrectType()
    {
        using var structType = DataType.Struct(
            ("Id", DataType.Int32),
            ("Name", DataType.String),
            ("Score", DataType.Float64),
            ("IsActive", DataType.Boolean)
        );
        using var dTypeExpr = structType.ToDataTypeExpr();

        using var activeFieldDTypeExpr = dTypeExpr.Struct[3];

        using var schema = new PolarsSchema().Add("dummy", DataType.Int32);
        using var resolvedType = activeFieldDTypeExpr.CollectDataType(schema);

        Assert.NotNull(resolvedType);
        Assert.Equal(DataType.Boolean, resolvedType);
    }
}