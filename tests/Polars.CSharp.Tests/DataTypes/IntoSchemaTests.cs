using System.Text;

namespace Polars.CSharp.Tests;

/// <summary>
/// An optional <see cref="IntoSchema"/> parameter given a null variable means no schema (the conversions refused the
/// null with ArgumentNullException); a non-null source converts as before.
/// </summary>
public class IntoSchemaTests
{
    [Fact]
    public void NullSources_AreNoSchema()
    {
        PolarsSchema schema = null;
        Dictionary<string, DataType> dict = null;
        string[] names = null;
        Apache.Arrow.Schema arrow = null;
        DataFrame df = null;
        IntoSchema? fromSchema = schema, fromDict = dict, fromNames = names, fromArrow = arrow, fromFrame = df;
        Assert.Null(fromSchema);
        Assert.Null(fromDict);
        Assert.Null(fromNames);
        Assert.Null(fromArrow);
        Assert.Null(fromFrame);

        // As an optional parameter: the types are inferred.
        byte[] csv = Encoding.UTF8.GetBytes("a,b\n1,x\n");
        using DataFrame read = DataFrame.ReadCsv(csv, schema: schema, dtypeOverride: dict);
        Assert.Equal(DataType.Int64, read["a"].DataType);
    }

    [Fact]
    public void NonNullSources_StillConvert()
    {
        using var schema = PolarsSchema.From(new Dictionary<string, DataType> { ["a"] = DataType.Int8, ["b"] = DataType.String });
        PolarsSchema given = schema;
        IntoSchema? into = given;
        Assert.NotNull(into);
        byte[] csv = Encoding.UTF8.GetBytes("a,b\n1,x\n");
        using DataFrame read = DataFrame.ReadCsv(csv, schema: given);
        Assert.Equal(DataType.Int8, read["a"].DataType);
        // A non-nullable target still refuses a null source.
        PolarsSchema none = null;
        Assert.Throws<ArgumentNullException>(() => { IntoSchema s = none; });
    }
}
