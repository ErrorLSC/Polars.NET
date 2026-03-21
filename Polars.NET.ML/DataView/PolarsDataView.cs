using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.ML;
using Microsoft.ML.Data;
using Polars.NET.Core;

namespace Polars.NET.ML.DataView;

/// <summary>
/// A zero-copy ML.NET IDataView implementation backed by an IPolarsDataFrame interface.
/// </summary>
public sealed class PolarsDataView(IPolarsDataFrame df) : IDataView
{
    private readonly IPolarsDataFrame _df = df ?? throw new ArgumentNullException(nameof(df));
    private readonly DataViewSchema _schema = BuildSchema(df);
    private readonly long _rowCount = df.Height;

    // ==========================================
    // IDataView Metadata
    // ==========================================

    public DataViewSchema Schema => _schema;
    
    public long? GetRowCount() => _rowCount;
    
    public bool CanShuffle => false; 

    // ==========================================
    // Cursor Factory
    // ==========================================

    public DataViewRowCursor GetRowCursor(IEnumerable<DataViewSchema.Column> columnsNeeded, Random? rand = null)
    {
        return new PolarsRowCursor(_schema, GetArrowBatches());
    }

    public DataViewRowCursor[] GetRowCursorSet(IEnumerable<DataViewSchema.Column> columnsNeeded, int n, Random? rand = null)
    {
        return [GetRowCursor(columnsNeeded, rand)];
    }

    // ==========================================
    // Private Helpers
    // ==========================================

    private IEnumerable<RecordBatch> GetArrowBatches()
    {
        yield return _df.ToArrow(); 
    }

    private static DataViewSchema BuildSchema(IPolarsDataFrame df)
    {
        var builder = new DataViewSchema.Builder();

        IPolarsSchema schema = df.Schema;

        foreach (var kvp in schema.ToDictionary())
        {
            string columnName = kvp.Key;
            IPolarsDataType polarsType = kvp.Value;

            IArrowType arrowType = polarsType.GetArrowType();
            DataViewType mlType = ArrowDataViewMapper.GetDataViewType(arrowType);

            builder.AddColumn(columnName, mlType);
        }

        return builder.ToSchema();
    }
}