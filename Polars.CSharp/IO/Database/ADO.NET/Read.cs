using System.Data;
using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Data;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Create a DataFrame directly from a <see cref="IDataReader"/>.
    /// <para>
    /// This method streams data from the reader into Arrow batches, allowing for memory-efficient 
    /// loading of large datasets from databases (e.g., SQL Server, PostgreSQL, SQLite).
    /// </para>
    /// <para>
    /// It automatically maps C# types to Polars types (e.g., <see cref="decimal"/> to Decimal128, <see cref="DateTime"/> to Timestamp).
    /// </para>
    /// </summary>
    /// <param name="reader">The open IDataReader instance.</param>
    /// <param name="batchSize">The number of rows to process per Arrow batch. Default is 50,000.</param>
    /// <returns>A new DataFrame.</returns>
    /// <example>
    /// <code>
    /// // Mocking a DataTable as a data source
    /// var table = new System.Data.DataTable();
    /// table.Columns.Add("Product", typeof(string));
    /// table.Columns.Add("Price", typeof(decimal)); // Correctly maps to Polars Decimal128
    /// 
    /// table.Rows.Add("Laptop", 1234.56m);
    /// table.Rows.Add("Mouse", 99.99m);
    /// 
    /// using IDataReader reader = table.CreateDataReader();
    /// 
    /// var df = DataFrame.ReadDatabase(reader);
    /// df.Show();
    /// /* Output:
    /// shape: (2, 2)
    /// ┌─────────┬─────────────────────────┐
    /// │ Product ┆ Price                   │
    /// │ ---     ┆ ---                     │
    /// │ str     ┆ decimal[38,18]          │
    /// ╞═════════╪═════════════════════════╡
    /// │ Laptop  ┆ 1234.560000000000000000 │
    /// │ Mouse   ┆ 99.990000000000000000   │
    /// └─────────┴─────────────────────────┘
    /// */
    /// </code>
    /// </example>
    public static DataFrame ReadDatabase(IDataReader reader, int batchSize = 50_000)
    {
        // Get Schema 
        var schema = reader.GetArrowSchema();

        var batchEnumerable = reader.ToArrowBatches(batchSize).Prefetch();

        var handle = ArrowStreamInterop.ImportEager(batchEnumerable, schema);
        
        if (handle.IsInvalid)
        {
            var emptyBatch = new RecordBatch(schema, [], 0);
            return new DataFrame(ArrowFfiBridge.ImportDataFrame(emptyBatch));
        }
        
        return new DataFrame(handle);
    }
}
