using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Apache.Arrow;
using Polars.NET.Core.Data;
using Apache.Arrow.Ipc;
using System.Data.Common;
using System.Threading.Channels;
using System.Numerics.Tensors;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Convert a DataFrame to a Series of type Struct.
    /// </summary>
    /// <param name="name">Name for the struct Series.</param>
    /// <returns></returns>
    public Series ToStruct(string name="")
    {
        using var df = Select(Pl.Struct(Pl.All()));
        var series = df[0];
        series.Rename(name);
        return series;
    }
    /// <summary>
    /// Convert the DataFrame to a dictionary of column name to Series.
    /// </summary>
    public IReadOnlyDictionary<string, Series> ToDict()
    {
        var dict = new Dictionary<string, Series>(StringComparer.Ordinal);
        foreach (var series in this)
        {
            dict[series.Name] = series;
        }
        return dict;
    }
    /// <summary>
    /// Convert every row to a dictionary of column name to value.
    /// Corresponds to Python's <c>DataFrame.to_dicts()</c>.
    /// </summary>
    /// <returns>A list of dictionaries, one per row, mapping column names to their values as <c>object?</c>.</returns>
    public List<Dictionary<string, object?>> ToDicts()
    {
        var columns = GetColumns();         
        int rowCount = (int)Height;
        var result = new List<Dictionary<string, object?>>(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            var dict = new Dictionary<string, object?>(columns.Length, StringComparer.Ordinal);
            foreach (var col in columns)
            {
                dict[col.Name] = col[i];
            }
            result.Add(dict);
        }

        return result;
    }

    /// <summary>
    /// Convert the DataFrame into a LazyFrame.
    /// This allows building a query plan and optimizing execution.
    /// </summary>
    public LazyFrame Lazy()
        => new(PolarsWrapper.DataFrameToLazy(Handle));

    /// <summary>
    /// Export DataFrame to Record Batch
    /// </summary>
    /// <param name="onBatchReceived">Receive RecordBatch Callback</param>
    public void ExportBatches(Action<RecordBatch> onBatchReceived) => PolarsWrapper.ExportBatches(Handle, onBatchReceived);
    /// <summary>
    /// Export DataFrame As DbDataReader (Zero-Copy Enabled)
    /// </summary>    
    public DbDataReader AsDataReader(int bufferSize = 5, Dictionary<string, Type>? typeOverrides = null)
    {
        var channel = Channel.CreateBounded<RecordBatch>(new BoundedChannelOptions(bufferSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true, 
            SingleReader = true  
        });

        var cts = new CancellationTokenSource();

        var producerTask = Task.Run(async () => 
        {
            try
            {
                ExportBatches(batch => 
                {
                    channel.Writer.WriteAsync(batch, cts.Token).AsTask().Wait(cts.Token);
                });
                
                channel.Writer.Complete();
            }
            catch (OperationCanceledException)
            {

                channel.Writer.Complete();
            }
            catch (Exception ex)
            {
                channel.Writer.Complete(ex);
            }
        });

        var stream = channel.Reader.ReadAllAsync(cts.Token).ToBlockingEnumerable(cts.Token);
        
        var innerReader = new ArrowToDbStream(stream, typeOverrides);

        return new PolarsDataReader(innerReader, cts, producerTask); 
    }

    /// <summary>
    /// Export DataFrame to Arrow C Data Interface Stream.
    /// Supports zero-copy and lazy chunked reading.
    /// </summary>
    /// <param name="columnIndices">Optional column indices to prune the export (Projection Pushdown).</param>
    /// <param name="seed">Optional random seed to shuffle chunks.</param>
    /// <returns>Standard IArrowArrayStream</returns>
    public IArrowArrayStream ToArrowStream(ReadOnlySpan<int> columnIndices = default, ulong? seed = null)
        => ArrowStreamInterop.ExportToStream(Handle, columnIndices, seed);

    /// <summary>
    /// Converts selected DataFrame columns into a managed 2D Row-Major Tensor [Rows, Columns].
    /// Automatically handles memory fragmentation and performs the Column-Major to Row-Major transposition 
    /// required by most Machine Learning frameworks (like ONNX, TorchSharp, ML.NET).
    /// </summary>
    /// <typeparam name="T">Unmanaged Type Only (e.g., float, double, int)</typeparam>
    /// <param name="columnNames">Specific columns to extract. If empty, extracts all columns.</param>
    /// <returns>A 2D <see cref="Tensor{T}"/> representing the feature matrix.</returns>
    public Tensor<T> AsTensor<T>(params string[] columnNames) where T : unmanaged
    {
        Series[] targetColumns;
        if (columnNames == null || columnNames.Length == 0)
        {
            targetColumns = GetColumns();
        }
        else
        {
            targetColumns = new Series[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
            {
                targetColumns[i] = Column(columnNames[i]); 
            }
        }

        if (targetColumns.Length == 0) 
            throw new InvalidOperationException("Cannot create a Tensor from an empty DataFrame.");

        int cols = targetColumns.Length;
        int rows = (int)Height; 

        T[] tensorData = new T[rows * cols];
        Span<T> destSpan = tensorData.AsSpan();

        try
        {
            for (int c = 0; c < cols; c++)
            {
                var col = targetColumns[c];

                bool needsRechunk = !col.IsContiguous;
                Series activeCol = needsRechunk ? col.Rechunk() : col;

                try
                {
                    ReadOnlySpan<T> span = activeCol.AsReadOnlySpan<T>();

                    for (int r = 0; r < rows; r++)
                    {
                        destSpan[r * cols + c] = span[r];
                    }
                }
                finally
                {
                    if (needsRechunk) activeCol.Dispose();
                }
            }

            return Tensor.Create(tensorData, [rows, cols]);
        }
        finally
        {
            for (int i = 0; i < cols; i++)
            {
                targetColumns[i]?.Dispose();
            }
        }
    }
}