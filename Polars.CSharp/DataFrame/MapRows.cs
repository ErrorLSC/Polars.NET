using Polars.NET.Core.Helpers;

namespace Polars.CSharp;

public partial class DataFrame
{
    /// <summary>
    /// Transforms DataFrame rows into a new DataFrame using a strongly-typed mapping function.
    /// <para>
    /// Operates entirely in managed memory with pre-compiled property extraction and ingestion delegates,
    /// eliminating per-row reflection and intermediate boxing allocations.
    /// </para>
    /// </summary>
    /// <typeparam name="TIn">Input row DTO/POCO type.</typeparam>
    /// <typeparam name="TOut">Output row DTO/POCO type.</typeparam>
    /// <param name="mapFunc">The transformation delegate applied to each row.</param>
    /// <returns>A new DataFrame materialized from the mapped output records.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="mapFunc"/> is null.</exception>
    /// <exception cref="OverflowException">Thrown when DataFrame height exceeds <see cref="int.MaxValue"/>.</exception>
    public DataFrame MapRows<TIn, TOut>(Func<TIn, TOut> mapFunc)
        where TIn : new()
    {
        ArgumentNullException.ThrowIfNull(mapFunc);

        long height = Height;
        if (height > int.MaxValue)
            throw new OverflowException($"DataFrame height ({height}) exceeds Int32.MaxValue.");

        int count = (int)height;
        var outProps = ObjectSchemaTransposer<TOut>.ModelProperties;

        if (outProps.Length == 0)
            return [];

        // 1. Handle empty DataFrame edge-case (preserves column schema)
        if (count == 0)
        {
            var emptyHandles = ObjectSchemaTransposer<TOut>.CreateEmptySeriesHandles();
            var emptySeries = new Series[emptyHandles.Length];
            for (int i = 0; i < emptyHandles.Length; i++)
            {
                emptySeries[i] = new Series(emptyHandles[i]);
            }
            return [.. emptySeries];
        }

        // 2. Pre-allocate exact capacity buffers for each output column
        var buffers = ObjectSchemaTransposer<TOut>.CreateBuffers(count);

        // 3. Obtain pre-compiled non-boxing row writer for TOut
        var writer = ObjectSchemaTransposer<TOut>.RowWriter;

        // 4. Stream rows using zero-heap ref struct enumerator
        var enumerator = Rows<TIn>();
        while (enumerator.MoveNext())
        {
            TOut mappedRow = mapFunc(enumerator.Current);
            writer(mappedRow, buffers);
        }

        // 5. Seal buffers into Polars Series and build final DataFrame
        var seriesList = new Series[outProps.Length];
        for (int i = 0; i < outProps.Length; i++)
        {
            var handle = buffers[i].ToSeriesHandle(outProps[i].Name);
            seriesList[i] = new Series(handle);
        }

        return [.. seriesList];
    }
}
