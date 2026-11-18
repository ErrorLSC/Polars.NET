using System.Diagnostics.CodeAnalysis;
using Kaminari.Sazanami.Data;
using Polars.NET.Core.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Helpers;

namespace Polars.CSharp;

public partial class DataFrame
{
    /// <summary>
    /// Transforms DataFrame rows into a new DataFrame using a strongly-typed mapping function.
    /// <para>
    /// The mapped rows are appended by code compiled once per type into native Arrow memory (Sazanami's
    /// <c>ArrowRowBuilder</c>), without per-row reflection or boxing, and imported into the engine without copying.
    /// </para>
    /// </summary>
    /// <typeparam name="TIn">Input row DTO/POCO type.</typeparam>
    /// <typeparam name="TOut">Output row DTO/POCO type.</typeparam>
    /// <param name="mapFunc">The transformation delegate applied to each row.</param>
    /// <returns>A new DataFrame materialized from the mapped output records.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="mapFunc"/> is null.</exception>
    /// <exception cref="OverflowException">Thrown when DataFrame height exceeds <see cref="int.MaxValue"/>.</exception>
    public DataFrame MapRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TIn, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TOut>(Func<TIn, TOut> mapFunc)
    {
        ArgumentNullException.ThrowIfNull(mapFunc);

        long height = Height;
        if (height > int.MaxValue)
            throw new OverflowException($"DataFrame height ({height}) exceeds Int32.MaxValue.");

        // The mapped rows as one Arrow batch in native memory (Sazanami's row builder), imported without copying.
        using var builder = new ArrowRowBuilder<TOut>();
        builder.Reserve((int)height);

        // Stream rows using zero-heap ref struct enumerator
        var enumerator = Rows<TIn>();
        while (enumerator.MoveNext())
        {
            builder.Append(mapFunc(enumerator.Current));
        }

        using Apache.Arrow.RecordBatch batch = builder.ExportBatch();
        return new DataFrame(ArrowFfiBridge.ImportDataFrame(batch));
    }
    /// <summary>
    /// Iterates over all rows in the DataFrame as strongly-typed models using a zero-allocation stack enumerator,
    /// executing the specified action for each row without allocating heap objects.
    /// </summary>
    /// <typeparam name="T">The target model or record type matching the DataFrame schema.</typeparam>
    /// <param name="action">The action to execute for each row.</param>
    public void IterRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Action<T> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        if (Height == 0)
            return;

        // Stream rows using zero-heap ref struct enumerator
        var enumerator = Rows<T>();
        while (enumerator.MoveNext())
        {
            action(enumerator.Current);
        }
    }
}
