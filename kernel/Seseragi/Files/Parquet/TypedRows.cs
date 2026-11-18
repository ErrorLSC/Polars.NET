using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// What a typed read (<see cref="ParquetReader{T}"/>, <see cref="ParquetDatasetReader{T}"/>) asks of its batch reader,
/// and the conjuncts it evaluates on the rows itself.
/// </summary>
/// <param name="Options">The batch reader's options: the members' columns, the predicate's pushed conjuncts as the filter.</param>
/// <param name="Client">The conjuncts evaluated on the rows (null: none).</param>
/// <param name="MaxRows">The rows to deliver, counted on the rows when the client evaluates conjuncts (else the batch reader limits).</param>
/// <param name="Exact">Whether the options' own filter (with a predicate) is evaluated exactly; null without a predicate (the batch reader tells).</param>
internal sealed record TypedRead<T>(ParquetReadOptions Options, Func<T, bool>? Client, long? MaxRows, bool? Exact);

/// <summary>The predicate split of <see cref="TypedRows.Plan{T}"/> over Parquet read options.</summary>
internal static class ParquetTypedRows
{
    /// <inheritdoc cref="TypedRows.Plan{T}"/>
    public static TypedRead<T> Plan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Expression<Func<T, bool>>? where,
        IReadOnlyList<string> names, Func<ParquetReadOptions, Apache.Arrow.Schema> lowering, ParquetReadOptions options, string what)
    {
        TypedPlan<T> plan = TypedRows.Plan(where, names, columns => lowering(options with { Columns = columns }),
            new TypedRequest(options.Columns, options.ColumnPaths is not null, options.Filter, options.MaxRows, "Parquet"), what);
        options = options with { Columns = plan.Columns, MaxRows = plan.MaxRows };
        if (plan.FilterRows) options = options with { Filter = plan.Filter, FilterRows = true };
        return new TypedRead<T>(options, plan.Client, plan.ClientMaxRows, plan.Exact);
    }
}
