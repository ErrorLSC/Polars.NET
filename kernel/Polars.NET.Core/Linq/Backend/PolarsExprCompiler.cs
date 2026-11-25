using System.Linq.Expressions;
using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Microsoft.FSharp.Collections;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>
/// Compiles standalone LINQ expressions (outside a query pipeline) into native Polars expressions, e.g. for TypedMerge
/// conditions/setters and DataFrame-level key selectors.
/// </summary>
internal static class PolarsExprCompiler
{
    /// <summary>
    /// Translates the expression to a symbolic ScalarExpr in a compilation scope for the Polars backend (its enum
    /// conversions resolved there), then lowers it to an ExprHandle. <paramref name="schema"/> holds the columns the
    /// expression reads (enum conversions depend on their types).
    /// </summary>
    public static ExprHandle? TryCompile(TranslationContext context, LogicalSchema schema, Expression expr)
    {
        PolarsRuntimeConfig.EnsureImported();
        return QueryCompiler.TryTranslateScalar(PolarsQueryBackend.Instance, context, schema, expr) is { } translated
            ? PolarsScalarLowerer.Lower(translated)
            : null;
    }

    /// <summary>Like <see cref="TryCompile"/>, for an expression whose value is stored into a column of <paramref name="columnType"/>.</summary>
    public static ExprHandle? TryCompileAssignment(TranslationContext context, LogicalSchema schema, LogicalType columnType, Expression expr)
    {
        PolarsRuntimeConfig.EnsureImported();
        return QueryCompiler.TryTranslateAssignment(PolarsQueryBackend.Instance, context, schema, columnType, expr) is { } translated
            ? PolarsScalarLowerer.Lower(translated)
            : null;
    }

    /// <summary>The schema, with column types, of a LazyFrame.</summary>
    public static LogicalSchema SchemaOf(LazyFrameHandle lf) => PolarsPhysicalLowerer.SchemaOfLazyFrame(lf);

    /// <summary>
    /// The columns a merge expression reads: the target's under their names and the source's with
    /// <paramref name="sourceSuffix"/> (as TranslationContext.Merge resolves them).
    /// </summary>
    public static LogicalSchema MergeSchema(LogicalSchema target, LogicalSchema source, string sourceSuffix) =>
        new(ListModule.OfSeq(target.Fields.Concat(source.Fields.Select(f => new LogicalField(f.Name + sourceSuffix, f.DataType, f.Nullable)))));

    /// <summary>The type of <paramref name="column"/> in <paramref name="schema"/>, or Unknown.</summary>
    public static LogicalType ColumnType(LogicalSchema schema, string column) =>
        schema.Fields.FirstOrDefault(f => f.Name == column)?.DataType ?? LogicalType.Unknown;
}
