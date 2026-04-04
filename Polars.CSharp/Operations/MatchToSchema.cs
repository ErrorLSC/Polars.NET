using System.Runtime.InteropServices;
using Polars.NET.Core;
using Polars.NET.Core.Native;

namespace Polars.CSharp;

/// <summary>
/// Represents the action to take when a column from the target schema is missing in the current frame.
/// </summary>
public class MissingColumnAction
{
    internal PlMissingColumnsPolicyType Type { get; }
    internal Expr? Expression { get; }

    private MissingColumnAction(PlMissingColumnsPolicyType type, Expr? expr = null)
    {
        Type = type;
        Expression = expr?.Clone();
    }

    /// <summary>
    /// Insert the missing column with null values.
    /// </summary>
    public static MissingColumnAction Insert() => new(PlMissingColumnsPolicyType.Insert);
    
    /// <summary>
    /// Raise an error if the column is missing.
    /// </summary>
    public static MissingColumnAction Raise() => new(PlMissingColumnsPolicyType.Raise);
    
    /// <summary>
    /// Insert the missing column and fill it with the evaluated expression or literal value.
    /// </summary>
    /// <param name="expr">The expression or primitive value to fill the missing column with.</param>
    public static MissingColumnAction InsertWith(IntoExpr expr) 
        => new(PlMissingColumnsPolicyType.InsertWith, expr.Consume());
}

/// <summary>
/// Configuration for schema matching policies during a MatchToSchema operation.
/// Provides fine-grained control over how missing columns, struct fields, and type castings are handled.
/// </summary>
public record MatchSchemaConfig
{
    /// <summary>
    /// Policy for handling top-level columns present in the target schema but missing in the frame.
    /// Defaults to <see cref="MissingColumnAction.Raise"/>.
    /// </summary>
    public MissingColumnAction MissingColumns { get; init; } = MissingColumnAction.Raise();
    
    /// <summary>
    /// Policy for handling missing fields within nested Struct columns.
    /// Defaults to <see cref="MissingColumnsPolicy.Raise"/>.
    /// </summary>
    public MissingColumnsPolicy MissingStructFields { get; init; } = MissingColumnsPolicy.Raise;
    
    /// <summary>
    /// Policy for handling extra fields within nested Struct columns that are not in the target schema.
    /// Defaults to <see cref="ExtraColumnsPolicy.Raise"/>.
    /// </summary>
    public ExtraColumnsPolicy ExtraStructFields { get; init; } = ExtraColumnsPolicy.Raise;
    
    /// <summary>
    /// Policy for handling integer type casting (e.g., allowing upcast from Int32 to Int64).
    /// Defaults to <see cref="UpcastOrForbid.Forbid"/>.
    /// </summary>
    public UpcastOrForbid IntegerCast { get; init; } = UpcastOrForbid.Forbid;
    
    /// <summary>
    /// Policy for handling floating-point type casting (e.g., allowing upcast from Float32 to Float64).
    /// Defaults to <see cref="UpcastOrForbid.Forbid"/>.
    /// </summary>
    public UpcastOrForbid FloatCast { get; init; } = UpcastOrForbid.Forbid;
    internal PolarsWrapper.PlMatchToSchemaConfig ToCoreConfig() => new()
    {
        MissingColumnsType = MissingColumns.Type,
        MissingColumnsExpr = MissingColumns.Expression?.CloneHandle(),
        MissingStructFields = (PlMissingColumnsPolicy)MissingStructFields,
        ExtraStructFields = (PlExtraColumnsPolicy)ExtraStructFields,
        IntegerCast = (PlUpcastOrForbid)IntegerCast,
        FloatCast = (PlUpcastOrForbid)FloatCast
    };
}
public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
    /// <summary>
    /// Align the Frame's schema to the target schema.
    /// Columns not in the target schema are handled according to the extraColumns policy.
    /// </summary>
    /// <param name="schema">The target schema to match against.</param>
    /// <param name="extraColumns">Policy for handling top-level columns present in the frame but not in the target schema.</param>
    /// <param name="defaultConfig">The default configuration applied to all columns for handling missing data and type casting.</param>
    /// <param name="columnOverrides">Specific configuration overrides mapped by column name.</param>
    /// <returns>A new <see cref="LazyFrame"/> with the aligned schema.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the provided schema is null.</exception>

    public LazyFrame MatchToSchema(
        PolarsSchema schema,
        ExtraColumnsPolicy extraColumns = ExtraColumnsPolicy.Raise,
        MatchSchemaConfig? defaultConfig = null,
        IReadOnlyDictionary<string, MatchSchemaConfig>? columnOverrides = null)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var coreDefault = (defaultConfig ?? new MatchSchemaConfig()).ToCoreConfig();

        Dictionary<string,PolarsWrapper.PlMatchToSchemaConfig>? coreOverrides = null;
        if (columnOverrides != null && columnOverrides.Count > 0)
        {
            coreOverrides = new Dictionary<string, PolarsWrapper.PlMatchToSchemaConfig>(columnOverrides.Count);
            foreach (var kvp in columnOverrides)
            {
                coreOverrides[kvp.Key] = kvp.Value.ToCoreConfig();
            }
        }

        var handle = PolarsWrapper.MatchToSchema(
            this.CloneHandle(),
            schema.Handle,
            (PlExtraColumnsPolicy)extraColumns,
            coreDefault,
            coreOverrides
        );

        return new LazyFrame(handle);
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <inheritdoc cref="LazyFrame.MatchToSchema(PolarsSchema, ExtraColumnsPolicy, MatchSchemaConfig?, IReadOnlyDictionary{string, MatchSchemaConfig}?)"/>
    public DataFrame MatchToSchema(
        PolarsSchema schema,
        ExtraColumnsPolicy extraColumns = ExtraColumnsPolicy.Raise,
        MatchSchemaConfig? defaultConfig = null,
        IReadOnlyDictionary<string, MatchSchemaConfig>? columnOverrides = null)
    => Lazy().MatchToSchema(schema,extraColumns,defaultConfig,columnOverrides).Collect();
}