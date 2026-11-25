using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>Uniform contract to expose the compiled native LazyFrameHandle of a Polars queryable without leaking generic parameters.</summary>
internal interface IPolarsPlanSource
{
    /// <summary>Compiles the queryable pipeline stages and returns the final optimized LazyFrameHandle.</summary>
    LazyFrameHandle GetCompiledLazyFrameHandle();
}

internal static class NativeOpsApplier
{
    /// <summary>All column names of a LazyFrame's schema with their DataTypeHandle.</summary>
    public static (string Name, DataTypeHandle Type)[] GetLazySchemaFields(LazyFrameHandle lf)
    {
        var schema = PolarsWrapper.GetLazySchema(lf);
        var count = PolarsWrapper.GetSchemaLen(schema);
        var fields = new (string, DataTypeHandle)[(int)count];
        for (int i = 0; i < fields.Length; i++)
        {
            PolarsWrapper.GetSchemaFieldAt(schema, (ulong)i, out string name, out DataTypeHandle type);
            fields[i] = (name, type);
        }
        return fields;
    }

    /// <summary>
    /// Aligns <paramref name="targetLf"/> to the columns of <paramref name="baseFields"/> (by name when the names
    /// match, else by position), casting where the types differ; <paramref name="targetFields"/> is the schema of
    /// <paramref name="targetLf"/>.
    /// </summary>
    public static LazyFrameHandle AlignToFields(
        (string Name, DataTypeHandle Type)[] baseFields,
        (string Name, DataTypeHandle Type)[] targetFields,
        LazyFrameHandle targetLf)
    {
        static ExprHandle CastTo(ExprHandle column, DataTypeHandle baseType, DataTypeHandle targetType) =>
            PolarsWrapper.DataTypeEq(baseType, targetType)
                ? column
                : PolarsWrapper.ExprCast(column, PolarsWrapper.DataTypeExprFromDataType(baseType), strict: false, wrapNumerical: false);

        if (baseFields.Select(f => f.Name).ToHashSet().SetEquals(targetFields.Select(f => f.Name)))
        {
            var targetTypes = targetFields.ToDictionary(f => f.Name, f => f.Type);
            ExprHandle[] reordered = [.. baseFields.Select(f => CastTo(PolarsWrapper.Col(f.Name), f.Type, targetTypes[f.Name]))];
            return PolarsWrapper.LazySelect(targetLf, reordered);
        }
        if (baseFields.Length == targetFields.Length)
        {
            ExprHandle[] projected =
            [
                .. baseFields.Zip(targetFields, (b, t) => PolarsWrapper.Alias(CastTo(PolarsWrapper.Col(t.Name), b.Type, t.Type), b.Name)),
            ];
            return PolarsWrapper.LazySelect(targetLf, projected);
        }
        return targetLf;
    }

    /// <summary>Aligns <paramref name="targetLf"/> to the schema of <paramref name="baseLf"/> (see <see cref="AlignToFields"/>).</summary>
    public static LazyFrameHandle AlignSchema(LazyFrameHandle baseLf, LazyFrameHandle targetLf) =>
        AlignToFields(GetLazySchemaFields(baseLf), GetLazySchemaFields(targetLf), targetLf);
}
