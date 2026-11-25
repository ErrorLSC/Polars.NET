// The LINQ provider builds generic code at run time (rows of element types known only then): its public entry points
// carry RequiresDynamicCode / RequiresUnreferencedCode, so the analyzer's warnings inside it are silenced here.
#pragma warning disable IL2026, IL2060, IL2067, IL2070, IL2072, IL2075, IL2080, IL2087, IL2090, IL2091, IL3050, IL3051

using System.Linq.Expressions;
using System.Reflection;
using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Marshalling;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>
/// Dedicated converter for translating in-memory .NET collections, arrays, and objects into native Polars Series,
/// DataFrame, and LazyFrame handles.
/// </summary>
internal static class PolarsObjectConverter
{
    private const BindingFlags HandleFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;

    /// <summary>Attempts to extract or convert an in-memory object into a DataFrameHandle.</summary>
    public static DataFrameHandle? TryCreateDataFrameHandle(object? value)
    {
        switch (value)
        {
            case null:
                return null;
            case DataFrameHandle df:
                return df;
        }
        var valueType = value.GetType();
        if (valueType.GetProperty("Handle", HandleFlags) is { } handleProperty)
            return handleProperty.GetValue(value) as DataFrameHandle;
        if (valueType == typeof(string) || ClrTypeHelper.TryGetEnumerableElementType(valueType) is not { } elementType) return null;
        try
        {
            var handle = (DataFrameHandle?)MaterializerInvokers.FromRowsMethod.MakeGenericMethod(elementType).Invoke(null, [value]);
            return handle is not null && !handle.IsInvalid ? handle : null;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>Attempts to extract or compile an in-memory object / queryable / handle into a LazyFrameHandle.</summary>
    public static LazyFrameHandle? TryCreateLazyFrameHandle(object? value)
    {
        switch (value)
        {
            case null: return null;
            case IPolarsPlanSource plan: return plan.GetCompiledLazyFrameHandle();
            case LazyFrameHandle lf: return PolarsWrapper.LazyClone(lf);
            case DataFrameHandle df: return PolarsWrapper.DataFrameToLazy(df);
        }
        if (value.GetType().GetProperty("Handle", HandleFlags) is { } handleProperty)
        {
            return handleProperty.GetValue(value) switch
            {
                LazyFrameHandle lf => PolarsWrapper.LazyClone(lf),
                DataFrameHandle df => PolarsWrapper.DataFrameToLazy(df),
                _ => null,
            };
        }
        return TryCreateDataFrameHandle(value) is { } frame ? PolarsWrapper.DataFrameToLazy(frame) : null;
    }

    /// <summary>Uniformly extracts a LazyFrameHandle from an Expression.</summary>
    public static LazyFrameHandle? ResolveToLazyFrameHandle(Expression expr)
    {
        var clean = QueryCompiler.Unwrap(expr);
        object? raw;
        if (clean is ConstantExpression constant) raw = constant.Value;
        else if (QueryCompiler.TryEvaluate(clean, out var evaluated)) raw = evaluated;
        else
        {
            try
            {
                raw = Expression.Lambda<Func<object>>(Expression.Convert(clean, typeof(object))).Compile().Invoke();
            }
            catch
            {
                raw = null;
            }
        }
        return raw is null ? null : TryCreateLazyFrameHandle(raw);
    }

    /// <summary>
    /// Binds a backend-native runtime object (compiled queryable, LazyFrame/DataFrame handle, or a wrapper exposing a
    /// Handle property) as an opaque table source. In-memory collections return null.
    /// </summary>
    public static ILogicalTableSource? TryBindExternalSource(object? value)
    {
        // A frame a Delta scan returned keeps its origin (file skipping); its clone is the frame the source reads.
        static ILogicalTableSource OfFrame(LazyFrameHandle lf) => DeltaKernelSources.OfLazyFrame(lf, PolarsWrapper.LazyClone(lf));
        switch (value)
        {
            case null: return null;
            case IPolarsPlanSource plan: return PolarsPhysicalLowerer.TableSourceOfLazyFrame(plan.GetCompiledLazyFrameHandle());
            case LazyFrameHandle lf: return OfFrame(lf);
            case DataFrameHandle df: return PolarsPhysicalLowerer.TableSourceOfLazyFrame(PolarsWrapper.DataFrameToLazy(df));
        }
        if (value.GetType().GetProperty("Handle", HandleFlags) is not { } handleProperty) return null;
        return handleProperty.GetValue(value) switch
        {
            LazyFrameHandle lf => OfFrame(lf),
            DataFrameHandle df => PolarsPhysicalLowerer.TableSourceOfLazyFrame(PolarsWrapper.DataFrameToLazy(df)),
            _ => null,
        };
    }
}
