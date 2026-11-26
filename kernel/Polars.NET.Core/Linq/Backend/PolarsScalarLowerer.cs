// The LINQ provider builds generic code at run time (series of element types known only then, user functions'
// return types): its public entry points carry RequiresDynamicCode / RequiresUnreferencedCode, so the analyzer's
// warnings inside it are silenced here.
#pragma warning disable IL2026, IL2060, IL2067, IL2070, IL2072, IL2075, IL2080, IL2087, IL2090, IL2091, IL3050, IL3051

using System.Collections;
using System.Reflection;
using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Marshalling;
using Microsoft.FSharp.Collections;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>Lowers pure symbolic ScalarExpr AST nodes to unmanaged Polars ExprHandle pointers.</summary>
internal static class PolarsScalarLowerer
{
    /// <summary>
    /// Converts an in-memory collection (array or IEnumerable) into a native SeriesHandle. Conversion errors propagate
    /// with their original message instead of being reduced to a generic failure.
    /// </summary>
    public static SeriesHandle CreateSeriesHandle(string name, object? value)
    {
        SeriesHandle Checked(SeriesHandle? h) =>
            h is null || h.IsInvalid
                ? throw new InvalidOperationException($"Converting a {value!.GetType().Name} to a Polars Series produced no series.")
                : h;

        switch (value)
        {
            case System.Array array:
                return Checked(SeriesFactory.Create(name, array));
            case IEnumerable seq and not string:
                var elementType = ClrTypeHelper.TryGetEnumerableElementType(seq.GetType()) ?? typeof(object);
                var createGeneric = typeof(SeriesFactory)
                    .GetMethod(nameof(SeriesFactory.CreateGenericType), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(elementType);
                try
                {
                    return Checked((SeriesHandle?)createGeneric.Invoke(null, [name, seq]));
                }
                catch (TargetInvocationException e) when (e.InnerException is not null)
                {
                    ExceptionDispatchInfo.Capture(e.InnerException).Throw();
                    throw;
                }
            case null:
                throw new ArgumentException("Cannot convert null to a Polars Series.", nameof(value));
            default:
                throw new ArgumentException($"Cannot convert a {value.GetType().Name} to a Polars Series.", nameof(value));
        }
    }

    /// <summary>Materializes a symbolic JsonDataType into a native Polars DataTypeHandle.</summary>
    public static DataTypeHandle LowerJsonDataType(JsonDataType dt) => dt switch
    {
        JsonDataType.Primitive p => PolarsVocabulary.ToDataTypeHandle(p.primitiveType),
        JsonDataType.List l => PolarsWrapper.NewListType(LowerJsonDataType(l.element)),
        JsonDataType.Struct s => PolarsWrapper.NewStructType([.. s.fields.Select(f => f.Item1)], [.. s.fields.Select(f => LowerJsonDataType(f.Item2))]),
        _ => throw new ArgumentOutOfRangeException(nameof(dt)),
    };

    /// <summary>Infers the JSON decode schema for a .NET type and materializes it into a native DataTypeHandle; null when it has none.</summary>
    public static DataTypeHandle? InferPolarsJsonDataType(Type t) =>
        IrOps.TryInferJsonType(t) is { } json ? LowerJsonDataType(json) : null;

    /// <summary>Strongly-typed zero-boxing conversion from LiteralValue to Polars ExprHandle.</summary>
    public static ExprHandle LowerLiteral(LiteralValue value)
    {
        switch (value)
        {
            // A typed null keeps its type, so operators (negation, not, a filter) accept it.
            case LiteralValue.Null n when !n.Item.Equals(LogicalType.Unknown) && !n.Item.Equals(LogicalTypes.Null):
                var dt = PolarsVocabulary.ToDataTypeHandle(n.Item);
                return PolarsWrapper.ExprCast(PolarsWrapper.LitNull(), PolarsWrapper.DataTypeExprFromDataType(dt), strict: false, wrapNumerical: false);
            case LiteralValue.Null:
                return PolarsWrapper.LitNull();
            case LiteralValue.Bool v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.Int8 v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.Int16 v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.Int32 v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.Int64 v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.UInt8 v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.UInt16 v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.UInt32 v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.UInt64 v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.Float16 v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.Float32 v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.Float64 v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.Decimal v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.String v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.Date v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.DateTime v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.DateTimeOffset v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.TimeSpan v: return PolarsWrapper.Lit(v.Item);
            case LiteralValue.TimeOnly v: return PolarsWrapper.Lit(v.Item);
            // A typed null filtered to no rows and imploded: one empty list, a scalar that broadcasts.
            case LiteralValue.EmptyList e:
                var none = LowerLiteral(LiteralValue.NewNull(e.element));
                return PolarsWrapper.Implode(PolarsWrapper.Filter(none, PolarsWrapper.Lit(false)), maintainOrder: true);
            default:
                throw new ArgumentOutOfRangeException(nameof(value));
        }
    }

    /// <summary>
    /// Literals of one CLR kind (strings or integers, …) as a typed array (a Series of them), or null for another value
    /// (null, a decimal, kinds mixed).
    /// </summary>
    private static System.Array? TryLiteralArray(FSharpList<LiteralValue> values)
    {
        var boxed = new object[values.Length];
        int i = 0;
        foreach (var value in values)
        {
            object? item = value switch
            {
                LiteralValue.String v => v.Item,
                LiteralValue.Int8 v => v.Item,
                LiteralValue.Int16 v => v.Item,
                LiteralValue.Int32 v => v.Item,
                LiteralValue.Int64 v => v.Item,
                LiteralValue.UInt8 v => v.Item,
                LiteralValue.UInt16 v => v.Item,
                LiteralValue.UInt32 v => v.Item,
                LiteralValue.UInt64 v => v.Item,
                LiteralValue.Float32 v => v.Item,
                LiteralValue.Float64 v => v.Item,
                LiteralValue.Bool v => v.Item,
                LiteralValue.Date v => v.Item,
                _ => null,
            };
            if (item is null) return null;
            boxed[i++] = item;
        }
        var elementType = boxed.Length > 0 ? boxed[0].GetType() : typeof(string);
        if (boxed.Any(v => v.GetType() != elementType)) return null;
        var array = System.Array.CreateInstance(elementType, boxed.Length);
        for (int j = 0; j < boxed.Length; j++) array.SetValue(boxed[j], j);
        return array;
    }

    private static System.Array LiteralArray(FSharpList<LiteralValue> values) =>
        TryLiteralArray(values) ?? throw new InvalidOperationException($"[Polars.NET.Linq Bug] Unsupported literals in a value list: {values}");

    /// <summary>
    /// The managed callback of ScalarFunction.AssertCount: true for every count in the check's range, otherwise the
    /// check's error, relayed to the caller as the .NET exception LINQ raises.
    /// </summary>
    private static Func<IArrowArray, IArrowArray> CountChecker(CountCheck check)
    {
        static Exception Relayed(QueryError error)
        {
            Exception exception = error.Kind.Tag switch
            {
                QueryErrorKind.Tags.InvalidOperation => new InvalidOperationException(error.Message),
                QueryErrorKind.Tags.ArgumentOutOfRange when string.IsNullOrEmpty(error.Message) => new ArgumentOutOfRangeException("index"),
                QueryErrorKind.Tags.ArgumentOutOfRange => new ArgumentOutOfRangeException("index", error.Message),
                QueryErrorKind.Tags.Overflow when string.IsNullOrEmpty(error.Message) => new OverflowException(),
                QueryErrorKind.Tags.Overflow => new OverflowException(error.Message),
                QueryErrorKind.Tags.DivideByZero when string.IsNullOrEmpty(error.Message) => new DivideByZeroException(),
                QueryErrorKind.Tags.DivideByZero => new DivideByZeroException(error.Message),
                _ => throw new ArgumentOutOfRangeException(nameof(error)),
            };
            return ManagedErrorRelay.Capture(exception);
        }

        return counts =>
        {
            long ValueAt(int i) => counts switch
            {
                UInt32Array a => a.GetValue(i).GetValueOrDefault(),
                UInt64Array a => (long)a.GetValue(i).GetValueOrDefault(),
                Int64Array a => a.GetValue(i).GetValueOrDefault(),
                Int32Array a => a.GetValue(i).GetValueOrDefault(),
                _ => throw new InvalidOperationException($"[Polars.NET.Linq Bug] AssertCount over a {counts.GetType().Name} count"),
            };

            var builder = new BooleanArray.Builder();
            for (int i = 0; i < counts.Length; i++)
            {
                long n = ValueAt(i);
                if (n < check.Min) throw Relayed(check.TooFew);
                if (check.Max.ToNullable() is { } max && n > max) throw Relayed(check.TooMany);
                builder.Append(true);
            }
            return builder.Build(PooledArrowAllocator.Instance);
        };
    }

    /// <summary>A SortOrder as (ExprHandle, isDescending, nullsLast).</summary>
    public static (ExprHandle Expr, bool Descending, bool NullsLast) LowerSortOrder(SortOrder order) =>
        (Lower(order.Expr), order.Descending, order.NullsLast);

    /// <summary>An item of a .NET string concatenation, which reads a null as the empty string.</summary>
    public static ExprHandle LowerConcatItem(ScalarExpr item) =>
        Lower(ScalarExpr.NewCoalesce(IrList.Of(
            ScalarExpr.NewCast(item, LogicalTypes.String, false),
            ScalarExpr.NewLiteral(LiteralValue.NewString("")))));

    private static ExprHandle[] LowerAll(IEnumerable<ScalarExpr> exprs) => [.. exprs.Select(Lower)];

    /// <summary>Lowers a ScalarExpr recursively to an ExprHandle.</summary>
    public static ExprHandle Lower(ScalarExpr expr)
    {
        switch (expr)
        {
            case ScalarExpr.Column c:
                return PolarsWrapper.Col(c.name);

            // .NET `%` and integer `/` truncate toward zero (the engine floors): one-pass shim kernels.
            case ScalarExpr.Binary { op.IsModulo: true } b:
                return PolarsWrapper.DotNetRem(Lower(b.left), Lower(b.right));
            case ScalarExpr.Binary { op.IsTruncatedDivide: true } b:
                return PolarsWrapper.DotNetDiv(Lower(b.left), Lower(b.right));
            case ScalarExpr.Binary { op.IsStringConcat: true } b:
                return PolarsWrapper.ConcatString([LowerConcatItem(b.left), LowerConcatItem(b.right)], "", ignoreNulls: false);

            case ScalarExpr.Literal lit:
                return LowerLiteral(lit.value);

            case ScalarExpr.Binary b:
                return LowerBinary(b.op, Lower(b.left), Lower(b.right));

            case ScalarExpr.Unary u:
            {
                var inner = Lower(u.expr);
                return u.op.Tag switch
                {
                    UnaryOp.Tags.Not => PolarsWrapper.Not(inner),
                    UnaryOp.Tags.Negate => PolarsWrapper.Neg(inner),
                    UnaryOp.Tags.IsNull => PolarsWrapper.IsNull(inner),
                    UnaryOp.Tags.IsNotNull => PolarsWrapper.IsNotNull(inner),
                    UnaryOp.Tags.IsNan => PolarsWrapper.ExprIsNan(inner),
                    UnaryOp.Tags.Abs => PolarsWrapper.Abs(inner),
                    _ => throw new ArgumentOutOfRangeException(nameof(expr)),
                };
            }

            case ScalarExpr.Cast c:
            {
                var inner = Lower(c.expr);
                using var dt = PolarsVocabulary.ToDataTypeHandle(c.targetType);
                var dtExpr = PolarsWrapper.DataTypeExprFromDataType(dt);
                // A non-strict numeric cast wraps on overflow, as .NET's unchecked conversion (the engine would give null).
                return PolarsWrapper.ExprCast(inner, dtExpr, strict: c.isStrict, wrapNumerical: !c.isStrict);
            }

            case ScalarExpr.Conditional c:
                return PolarsWrapper.IfElse(Lower(c.test), Lower(c.ifTrue), Lower(c.ifFalse));

            case ScalarExpr.Coalesce c:
                return PolarsWrapper.Coalesce(LowerAll(c.exprs));

            case ScalarExpr.ItemAccess a:
                return PolarsWrapper.ListGet(Lower(a.target), Lower(a.index), nullOnOob: true);

            case ScalarExpr.Slice s:
            {
                var target = Lower(s.target);
                if (s.offset == 0L && s.length < uint.MaxValue) return PolarsWrapper.Head(target, (int)s.length);
                if (s.length == uint.MaxValue) return PolarsWrapper.ExprSlice(target, PolarsWrapper.Lit(s.offset), PolarsWrapper.Lit(long.MaxValue));
                return PolarsWrapper.ExprSlice(target, PolarsWrapper.Lit(s.offset), PolarsWrapper.Lit((long)s.length));
            }

            case ScalarExpr.SortBy s:
                return PolarsWrapper.SortBy(
                    Lower(s.expr),
                    [.. s.by.Select(o => Lower(o.Expr))],
                    [.. s.by.Select(o => o.Descending)],
                    [.. s.by.Select(o => o.NullsLast)],
                    multithreaded: true,
                    maintainOrder: true);

            case ScalarExpr.Alias a:
                return PolarsWrapper.Alias(Lower(a.expr), a.alias);

            case ScalarExpr.Aggregation agg:
                return LowerAggregation(agg.kind, agg.expr, agg.isDistinct);

            case ScalarExpr.Window w:
                return LowerWindow(w);

            case ScalarExpr.Function { func: ScalarFunction.ReplaceStrict replace } f when f.args.Length > 0:
            {
                var input = Lower(f.args.Head);
                var oldH = PolarsWrapper.Lit(CreateSeriesHandle("old", LiteralArray(replace.oldValues)));
                var newH = PolarsWrapper.Lit(CreateSeriesHandle("new", LiteralArray(replace.newValues)));
                ExprHandle? fallback = f.args.Tail switch
                {
                    [] => null,
                    [var d] => Lower(d),
                    _ => throw new InvalidOperationException("[Polars.NET.Linq Bug] replace_strict takes at most one default."),
                };
                using var dt = PolarsVocabulary.ToDataTypeHandle(f.returnType);
                return PolarsWrapper.ExprReplaceStrict(input, oldH, newH, fallback!, PolarsWrapper.DataTypeExprFromDataType(dt));
            }

            case ScalarExpr.EnumConvert e:
                throw new InvalidOperationException(
                    $"[Polars.NET.Linq Bug] Enum conversion {e.conversion} of {IrOps.Format(e.expr)} reached lowering unresolved; EnumResolution must run first.");

            // .NET checked narrowing (null where .NET throws, for NarrowOrNull): one shim kernel, element by element
            // (per group, each group's value). An exact sum of integers in Decimal(38, 0), the IR's widest exact
            // integer, is summed in the engine's Int128: the same values, about half the time of its decimal sum.
            // A guarded row keeps its value; elsewhere it is null, which never fails (SEMANTICS.md, Guards).
            case ScalarExpr.Function { func.IsCheckedNarrow: true, args: [var value, var guard] } f:
                return LowerNarrowing(value, f.returnType, nullOnOverflow: false, guard);
            // ANSI division: the dividend is null off the guard's rows, so only guarded rows can fail.
            case ScalarExpr.Function { func: ScalarFunction.CheckedDivide division, args: [var dividend, var divisor, var guard] } f:
            {
                var kind = division.kind.Tag switch
                {
                    DivisionKind.Tags.Divide => PlCheckedDivision.Divide,
                    DivisionKind.Tags.IntegerDivide => PlCheckedDivision.IntegerDivide,
                    _ => PlCheckedDivision.Remainder,
                };
                var (precision, scale) = f.returnType is LogicalType.Arrow { arrowType: Decimal128Type d } ? (d.Precision, d.Scale) : (-1, 0);
                return PolarsWrapper.CheckedDiv(Guarded(Lower(dividend), guard), Lower(divisor), kind, precision, scale);
            }
            case ScalarExpr.Function { func.IsNarrowOrNull: true, args: [var value] } f:
                return LowerNarrowing(value, f.returnType, nullOnOverflow: true);

            // .NET 11's conversions between decimals and floating values: shim kernels (the engine's casts round otherwise).
            case ScalarExpr.Function { func.IsDecimalToFloating: true, args: [var value] } f:
            {
                var target = f.returnType switch
                {
                    LogicalType.Arrow { arrowType: FloatType } => PlDataType.Float32,
                    LogicalType.Arrow { arrowType: DoubleType } => PlDataType.Float64,
                    var other => throw new InvalidOperationException($"[Polars.NET.Linq Bug] a decimal converted to {other}"),
                };
                return PolarsWrapper.DotNetDecimalToFloat(Lower(value), target);
            }
            case ScalarExpr.Function { func.IsFloatingToDecimal: true, args: [var value], returnType: LogicalType.Arrow { arrowType: Decimal128Type d } }:
                return PolarsWrapper.DotNetFloatToDecimal(Lower(value), d.Precision, d.Scale);

            case ScalarExpr.Function f:
                return LowerFunction(f.func, f.args);

            case ScalarExpr.Udf udf:
                return LowerUdf(LowerAll(udf.args), udf.invoker);

            // Literals of another kind (a null among them, decimals): one equality per value, which gives IN's result
            // (true on a match; without nullsEqual null when a side is null and nothing matched).
            case ScalarExpr.InList { source: InListSource.Literals literals } inList
                when TryLiteralArray(literals.Item) is null && !literals.Item.IsEmpty:
            {
                var eq = inList.nullsEqual ? BinaryOp.EqMissing : BinaryOp.Eq;
                var any = literals.Item
                    .Select(v => ScalarExpr.NewBinary(eq, inList.expr, ScalarExpr.NewLiteral(v)))
                    .Aggregate((a, b) => ScalarExpr.NewBinary(BinaryOp.Or, a, b));
                return Lower(inList.negated ? ScalarExpr.NewUnary(UnaryOp.Not, any) : any);
            }

            case ScalarExpr.InList inList:
            {
                var inner = Lower(inList.expr);
                var values = inList.source switch
                {
                    InListSource.Collection c => CreateSeriesHandle("in_vals", c.values),
                    InListSource.Literals l => CreateSeriesHandle("in_vals", LiteralArray(l.Item)),
                    _ => throw new ArgumentOutOfRangeException(nameof(expr)),
                };
                var list = PolarsWrapper.Implode(PolarsWrapper.Lit(values), maintainOrder: false);
                var isIn = PolarsWrapper.IsIn(inner, list, nullsEqual: inList.nullsEqual);
                return inList.negated ? PolarsWrapper.Not(isIn) : isIn;
            }

            case ScalarExpr.Subquery s:
                throw new InvalidOperationException($"[Samidare Bug] Subquery #{s.scope} reached physical lowering: the Decorrelation pass replaces subqueries by joins first.");
            case ScalarExpr.OuterColumn o:
                throw new InvalidOperationException($"[Samidare Bug] Subquery #{o.scope} reached physical lowering: the Decorrelation pass replaces subqueries by joins first.");

            default:
                throw new ArgumentOutOfRangeException(nameof(expr), $"Unsupported expression {expr}");
        }
    }

    private static ExprHandle LowerBinary(BinaryOp op, ExprHandle left, ExprHandle right) => op.Tag switch
    {
        BinaryOp.Tags.Eq => PolarsWrapper.Eq(left, right),
        BinaryOp.Tags.NotEq => PolarsWrapper.Neq(left, right),
        BinaryOp.Tags.EqMissing => PolarsWrapper.EqMissing(left, right),
        BinaryOp.Tags.NotEqMissing => PolarsWrapper.NeqMissing(left, right),
        BinaryOp.Tags.Lt => PolarsWrapper.Lt(left, right),
        BinaryOp.Tags.LtEq => PolarsWrapper.LtEq(left, right),
        BinaryOp.Tags.Gt => PolarsWrapper.Gt(left, right),
        BinaryOp.Tags.GtEq => PolarsWrapper.GtEq(left, right),
        BinaryOp.Tags.And => PolarsWrapper.And(left, right),
        BinaryOp.Tags.Or => PolarsWrapper.Or(left, right),
        BinaryOp.Tags.Xor => PolarsWrapper.Xor(left, right),
        BinaryOp.Tags.Add => PolarsWrapper.Add(left, right),
        BinaryOp.Tags.Subtract => PolarsWrapper.Sub(left, right),
        BinaryOp.Tags.Multiply => PolarsWrapper.Mul(left, right),
        BinaryOp.Tags.Divide => PolarsWrapper.Div(left, right),
        BinaryOp.Tags.Modulo or BinaryOp.Tags.TruncatedDivide =>
            throw new InvalidOperationException("[Polars.NET.Linq Bug] Modulo / TruncatedDivide are lowered before their operands"),
        BinaryOp.Tags.FloorDivide => PolarsWrapper.FloorDiv(left, right),
        BinaryOp.Tags.Power => PolarsWrapper.Pow(left, right),
        BinaryOp.Tags.StringConcat => throw new InvalidOperationException("[Polars.NET.Linq Bug] StringConcat is lowered before its operands"),
        BinaryOp.Tags.Contains => PolarsWrapper.StrContains(left, right, literal: false, strict: false),
        BinaryOp.Tags.StartsWith => PolarsWrapper.StrStartsWith(left, right),
        BinaryOp.Tags.EndsWith => PolarsWrapper.StrEndsWith(left, right),
        BinaryOp.Tags.Like => PolarsWrapper.StrContains(left, right, literal: true, strict: false),
        BinaryOp.Tags.RegexMatch => PolarsWrapper.StrContains(left, right, literal: false, strict: false),
        _ => throw new ArgumentOutOfRangeException(nameof(op)),
    };

    private static ExprHandle LowerAggregation(AggKind kind, Microsoft.FSharp.Core.FSharpOption<ScalarExpr>? argument, bool isDistinct)
    {
        ExprHandle input;
        if (argument.OrNull() is { } e)
        {
            var h = Lower(e);
            input = isDistinct && !kind.IsCount ? PolarsWrapper.ExprUnique(h) : h;
        }
        else input = PolarsWrapper.Col("*");

        switch (kind)
        {
            case AggKind.Std std: return PolarsWrapper.Std(input, std.ddof);
            case AggKind.Var var: return PolarsWrapper.Var(input, var.ddof);
            case AggKind.Quantile q: return PolarsWrapper.Quantile(input, q.prob, PlQuantileMethod.Nearest);
        }
        return kind.Tag switch
        {
            AggKind.Tags.Sum => PolarsWrapper.Sum(input),
            AggKind.Tags.CheckedSum => PolarsWrapper.DotNetCheckedSum(input),
            // The engine sums no values to 0: null unless a value is counted (distinct or not, the same test).
            AggKind.Tags.SumOrNull => PolarsWrapper.IfElse(
                PolarsWrapper.Gt(Lower(ScalarExpr.NewAggregation(AggKind.Count, argument, false)), LowerLiteral(LiteralValue.NewUInt32(0u))),
                PolarsWrapper.Sum(input),
                PolarsWrapper.LitNull()),
            AggKind.Tags.Mean => PolarsWrapper.Mean(input),
            // IR Min is .NET Min: NaN as soon as one value is NaN (nan_min is min on non-floating values).
            AggKind.Tags.Min => PolarsWrapper.NanMin(input),
            AggKind.Tags.Max => PolarsWrapper.Max(input),
            // The engine's min skips NaN unless every value is NaN; nan_max propagates it.
            AggKind.Tags.MinNanLargest => PolarsWrapper.Min(input),
            AggKind.Tags.MaxNanLargest => PolarsWrapper.NanMax(input),
            AggKind.Tags.Count => isDistinct ? PolarsWrapper.NUnique(input) : PolarsWrapper.Count(input),
            AggKind.Tags.First => PolarsWrapper.First(input, false),
            AggKind.Tags.Last => PolarsWrapper.Last(input, false),
            AggKind.Tags.FirstNonNull => PolarsWrapper.First(input, true),
            AggKind.Tags.LastNonNull => PolarsWrapper.Last(input, true),
            AggKind.Tags.Median => PolarsWrapper.Median(input),
            AggKind.Tags.List => PolarsWrapper.Implode(input, false),
            AggKind.Tags.Len => PolarsWrapper.Len(),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };
    }

    private static ExprHandle LowerWindow(ScalarExpr.Window w)
    {
        var inner = Lower(w.expr);
        var partitions = LowerAll(w.partitionBy);
        // The engine orders a window by its keys in one direction, nulls at one end: keys in mixed directions or null
        // placements are each replaced by its dense rank in its own direction, nulls at its own end.
        IReadOnlyList<SortOrder> orderBy = w.orderBy;
        if (orderBy.Count > 1 && orderBy.Skip(1).Any(o => o.Descending != orderBy[0].Descending || o.NullsLast != orderBy[0].NullsLast))
        {
            orderBy =
            [
                .. orderBy.Select(o =>
                {
                    var dense = ScalarExpr.NewFunction(
                        ScalarFunction.NewRank(Kaminari.Samidare.IR.RankMethod.Dense, null),
                        IrList.Of(o.Expr, ScalarExpr.NewLiteral(LiteralValue.NewBool(o.Descending))),
                        LogicalTypes.UInt32);
                    long nullRank = o.NullsLast ? long.MaxValue : 0L;
                    var key = ScalarExpr.NewFunction(
                        ScalarFunction.FillNull,
                        IrList.Of(ScalarExpr.NewCast(dense, LogicalTypes.Int64, false), ScalarExpr.NewLiteral(LiteralValue.NewInt64(nullRank))),
                        LogicalTypes.Int64);
                    return new SortOrder(key, false, false);
                }),
            ];
        }
        var orders = LowerAll(orderBy.Select(o => o.Expr));
        bool descending = orderBy.Count > 0 && orderBy[0].Descending;
        bool nullsLast = orderBy.Count > 0 && orderBy[0].NullsLast;
        return PolarsWrapper.Over(
            inner,
            partitions,
            orders,
            descending: descending,
            nullsLast: nullsLast,
            multithreaded: true,
            maintainOrder: true,
            mappingCode: PlWindowMapping.GroupsToRows);
    }

    private static ExprHandle LowerNarrowing(ScalarExpr value, LogicalType returnType, bool nullOnOverflow, ScalarExpr? guard = null)
    {
        var target = returnType switch
        {
            LogicalType.Arrow { arrowType: Int8Type } => PlDataType.Int8,
            LogicalType.Arrow { arrowType: Int16Type } => PlDataType.Int16,
            LogicalType.Arrow { arrowType: Int32Type } => PlDataType.Int32,
            LogicalType.Arrow { arrowType: Int64Type } => PlDataType.Int64,
            LogicalType.Arrow { arrowType: UInt8Type } => PlDataType.UInt8,
            LogicalType.Arrow { arrowType: UInt16Type } => PlDataType.UInt16,
            LogicalType.Arrow { arrowType: UInt32Type } => PlDataType.UInt32,
            LogicalType.Arrow { arrowType: UInt64Type } => PlDataType.UInt64,
            var other => throw new InvalidOperationException($"[Polars.NET.Linq Bug] a checked narrowing to {other}"),
        };
        ExprHandle valueH;
        if (value is ScalarExpr.Aggregation
            {
                kind.IsSum: true,
                expr.Value: ScalarExpr.Cast
                {
                    expr: ScalarExpr.Cast { targetType: LogicalType.Arrow { arrowType: Int64Type } } integers,
                    targetType: LogicalType.Arrow { arrowType: Decimal128Type { Scale: 0 } },
                },
            } sum)
        {
            using var int128 = PolarsWrapper.NewPrimitiveType((int)PlDataType.Int128);
            var cast = PolarsWrapper.ExprCast(Lower(integers), PolarsWrapper.DataTypeExprFromDataType(int128), strict: false, wrapNumerical: false);
            valueH = PolarsWrapper.Sum(sum.isDistinct ? PolarsWrapper.ExprUnique(cast) : cast);
        }
        else valueH = Lower(value);
        return PolarsWrapper.DotNetCheckedNarrow(Guarded(valueH, guard), target, nullOnOverflow: nullOnOverflow);
    }

    /// <summary>
    /// The input of a raising kernel restricted to its guard's rows: null where the guard is not true (a null input
    /// never raises), unchanged without a guard or under literal true.
    /// </summary>
    private static ExprHandle Guarded(ExprHandle input, ScalarExpr? guard) => guard switch
    {
        null or ScalarExpr.Literal { value: LiteralValue.Bool { Item: true } } => input,
        _ => PolarsWrapper.IfElse(Lower(guard), input, PolarsWrapper.LitNull()),
    };

    private static ExprHandle LowerUdf(ExprHandle[] args, UdfInvoker invoker)
    {
        switch (invoker)
        {
            case UdfInvoker.SingleBatch s:
                return PolarsWrapper.Map(args[0], (Func<IArrowArray, IArrowArray>)s.callback, NativeDataTypeResolver.GetDataTypeHandle(s.clrReturnType));
            case UdfInvoker.MultiBatch m:
                return PolarsWrapper.MapMany(args[0], args[1..], (Func<IReadOnlyList<IArrowArray>, IArrowArray>)m.callback, NativeDataTypeResolver.GetDataTypeHandle(m.clrReturnType));
            case UdfInvoker.GroupSingleBatch g:
                return PolarsWrapper.Apply(args[0], (Func<IArrowArray, IArrowArray>)g.callback, NativeDataTypeResolver.GetDataTypeHandle(g.clrReturnType), returnsScalar: g.returnsScalar);
            case UdfInvoker.GroupMultiBatch g:
                return PolarsWrapper.ApplyMany(args[0], args[1..], (Func<IReadOnlyList<IArrowArray>, IArrowArray>)g.callback, NativeDataTypeResolver.GetDataTypeHandle(g.clrReturnType), returnsScalar: g.returnsScalar);
            case UdfInvoker.StringPredicate p:
                return PolarsWrapper.MapStringPredicate(args[0], p.predicate);
            case UdfInvoker.StringTransform t:
                return PolarsWrapper.MapStringTransform(args[0], t.transform);
            case UdfInvoker.StringToList l:
                return PolarsWrapper.MapStringToList(args[0], l.mapper);
            default:
                throw new ArgumentOutOfRangeException(nameof(invoker));
        }
    }

    private static string? StringLiteral(ScalarExpr e) => e is ScalarExpr.Literal { value: LiteralValue.String s } ? s.Item : null;

    private static ExprHandle LowerFunction(ScalarFunction fn, FSharpList<ScalarExpr> args)
    {
        ExprHandle L(ScalarExpr e) => Lower(e);
        switch (fn, args)
        {
            // Math
            case ({ IsAbs: true }, [var a]): return PolarsWrapper.Abs(L(a));
            case (ScalarFunction.Round r, [var a]): return PolarsWrapper.Round(L(a), 0u, PolarsVocabulary.RoundMode(r.mode));
            case (ScalarFunction.Round r, [var a, ScalarExpr.Literal decimals]):
            {
                uint digits = decimals.value switch
                {
                    LiteralValue.Int32 d => (uint)Math.Max(0, d.Item),
                    LiteralValue.Int64 d => (uint)Math.Max(0L, d.Item),
                    LiteralValue.UInt32 d => d.Item,
                    LiteralValue.UInt8 d => d.Item,
                    LiteralValue.Int16 d => (uint)Math.Max((short)0, d.Item),
                    _ => 0u,
                };
                return PolarsWrapper.Round(L(a), digits, PolarsVocabulary.RoundMode(r.mode));
            }
            case ({ IsFloor: true }, [var a]): return PolarsWrapper.Floor(L(a));
            case ({ IsCeil: true }, [var a]): return PolarsWrapper.Ceil(L(a));
            case ({ IsTruncate: true }, [var a]): return PolarsWrapper.Truncate(L(a), 0u);
            case ({ IsSign: true }, [var a]): return PolarsWrapper.Sign(L(a));
            case ({ IsSqrt: true }, [var a]): return PolarsWrapper.Sqrt(L(a));
            case ({ IsCbrt: true }, [var a]): return PolarsWrapper.Cbrt(L(a));
            case ({ IsExp: true }, [var a]): return PolarsWrapper.Exp(L(a));
            case ({ IsLog: true }, [var a]): return PolarsWrapper.Log(L(a), PolarsWrapper.Lit(Math.E));
            case ({ IsLog: true }, [var a, var b]): return PolarsWrapper.Log(L(a), L(b));
            case ({ IsPow: true }, [var a, var b]): return PolarsWrapper.Pow(L(a), L(b));
            case ({ IsSin: true }, [var a]): return PolarsWrapper.Sin(L(a));
            case ({ IsCos: true }, [var a]): return PolarsWrapper.Cos(L(a));
            case ({ IsTan: true }, [var a]): return PolarsWrapper.Tan(L(a));
            case ({ IsAsin: true }, [var a]): return PolarsWrapper.ArcSin(L(a));
            case ({ IsAcos: true }, [var a]): return PolarsWrapper.ArcCos(L(a));
            case ({ IsAtan: true }, [var a]): return PolarsWrapper.ArcTan(L(a));
            case ({ IsAtan2: true }, [var y, var x]): return PolarsWrapper.ArcTan2(L(y), L(x));
            case ({ IsSinh: true }, [var a]): return PolarsWrapper.Sinh(L(a));
            case ({ IsCosh: true }, [var a]): return PolarsWrapper.Cosh(L(a));
            case ({ IsTanh: true }, [var a]): return PolarsWrapper.Tanh(L(a));
            case ({ IsClamp: true }, [var v, var min, var max]): return PolarsWrapper.Clip(L(v), L(min), L(max));

            // String
            case ({ IsToUpper: true }, [var a]): return PolarsWrapper.StrToUpper(L(a));
            case ({ IsToLower: true }, [var a]): return PolarsWrapper.StrToLower(L(a));
            case ({ IsToTitlecase: true }, [var a]): return PolarsWrapper.StrToTitlecase(L(a));
            case ({ IsLenChars: true }, [var a]): return PolarsWrapper.StrLenChars(L(a));
            case ({ IsLenBytes: true }, [var a]): return PolarsWrapper.StrLenBytes(L(a));
            case ({ IsReverse: true }, [var a]): return PolarsWrapper.StrReverse(L(a));
            case ({ IsSlice: true }, [var a, var offset, var length]): return PolarsWrapper.StrSlice(L(a), L(offset), L(length));
            case ({ IsSplitExact: true }, [var a, var by, ScalarExpr.Literal { value: LiteralValue.Int32 n }]):
                return PolarsWrapper.StrSplitExact(L(a), L(by), n.Item, inclusive: false);
            case ({ IsReplace: true }, [var a, var pat, var v, ScalarExpr.Literal { value: LiteralValue.Int64 n }]):
                return PolarsWrapper.StrReplace(L(a), L(pat), L(v), literal: false, n: n.Item);
            case ({ IsReplaceAll: true }, [var a, var pat, var v]):
                return PolarsWrapper.StrReplaceAll(L(a), L(pat), L(v), literal: false);
            case ({ IsReplaceAll: true }, [var a, var pat, var v, ScalarExpr.Literal { value: LiteralValue.Bool literal }]):
                return PolarsWrapper.StrReplaceAll(L(a), L(pat), L(v), literal: literal.Item);
            case ({ IsStrip: true }, [var a, var m]): return PolarsWrapper.StrStripChars(L(a), L(m));
            case ({ IsStripStart: true }, [var a, var m]): return PolarsWrapper.StrStripCharsStart(L(a), L(m));
            case ({ IsStripEnd: true }, [var a, var m]): return PolarsWrapper.StrStripCharsEnd(L(a), L(m));
            case ({ IsFormatString: true }, [var format, ..]):
                return PolarsWrapper.FormatString(
                    StringLiteral(format) ?? throw new InvalidOperationException("format must be string literal"),
                    [.. args.Tail.Select(LowerConcatItem)]);
            case ({ IsConcatString: true }, [var separator, ..]):
                return PolarsWrapper.ConcatString([.. args.Tail.Select(LowerConcatItem)], StringLiteral(separator) ?? "", ignoreNulls: false);
            case ({ IsFillNull: true }, [var e, var v]): return PolarsWrapper.FillNull(L(e), L(v));
            case ({ IsPadStart: true }, [var t, var w, ScalarExpr.Literal { value: LiteralValue.String pad }]):
                return PolarsWrapper.StrPadStart(L(t), L(w), pad.Item);
            case ({ IsPadEnd: true }, [var t, var w, ScalarExpr.Literal { value: LiteralValue.String pad }]):
                return PolarsWrapper.StrPadEnd(L(t), L(w), pad.Item);
            case (ScalarFunction.Normalize n, [var t]): return PolarsWrapper.StrNormalize(L(t), n.form);
            case ({ IsStrContains: true }, [var t, var p, ScalarExpr.Literal { value: LiteralValue.Bool literal }]):
                return PolarsWrapper.StrContains(L(t), L(p), literal: literal.Item, strict: false);
            case ({ IsStartsWith: true }, [var t, var p]): return PolarsWrapper.StrStartsWith(L(t), L(p));
            case ({ IsEndsWith: true }, [var t, var p]): return PolarsWrapper.StrEndsWith(L(t), L(p));
            case ({ IsStrFind: true }, [var t, var p, ScalarExpr.Literal { value: LiteralValue.Bool literal }]):
                return PolarsWrapper.StrFind(L(t), L(p), literal: literal.Item, strict: false);
            case ({ IsStrSplit: true }, [var t, var by, ScalarExpr.Literal { value: LiteralValue.Bool literal }]):
                return PolarsWrapper.StrSplit(L(t), L(by), inclusive: false, literal: literal.Item, strict: false);
            case ({ IsStrSplit: true }, [var t, var by]):
                return PolarsWrapper.StrSplit(L(t), L(by), inclusive: false, literal: true, strict: false);
            case ({ IsStrSplitN: true }, [var t, var by, ScalarExpr.Literal { value: LiteralValue.Int32 n }]):
            {
                // The native split pads a struct with nulls up to n fields; the IR's list holds only the pieces.
                ExprHandle[] fields = [.. Enumerable.Range(0, n.Item).Select(i => PolarsWrapper.StructFieldByIndex(PolarsWrapper.StrSplitN(L(t), L(by), n.Item), i))];
                return PolarsWrapper.ListDropNulls(PolarsWrapper.ConcatList(fields));
            }
            case ({ IsStrExtract: true }, [var t, var pat, ScalarExpr.Literal { value: LiteralValue.Int32 index }]):
                return PolarsWrapper.StrExtract(L(t), L(pat), index.Item);
            case ({ IsStrExtractGroups: true }, [var t, ScalarExpr.Literal { value: LiteralValue.String pat }]):
                return PolarsWrapper.StrExtractGroups(L(t), pat.Item);
            case ({ IsStrExtractAll: true }, [var t, var pat]): return PolarsWrapper.StrExtractAll(L(t), L(pat));
            case ({ IsStrCountMatches: true }, [var t, var pat, ScalarExpr.Literal { value: LiteralValue.Bool literal }]):
                return PolarsWrapper.StrCountMatches(L(t), L(pat), literal: literal.Item);

            // Temporal
            case ({ IsDtYear: true }, [var a]): return PolarsWrapper.DtYear(L(a));
            case ({ IsDtMonth: true }, [var a]): return PolarsWrapper.DtMonth(L(a));
            case ({ IsDtDay: true }, [var a]): return PolarsWrapper.DtDay(L(a));
            case ({ IsDtOrdinalDay: true }, [var a]): return PolarsWrapper.DtOrdinalDay(L(a));
            case ({ IsDtWeekday: true }, [var a]): return PolarsWrapper.DtWeekday(L(a));
            case ({ IsDtHour: true }, [var a]): return PolarsWrapper.DtHour(L(a));
            case ({ IsDtMinute: true }, [var a]): return PolarsWrapper.DtMinute(L(a));
            case ({ IsDtSecond: true }, [var a]): return PolarsWrapper.DtSecond(L(a));
            case ({ IsDtMillisecond: true }, [var a]): return PolarsWrapper.DtMillisecond(L(a));
            case ({ IsDtMicrosecond: true }, [var a]): return PolarsWrapper.DtMicrosecond(L(a));
            case ({ IsDtNanosecond: true }, [var a]): return PolarsWrapper.DtNanosecond(L(a));
            case ({ IsDtDate: true }, [var a]): return PolarsWrapper.DtDate(L(a));
            case ({ IsDtTime: true }, [var a]): return PolarsWrapper.DtTime(L(a));
            case ({ IsDtTotalDays: true }, [var a]): return PolarsWrapper.DtTotalDays(L(a), fractional: true);
            case ({ IsDtTotalHours: true }, [var a]): return PolarsWrapper.DtTotalHours(L(a), fractional: true);
            case ({ IsDtTotalMinutes: true }, [var a]): return PolarsWrapper.DtTotalMinutes(L(a), fractional: true);
            case ({ IsDtTotalSeconds: true }, [var a]): return PolarsWrapper.DtTotalSeconds(L(a), fractional: true);
            case ({ IsDtTotalMilliseconds: true }, [var a]): return PolarsWrapper.DtTotalMilliseconds(L(a), fractional: true);
            case ({ IsDtTotalMicroseconds: true }, [var a]): return PolarsWrapper.DtTotalMicroseconds(L(a), fractional: true);
            case ({ IsDtTotalNanoseconds: true }, [var a]): return PolarsWrapper.DtTotalNanoseconds(L(a), fractional: true);
            case ({ IsStrToDatetime: true }, [var e, ScalarExpr.Literal { value: LiteralValue.String format }, ScalarExpr.Literal { value: LiteralValue.Bool exact }]):
            {
                string? chrono = string.IsNullOrEmpty(format.Item) ? null : DateTimeFormatHelper.ToChronoFormat(format.Item);
                return PolarsWrapper.StrToDatetime(e: L(e), unit: PlTimeUnit.Microseconds, timeZone: null!, format: chrono!, strict: false, exact: exact.Item, cache: true, ambiguous: PolarsWrapper.Lit("raise"));
            }
            case ({ IsStrToDatetime: true }, [var e, ScalarExpr.Literal { value: LiteralValue.Null }, ScalarExpr.Literal { value: LiteralValue.Bool exact }]):
                return PolarsWrapper.StrToDatetime(e: L(e), unit: PlTimeUnit.Microseconds, timeZone: null!, format: null!, strict: false, exact: exact.Item, cache: true, ambiguous: PolarsWrapper.Lit("raise"));
            case ({ IsStrToDate: true }, [var e, ScalarExpr.Literal { value: LiteralValue.String format }, ScalarExpr.Literal { value: LiteralValue.Bool exact }]):
                return PolarsWrapper.StrToDate(L(e), DateTimeFormatHelper.ToChronoFormat(format.Item), strict: false, exact: exact.Item, cache: true);
            case ({ IsStrToDate: true }, [var e, ScalarExpr.Literal { value: LiteralValue.Null }, ScalarExpr.Literal { value: LiteralValue.Bool exact }]):
                return PolarsWrapper.StrToDate(L(e), format: null!, strict: false, exact: exact.Item, cache: true);
            case ({ IsStrToTime: true }, [var e, ScalarExpr.Literal { value: LiteralValue.String format }, ScalarExpr.Literal { value: LiteralValue.Bool exact }]):
                return PolarsWrapper.StrToTime(L(e), DateTimeFormatHelper.ToChronoFormat(format.Item), strict: false, exact: exact.Item, cache: true);
            case ({ IsStrToTime: true }, [var e, ScalarExpr.Literal { value: LiteralValue.Null }, ScalarExpr.Literal { value: LiteralValue.Bool exact }]):
                return PolarsWrapper.StrToTime(L(e), format: null!, strict: false, exact: exact.Item, cache: true);
            case ({ IsDtIsLeapYear: true }, [var e]): return PolarsWrapper.DtIsLeapYear(L(e));
            case ({ IsDtToString: true }, [var e, ScalarExpr.Literal { value: LiteralValue.String format }]):
                return PolarsWrapper.DtToString(L(e), DateTimeFormatHelper.ToChronoFormat(format.Item)!);
            case (ScalarFunction.DtToText text, [var e]):
                return PolarsDateText.TryStrftime(text.format, text.culture) is { } strftime
                    ? PolarsWrapper.DtToString(L(e), strftime)
                    : throw new InvalidOperationException($"[Polars.NET.Linq Bug] date text '{text.format}' of culture '{text.culture}', which IsDateTextSupported refuses");
            case (ScalarFunction.FloatToText text, [var e]):
            {
                var symbols = text.symbols;
                return PolarsWrapper.DotNetFloatToString(
                    L(e), symbols.NegativeSign, symbols.PositiveSign, symbols.DecimalSeparator, symbols.NaN, symbols.PositiveInfinity, symbols.NegativeInfinity);
            }
            case ({ IsDtOffsetBy: true }, [var e, ScalarExpr.Literal { value: LiteralValue.TimeSpan offset }]):
                return PolarsWrapper.DtOffsetBy(L(e), PolarsWrapper.Lit(DurationFormatter.ToPolarsString(offset.Item)));
            // The engine's calendar offset clamps the day to the month's last.
            case ({ IsDtOffsetMonths: true }, [var e, ScalarExpr.Literal { value: LiteralValue.Int32 months }]):
                return PolarsWrapper.DtOffsetBy(L(e), PolarsWrapper.Lit($"{months.Item}mo"));

            // Struct & List
            case ({ IsStructFieldByIndex: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Int64 index }]):
                return PolarsWrapper.StructFieldByIndex(L(a), index.Item);
            case ({ IsStructFieldByIndex: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Int32 index }]):
                return PolarsWrapper.StructFieldByIndex(L(a), index.Item);
            case ({ IsStructFieldByName: true }, [var a, ScalarExpr.Literal { value: LiteralValue.String name }]):
                return PolarsWrapper.StructFieldByName(L(a), [name.Item]);
            case ({ IsListGet: true }, [var a, var index]): return PolarsWrapper.ListGet(L(a), L(index), nullOnOob: true);
            case (ScalarFunction.ListContains c, [var a, var item]): return PolarsWrapper.ListContains(L(a), L(item), nullsEqual: c.nullsEqual);
            case ({ IsListLen: true }, [var a]): return PolarsWrapper.ListLen(L(a));
            case (ScalarFunction.IsIn i, [var a, var b]): return PolarsWrapper.IsIn(L(a), L(b), nullsEqual: i.nullsEqual);
            case ({ IsListSum: true }, [var a]): return PolarsWrapper.ListSum(L(a));
            case ({ IsListMean: true }, [var a]): return PolarsWrapper.ListMean(L(a));
            case ({ IsListMin: true }, [var a]): return PolarsWrapper.ListMin(L(a));
            case ({ IsListMax: true }, [var a]): return PolarsWrapper.ListMax(L(a));
            case ({ IsListJoin: true }, [var a, ScalarExpr.Literal { value: LiteralValue.String separator }]):
            {
                // Elements join as their string form (an Enum as its name), a null as "".
                var emptyForNull = LowerConcatItem(ScalarExpr.NewColumn(""));
                return PolarsWrapper.ListJoin(PolarsWrapper.ListEval(L(a), emptyForNull), separator.Item, ignoreNulls: false);
            }
            case ({ IsListHead: true }, [var a, var c]): return PolarsWrapper.ListHead(L(a), L(c));
            case ({ IsListTail: true }, [var a, var c]): return PolarsWrapper.ListTail(L(a), L(c));
            case ({ IsListSlice: true }, [var a, var offset, var length]): return PolarsWrapper.ListSlice(L(a), L(offset), L(length));
            case ({ IsListUnique: true }, [var a]): return PolarsWrapper.ListEval(L(a), PolarsWrapper.ExprUnique(PolarsWrapper.Col("")));
            case ({ IsListReverse: true }, [var a]): return PolarsWrapper.ListEval(L(a), PolarsWrapper.Reverse(PolarsWrapper.Col("")));
            case ({ IsListSort: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Bool descending }]):
                return PolarsWrapper.ListSort(L(a), descending: descending.Item, nullsLast: false, maintainOrder: false);
            case ({ IsListIntersection: true }, [var a, var b]): return PolarsWrapper.ListSetIntersection(L(a), L(b));
            case ({ IsListUnion: true }, [var a, var b]): return PolarsWrapper.ListSetUnion(L(a), L(b));
            case ({ IsListDifference: true }, [var a, var b]): return PolarsWrapper.ListSetDifference(L(a), L(b));
            case ({ IsListAny: true }, [var t, var condition]):
                return PolarsWrapper.ListGet(PolarsWrapper.ListEval(L(t), PolarsWrapper.Any(L(condition), ignoreNulls: false)), PolarsWrapper.Lit(0), nullOnOob: true);
            case ({ IsListAll: true }, [var t, var condition]):
                return PolarsWrapper.ListGet(PolarsWrapper.ListEval(L(t), PolarsWrapper.All(L(condition), ignoreNulls: false)), PolarsWrapper.Lit(0), nullOnOob: true);
            case ({ IsListEval: true }, [var t, var mapper]): return PolarsWrapper.ListEval(L(t), L(mapper));

            // JSON
            case ({ IsStrJsonPathMatch: true }, [var a, var b]): return PolarsWrapper.StrJsonPathMatch(L(a), L(b));
            case ({ IsStructJsonEncode: true }, [var a]): return PolarsWrapper.StructJsonEncode(L(a));
            case ({ IsAsStruct: true }, _): return PolarsWrapper.AsStruct(LowerAll(args));
            case (ScalarFunction.StrJsonDecode decode, [var a]):
            {
                using var dt = LowerJsonDataType(decode.targetType);
                return PolarsWrapper.StrJsonDecode(L(a), PolarsWrapper.DataTypeExprFromDataType(dt));
            }

            // Window & Extensions
            case ({ IsShift: true }, [var a, var b]): return PolarsWrapper.Shift(L(a), L(b));
            // The engine's rolling groups: the window (index + offset, index + offset + length] of an integer index.
            case (ScalarFunction.RollingWindow rolling, [var aggregate, ScalarExpr.Column index]):
                return PolarsWrapper.ExprRolling(L(aggregate), PolarsWrapper.Col(index.name), $"{rolling.length}i", $"{rolling.offset}i", PlClosedInterval.Right);
            case (ScalarFunction.Diff diff, [var a, var b]): return PolarsWrapper.Diff(L(a), L(b), PolarsVocabulary.NullBehavior(diff.nullBehavior));
            case (ScalarFunction.Rank rank, [var a, ScalarExpr.Literal { value: LiteralValue.Bool descending }]):
                return PolarsWrapper.Rank(L(a), PolarsVocabulary.RankMethod(rank.method), descending.Item, rank.seed.ToNullable());
            case ({ IsPctChange: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Int64 n }]): return PolarsWrapper.PctChange(L(a), n.Item);
            case (ScalarFunction.Interpolate interpolate, [var a]): return PolarsWrapper.Interpolate(L(a), PolarsVocabulary.InterpolationMethod(interpolate.method));
            case ({ IsInterpolateBy: true }, [var a, var b]): return PolarsWrapper.InterpolateBy(L(a), L(b));
            case ({ IsCumSum: true }, [var a]): return PolarsWrapper.CumSum(L(a), false);
            case ({ IsCumSum: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Bool reverse }]): return PolarsWrapper.CumSum(L(a), reverse.Item);
            case ({ IsCumMin: true }, [var a]): return PolarsWrapper.CumMin(L(a), false);
            case ({ IsCumMin: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Bool reverse }]): return PolarsWrapper.CumMin(L(a), reverse.Item);
            case ({ IsCumMax: true }, [var a]): return PolarsWrapper.CumMax(L(a), false);
            case ({ IsForwardFill: true }, [var a]): return PolarsWrapper.FillNullWithStrategy(L(a), PlFillNullStrategy.Forward, null);
            case ({ IsCumMax: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Bool reverse }]): return PolarsWrapper.CumMax(L(a), reverse.Item);
            case ({ IsCumProd: true }, [var a]): return PolarsWrapper.CumProd(L(a), false);
            case ({ IsCumProd: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Bool reverse }]): return PolarsWrapper.CumProd(L(a), reverse.Item);
            case ({ IsCumCount: true }, [var a]): return PolarsWrapper.CumCount(L(a), false);
            case ({ IsCumCount: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Bool reverse }]): return PolarsWrapper.CumCount(L(a), reverse.Item);
            case ({ IsRegexEscape: true }, [var a]): return PolarsWrapper.StrEscapeRegex(L(a));
            case ({ IsHead: true }, [var a, var b]): return PolarsWrapper.StrHead(L(a), L(b));
            case ({ IsTail: true }, [var a, var b]): return PolarsWrapper.StrTail(L(a), L(b));
            case (ScalarFunction.StrToInteger toInteger, [var a, var b]):
                return PolarsWrapper.StrToInteger(L(a), L(b), PolarsVocabulary.ToDataTypeHandle(toInteger.targetType), strict: false);
            case ({ IsStrToDecimal: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Int32 scale }]): return PolarsWrapper.StrToDecimal(L(a), scale.Item);
            case ({ IsHexEncode: true }, [var a]): return PolarsWrapper.StrHexEncode(L(a));
            case ({ IsHexDecode: true }, [var a]): return PolarsWrapper.StrHexDecode(L(a), false);
            case ({ IsBase64Encode: true }, [var a]): return PolarsWrapper.StrBase64Encode(L(a));
            case ({ IsBase64Decode: true }, [var a]): return PolarsWrapper.StrBase64Decode(L(a), false);

            // Bitwise operations
            case ({ IsBitLeftShift: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Int32 n }]): return PolarsWrapper.BitLeftShift(L(a), n.Item);
            case ({ IsBitRightShift: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Int32 n }]): return PolarsWrapper.BitRightShift(L(a), n.Item);
            case ({ IsBitwiseCountOnes: true }, [var a]): return PolarsWrapper.BitwiseCountOnes(L(a));
            case ({ IsBitwiseLeadingZeros: true }, [var a]): return PolarsWrapper.BitwiseLeadingZeros(L(a));
            case ({ IsBitwiseTrailingZeros: true }, [var a]): return PolarsWrapper.BitwiseTrailingZeros(L(a));

            // Series & Group-Level Operations
            case ({ IsImplode: true }, [var a]): return PolarsWrapper.Implode(L(a), maintainOrder: true);
            case ({ IsImplode: true }, [var a, ScalarExpr.Literal { value: LiteralValue.Bool maintainOrder }]): return PolarsWrapper.Implode(L(a), maintainOrder: maintainOrder.Item);
            case ({ IsNUnique: true }, [var a]): return PolarsWrapper.NUnique(L(a));
            case ({ IsUnique: true }, [var a]): return PolarsWrapper.ExprUnique(L(a));
            case ({ IsMinBy: true }, [var a, var by]): return PolarsWrapper.MinBy(L(a), L(by));
            case ({ IsMaxBy: true }, [var a, var by]): return PolarsWrapper.MaxBy(L(a), L(by));
            case ({ IsFilter: true }, [var a, var predicate]): return PolarsWrapper.Filter(L(a), L(predicate));
            case (ScalarFunction.Quantile quantile, [var a, ScalarExpr.Literal { value: LiteralValue.Float64 q }]):
                return PolarsWrapper.Quantile(L(a), q.Item, PolarsVocabulary.QuantileMethod(quantile.method));
            case ({ IsSeriesGet: true }, [var a, var index]): return PolarsWrapper.Get(L(a), L(index), nullOnOob: true);
            case ({ IsFloatOrderKey: true }, [var value]): return PolarsWrapper.DotNetFloatOrderKey(L(value));
            // Off the guard's rows the value is null, which is always listed (SEMANTICS.md, Guards).
            case (ScalarFunction.RequireListed listed, [var value, var guard]):
            {
                List<long> asInt64 = [];
                foreach (var v in listed.values)
                    if (v.BoxedValue is not ulong u || u <= long.MaxValue) asInt64.Add(Convert.ToInt64(v.BoxedValue));
                // Values beyond Int64 (a UInt64 enum): the engine's own unmapped-value error remains.
                return asInt64.Count == listed.values.Length
                    ? PolarsWrapper.RequireListedInts(Guarded(L(value), guard), [.. asInt64], listed.message)
                    : L(value);
            }
            case (ScalarFunction.AssertCount assert, [var count, var value]):
            {
                var ok = PolarsWrapper.Map(L(count), CountChecker(assert.check), NativeDataTypeResolver.GetDataTypeHandle(typeof(bool)));
                return PolarsWrapper.IfElse(ok, L(value), PolarsWrapper.LitNull());
            }

            default:
                throw new InvalidOperationException($"Unsupported function in PolarsScalarLowerer: {fn.Name}");
        }
    }

    /// <summary>
    /// Lowers a CLR constant of the given type, to be stored into a column of <paramref name="columnType"/>, to a
    /// native literal ExprHandle (an enum constant takes the column's representation).
    /// </summary>
    public static ExprHandle LowerConstant(object? value, Type t, LogicalType columnType) =>
        Lower(PlanOptimizer.ResolveEnumAssignment(LogicalSchema.Empty, columnType, ScalarExprOps.toLiteral(value, t)));
}
