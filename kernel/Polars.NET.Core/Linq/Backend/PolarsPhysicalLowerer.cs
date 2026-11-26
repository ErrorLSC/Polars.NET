// The LINQ provider builds generic code at run time (in-memory rows of element types known only then): its public
// entry points carry RequiresDynamicCode / RequiresUnreferencedCode, so the analyzer's warnings inside it are silenced
// here.
#pragma warning disable IL2026, IL2060, IL2067, IL2070, IL2072, IL2075, IL2080, IL2087, IL2090, IL2091, IL3050, IL3051

using System.Runtime.CompilerServices;
using Apache.Arrow.Types;
using Kaminari.Samidare;
using Kaminari.Samidare.Config;
using Kaminari.Samidare.IR;
using Microsoft.FSharp.Collections;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>Physical lowerer that translates LogicalPlan trees into native Polars operations.</summary>
internal static class PolarsPhysicalLowerer
{
    private static LazyFrameHandle CreateEmptyLazyFrame() => PolarsWrapper.DataFrameToLazy(PolarsWrapper.DataFrameNew([]));

    /// <summary>The schema, with column types, of a physical LazyFrameHandle.</summary>
    public static LogicalSchema SchemaOfLazyFrame(LazyFrameHandle lf) => PolarsTableSources.SchemaOf(lf);

    /// <summary>Wraps a physical LazyFrameHandle as an opaque ILogicalTableSource (schema, with column types, read from the frame).</summary>
    public static ILogicalTableSource TableSourceOfLazyFrame(LazyFrameHandle lf) => PolarsTableSources.OfLazyFrame(lf);

    /// <summary>
    /// Materializes an in-memory row snapshot into a LazyFrame, verifying that the transposed columns match the schema
    /// the frontend declared for the Scan node.
    /// </summary>
    private static LazyFrameHandle MaterializeInMemory(System.Array rows, Type elemType, LogicalSchema declared)
    {
        var df = (DataFrameHandle)MaterializerInvokers.FromRowsMethod.MakeGenericMethod(elemType).Invoke(null, [rows])!;
        var lf = PolarsWrapper.DataFrameToLazy(df);
        string[] actual = PolarsSchemaExtractor.GetColumnNames(lf);
        if (!actual.SequenceEqual(declared.ColumnNames))
            throw new InvalidOperationException(
                $"[Polars.NET.Linq Bug] InMemory({elemType.Name}) declared columns [{string.Join(", ", declared.ColumnNames)}] but FromRows produced [{string.Join(", ", actual)}].");
        return lf;
    }

    // Native schemas resolved while lowering (a union aligns its sides, a grouping-sets node types its null keys), by
    // plan node. Resolving a schema makes Polars convert and type the node's whole input, which dominates lowering a
    // large plan. A cached plan is lowered again on every hit, with the same nodes over tables whose schemas the cache
    // key fixes, so the schemas resolved the first time hold; they are reused only when every source is a table whose
    // schema the key pins exactly (no Unknown column type).
    private static readonly ConditionalWeakTable<LogicalPlan, object> ResolvedSchemas = new();
    private static readonly ConditionalWeakTable<SourceBinding[], object> SchemasPinned = new();

    private static bool IsPinnedType(LogicalType t) => t switch
    {
        LogicalType.ListOf list => IsPinnedType(list.element),
        LogicalType.StructOf st => st.fields.All(f => IsPinnedType(f.Item2)),
        LogicalType.Arrow or LogicalType.Enum => true,
        _ => false,
    };

    private static bool SourcesPinned(SourceBinding[] sources) =>
        (bool)SchemasPinned.GetValue(sources, static sources =>
            sources.All(s => s is SourceBinding.Table table && table.source.Schema.Fields.All(f => IsPinnedType(f.DataType))));

    /// <summary>The schemas <paramref name="resolve"/> reads for <paramref name="node"/>, resolved once per node when the sources pin them.</summary>
    private static T NodeSchemas<T>(SourceBinding[] sources, LogicalPlan node, Func<T> resolve) =>
        SourcesPinned(sources) ? (T)ResolvedSchemas.GetValue(node, _ => resolve()!) : resolve();

    /// <summary>
    /// Whether a slice over this plan would reach an order-keeping GroupBy through nodes Polars pushes a slice through
    /// (projections, renames), i.e. whether the slice selects groups by their order.
    /// </summary>
    private static bool SlicesOrderedGroups(LogicalPlan plan) => plan switch
    {
        LogicalPlan.GroupBy g => g.maintainOrder,
        LogicalPlan.Project p => SlicesOrderedGroups(p.input),
        LogicalPlan.WithColumns w => SlicesOrderedGroups(w.input),
        LogicalPlan.Rename r => SlicesOrderedGroups(r.input),
        _ => false,
    };

    /// <summary>
    /// The branch a conditional takes whatever the row, when its test reads literals alone (SQL's guard against a
    /// literal divisor of zero). A cast of a decimal literal only types it: the comparison of the values decides.
    /// </summary>
    private static ScalarExpr? DecidedBranch(ScalarExpr test, ScalarExpr ifTrue, ScalarExpr ifFalse)
    {
        static ScalarExpr UncastLiterals(ScalarExpr e) => e switch
        {
            ScalarExpr.Cast { expr: ScalarExpr.Literal { value: LiteralValue.Decimal } literal, targetType: LogicalType.Arrow { arrowType: Decimal128Type } } => literal,
            ScalarExpr.Binary b => ScalarExpr.NewBinary(b.op, UncastLiterals(b.left), UncastLiterals(b.right)),
            _ => e,
        };
        return FilterShapes.foldConstants(UncastLiterals(test)) switch
        {
            ScalarExpr.Literal { value: LiteralValue.Bool { Item: true } } => ifTrue,
            ScalarExpr.Literal { value: LiteralValue.Bool { Item: false } or LiteralValue.Null } => ifFalse,
            _ => null,
        };
    }

    /// <summary>Whether an expression is no column or literal (or a cast of one): computing it costs.</summary>
    private static bool IsComputed(ScalarExpr expr) => expr switch
    {
        ScalarExpr.Column or ScalarExpr.Literal => false,
        ScalarExpr.Cast c => IsComputed(c.expr),
        _ => true,
    };

    /// <summary>
    /// The columns an expression is null exactly when one of them is (arithmetic that passes nulls on: strict casts and
    /// casts that keep every value, addition, subtraction, multiplication, negation, division by a literal that is not
    /// zero), if it is such an expression; null otherwise.
    /// </summary>
    private static List<string>? NullsFrom(Lazy<LogicalSchema> schema, ScalarExpr expr)
    {
        bool Passes(ScalarExpr e)
        {
            switch (e)
            {
                case ScalarExpr.Column: return true;
                case ScalarExpr.Literal { value: LiteralValue.Null }: return false;
                case ScalarExpr.Literal: return true;
                // A strict cast raises where it cannot convert: it never makes a value null.
                case ScalarExpr.Cast { isStrict: true } c: return Passes(c.expr);
                case ScalarExpr.Cast c:
                {
                    var source = IrOps.InferType(schema.Value, c.expr);
                    // A decimal to one with as many digits before the point and as many after: every value fits.
                    bool decimalFits = (source, c.targetType) is (LogicalType.Arrow { arrowType: Decimal128Type f }, LogicalType.Arrow { arrowType: Decimal128Type t })
                        && t.Scale >= f.Scale && t.Precision - t.Scale >= f.Precision - f.Scale;
                    return (decimalFits || FilterShapes.castKeepsValuesOf(source, c.targetType)) && Passes(c.expr);
                }
                case ScalarExpr.Binary { op.IsAdd: true } or ScalarExpr.Binary { op.IsSubtract: true } or ScalarExpr.Binary { op.IsMultiply: true }:
                {
                    var b = (ScalarExpr.Binary)e;
                    return Passes(b.left) && Passes(b.right);
                }
                case ScalarExpr.Binary { op.IsDivide: true } d:
                {
                    var divisor = d.right is ScalarExpr.Conditional c ? DecidedBranch(c.test, c.ifTrue, c.ifFalse) ?? d.right : d.right;
                    bool nonZeroLiteral =
                        FilterShapes.foldConstants(ScalarExpr.NewBinary(BinaryOp.NotEq, divisor, ScalarExpr.NewLiteral(LiteralValue.NewInt32(0))))
                            is ScalarExpr.Literal { value: LiteralValue.Bool { Item: true } }
                        || divisor switch
                        {
                            ScalarExpr.Literal { value: LiteralValue.Decimal dec } => dec.Item != 0m,
                            ScalarExpr.Cast { expr: ScalarExpr.Literal { value: LiteralValue.Decimal dec } } => dec.Item != 0m,
                            _ => false,
                        };
                    return nonZeroLiteral && Passes(d.left);
                }
                case ScalarExpr.Unary { op.IsNegate: true } u: return Passes(u.expr);
                case ScalarExpr.Conditional c:
                    return DecidedBranch(c.test, c.ifTrue, c.ifFalse) is { } branch && Passes(branch);
                default: return false;
            }
        }
        var columns = ScalarExprOps.collectColumns(expr);
        return !columns.IsEmpty && Passes(expr) ? [.. columns] : null;
    }

    /// <summary>
    /// The count of an expression's non-null values from its columns, when its nulls are theirs: the expression is
    /// then computed once, by its sum, and the engine's grouping keeps its fast path (see <see cref="SplitSumOrNull"/>).
    /// </summary>
    private static ScalarExpr CountOf(Lazy<LogicalSchema> schema, ScalarExpr argument)
    {
        if (IsComputed(argument) && NullsFrom(schema, argument) is { Count: > 0 } columns)
        {
            if (columns.Count == 1) return ScalarExpr.NewAggregation(AggKind.Count, ScalarExpr.NewColumn(columns[0]), false);
            static ScalarExpr Present(string column) => ScalarExpr.NewUnary(UnaryOp.IsNotNull, ScalarExpr.NewColumn(column));
            var rows = columns.Skip(1).Aggregate(Present(columns[0]), (acc, column) => ScalarExpr.NewBinary(BinaryOp.And, acc, Present(column)));
            return ScalarExpr.NewAggregation(AggKind.Sum, rows, false);
        }
        return ScalarExpr.NewAggregation(AggKind.Count, argument, false);
    }

    /// <summary>
    /// A SumOrNull is lowered as a count and a sum of its argument (PolarsScalarLowerer), and the engine computes an
    /// argument twice when it is an expression. One that does arithmetic or calls a function is computed once instead,
    /// as a column of the aggregation's input (a grouping, grouping sets, or a projection of aggregates): TPC-DS Q4's
    /// decimal arithmetic per row was about half the grouping's time.
    /// </summary>
    private static (LogicalPlan Input, FSharpList<Tuple<string, ScalarExpr>> Aggs) HoistSumOrNullArguments(
        Func<ScalarExpr, bool> hoists, LogicalPlan input, FSharpList<Tuple<string, ScalarExpr>> aggs)
    {
        List<Tuple<string, ScalarExpr>> hoisted = [];
        void Collect(ScalarExpr expr)
        {
            switch (expr)
            {
                case ScalarExpr.Aggregation { kind.IsSumOrNull: true, isDistinct: false, expr.Value: var argument } when hoists(argument):
                    if (!hoisted.Any(h => h.Item2.Equals(argument))) hoisted.Add(Tuple.Create($"__sum_or_null_argument_{hoisted.Count}", argument));
                    break;
                case ScalarExpr.Aggregation:
                    break;
                default:
                    foreach (var child in ScalarExprOps.children(expr)) Collect(child);
                    break;
            }
        }
        foreach (var agg in aggs) Collect(agg.Item2);
        if (hoisted.Count == 0) return (input, aggs);

        // Every aggregate of a hoisted argument reads the column (AVG is a SumOrNull over a Count of the same value). A
        // count also reads it when its argument is the hoisted one before casts that keep every value: their nulls are
        // the same rows (SQL's AVG sums the value in a wider decimal than it counts).
        var schema = new Lazy<LogicalSchema>(() => IrOps.InferSchema(input));
        ScalarExpr WithoutWidening(ScalarExpr expr) =>
            expr is ScalarExpr.Cast c && FilterShapes.castKeepsValuesOf(IrOps.InferType(schema.Value, c.expr), c.targetType) ? WithoutWidening(c.expr) : expr;
        ScalarExpr Rewrite(ScalarExpr expr)
        {
            switch (expr)
            {
                case ScalarExpr.Aggregation { expr.Value: var argument } a:
                {
                    var same = hoisted.FirstOrDefault(h => h.Item2.Equals(argument));
                    if (same is null && a.kind.IsCount && !a.isDistinct)
                        same = hoisted.FirstOrDefault(h => WithoutWidening(h.Item2).Equals(WithoutWidening(argument)));
                    return same is null ? expr : ScalarExpr.NewAggregation(a.kind, ScalarExpr.NewColumn(same.Item1), a.isDistinct);
                }
                case ScalarExpr.Alias a: return ScalarExpr.NewAlias(Rewrite(a.expr), a.alias);
                case ScalarExpr.Cast c: return ScalarExpr.NewCast(Rewrite(c.expr), c.targetType, c.isStrict);
                case ScalarExpr.Unary u: return ScalarExpr.NewUnary(u.op, Rewrite(u.expr));
                case ScalarExpr.Binary b: return ScalarExpr.NewBinary(b.op, Rewrite(b.left), Rewrite(b.right));
                case ScalarExpr.Conditional c: return ScalarExpr.NewConditional(Rewrite(c.test), Rewrite(c.ifTrue), Rewrite(c.ifFalse));
                case ScalarExpr.Coalesce c: return ScalarExpr.NewCoalesce(ListModule.OfSeq(c.exprs.Select(Rewrite)));
                default: return expr;
            }
        }
        var rewritten = ListModule.OfSeq(aggs.Select(a => Tuple.Create(a.Item1, Rewrite(a.Item2))));
        return (LogicalPlan.NewWithColumns(input, ListModule.OfSeq(hoisted)), rewritten);
    }

    private static bool HoldsSumOrNull(ScalarExpr expr) => expr switch
    {
        ScalarExpr.Aggregation { kind.IsSumOrNull: true } => true,
        ScalarExpr.Aggregation => false,
        _ => ScalarExprOps.children(expr).Any(HoldsSumOrNull),
    };

    /// <summary>
    /// A grouping whose aggregates hold a SumOrNull, as a grouping of plain aggregates and a projection that combines
    /// them: the engine's grouping runs plain aggregates (<c>sum</c>, <c>count</c>) on a fast path, and an aggregate
    /// inside an expression (<c>when(count > 0).then(sum)</c>) on a slow, group by group one (TPC-DS Q4's
    /// pre-aggregation: 307 ms against 110). Null when no aggregate holds a SumOrNull, or with a HAVING, which reads the
    /// aggregates in place.
    /// </summary>
    private static LogicalPlan? SplitSumOrNull(
        LogicalPlan input, FSharpList<ScalarExpr> keys, FSharpList<Tuple<string, ScalarExpr>> aggs, ScalarExpr? having, bool maintainOrder)
    {
        if (having is not null || !aggs.Any(a => HoldsSumOrNull(a.Item2))) return null;
        var inputSchema = IrOps.InferSchema(input);
        var schema = new Lazy<LogicalSchema>(() => inputSchema);
        List<Tuple<string, ScalarExpr>> plain = [];
        ScalarExpr PlainColumn(ScalarExpr aggregate)
        {
            if (plain.FirstOrDefault(p => p.Item2.Equals(aggregate)) is { } found) return ScalarExpr.NewColumn(found.Item1);
            string name = $"__plain_aggregate_{plain.Count}";
            plain.Add(Tuple.Create(name, aggregate));
            return ScalarExpr.NewColumn(name);
        }
        // Each aggregate a plain one of the grouping; a SumOrNull its sum where its count is not zero.
        ScalarExpr Combine(ScalarExpr expr)
        {
            switch (expr)
            {
                case ScalarExpr.Aggregation { kind.IsSumOrNull: true, isDistinct: false, expr.Value: var argument }:
                {
                    var total = PlainColumn(ScalarExpr.NewAggregation(AggKind.Sum, argument, false));
                    var counted = PlainColumn(CountOf(schema, argument));
                    var none = ScalarExpr.NewLiteral(LiteralValue.NewNull(IrOps.InferType(inputSchema, expr)));
                    return ScalarExpr.NewConditional(ScalarExpr.NewBinary(BinaryOp.Gt, counted, ScalarExpr.NewLiteral(LiteralValue.NewUInt32(0u))), total, none);
                }
                case ScalarExpr.Aggregation { kind.IsCount: true, isDistinct: false, expr.Value: var argument }:
                    return PlainColumn(CountOf(schema, argument));
                case ScalarExpr.Aggregation:
                    return PlainColumn(expr);
                case ScalarExpr.Alias a: return ScalarExpr.NewAlias(Combine(a.expr), a.alias);
                case ScalarExpr.Cast c: return ScalarExpr.NewCast(Combine(c.expr), c.targetType, c.isStrict);
                case ScalarExpr.Unary u: return ScalarExpr.NewUnary(u.op, Combine(u.expr));
                case ScalarExpr.Binary b: return ScalarExpr.NewBinary(b.op, Combine(b.left), Combine(b.right));
                case ScalarExpr.Conditional c: return ScalarExpr.NewConditional(Combine(c.test), Combine(c.ifTrue), Combine(c.ifFalse));
                case ScalarExpr.Coalesce c: return ScalarExpr.NewCoalesce(ListModule.OfSeq(c.exprs.Select(Combine)));
                default: return expr;
            }
        }
        var combined = aggs.Select(a => (Name: a.Item1, Expr: Combine(a.Item2))).ToList();
        // A SumOrNull under an expression this cannot rebuild (a function, a window) stays as it was.
        if (combined.Any(c => HoldsSumOrNull(c.Expr))) return null;
        var grouping = LogicalPlan.NewGroupBy(input, keys, ListModule.OfSeq(plain), null, maintainOrder);
        var aggNames = aggs.Select(a => a.Item1).ToHashSet();
        var columns = IrOps.InferSchema(LogicalPlan.NewGroupBy(input, keys, aggs, null, maintainOrder)).ColumnNames
            .Select(name => aggNames.Contains(name)
                ? Tuple.Create(name, combined.First(c => c.Name == name).Expr)
                : Tuple.Create(name, ScalarExpr.NewColumn(name)));
        return LogicalPlan.NewProject(grouping, ListModule.OfSeq(columns));
    }

    /// <summary>The column a predicate that may fail the query is computed in before its filter.</summary>
    private const string RaisingPredicateColumn = "__polars_net_raising_predicate";

    /// <summary>
    /// A filter whose predicate may fail the query (SEMANTICS.md, Exceptional cases): the engine pushes a predicate
    /// down and merges it with the filters beneath, which would evaluate it on rows they remove. Its conjuncts that
    /// cannot fail filter first, as usual, when the others read only their own row (one reading other rows, a window
    /// or an aggregate, would see fewer of them); the others are computed as a column of the rows left, which the
    /// engine does not push below where it is computed, and the rows are filtered on that column.
    /// </summary>
    private static LazyFrameHandle LowerRaisingFilter(LazyFrameHandle input, ScalarExpr predicate)
    {
        var conjuncts = FilterShapes.conjuncts(predicate);
        var splits = !conjuncts.Any(c => ScalarExprOps.raises(c) && ScalarExprOps.dependsOnOtherRows(c));
        List<ScalarExpr> plain = splits ? [.. conjuncts.Where(c => !ScalarExprOps.raises(c))] : [];
        List<ScalarExpr> raising = splits ? [.. conjuncts.Where(ScalarExprOps.raises)] : [.. conjuncts];
        var lf = plain.Count == 0 ? input : PolarsWrapper.LazyFilter(input, PolarsScalarLowerer.Lower(Conjoined(plain)));
        var computed = PolarsWrapper.LazyWithColumns(lf, [PolarsWrapper.Alias(PolarsScalarLowerer.Lower(Conjoined(raising)), RaisingPredicateColumn)]);
        var filtered = PolarsWrapper.LazyFilter(computed, PolarsWrapper.Col(RaisingPredicateColumn));
        return PolarsWrapper.LazyFrameDrop(filtered, PolarsWrapper.SelectorCols([RaisingPredicateColumn]));

        static ScalarExpr Conjoined(List<ScalarExpr> parts) =>
            parts.Skip(1).Aggregate(parts[0], (acc, c) => ScalarExpr.NewBinary(BinaryOp.And, acc, c));
    }

    private static ExprHandle[] Aliased(IEnumerable<Tuple<string, ScalarExpr>> items) =>
        [.. items.Select(i => PolarsWrapper.Alias(PolarsScalarLowerer.Lower(i.Item2), i.Item1))];

    private static ExprHandle[] LowerAll(IEnumerable<ScalarExpr> exprs) => [.. exprs.Select(PolarsScalarLowerer.Lower)];

    private static LazyFrameHandle LowerNode(SourceBinding[] sources, Dictionary<int, LazyFrameHandle> shared, LogicalPlan plan)
    {
        LazyFrameHandle Lower(LogicalPlan p) => LowerNode(sources, shared, p);
        switch (plan)
        {
            // A common subplan is lowered once and cached; every occurrence reads a clone of the cached frame, so
            // Polars computes it once. Polars' own common-subplan elimination only finds some such subplans (none
            // nested in another, and none once the plan holds an explicit cache), so the IR states them.
            case LogicalPlan.Shared s:
            {
                if (shared.TryGetValue(s.id, out var cachedLf)) return PolarsWrapper.LazyClone(cachedLf);
                var lf = PolarsWrapper.LazyFrameCache(Lower(s.input));
                shared[s.id] = lf;
                return PolarsWrapper.LazyClone(lf);
            }

            case LogicalPlan.Scan scan:
                switch (scan.source)
                {
                    case LogicalPlanSource.Slot slot:
                        if (slot.index < 0 || slot.index >= sources.Length)
                            throw new InvalidOperationException($"[Polars.NET.Linq Bug] Scan slot {slot.index} has no source binding ({sources.Length} bound).");
                        return sources[slot.index] switch
                        {
                            SourceBinding.Table { source: PolarsTableSource pts } => PolarsWrapper.LazyClone(pts.Handle),
                            SourceBinding.Table { source: DeltaKernelSource delta } => delta.Open(),
                            SourceBinding.Table other => throw new InvalidOperationException($"Unsupported table source in PolarsPhysicalLowerer: {other.source.Name}"),
                            SourceBinding.InMemory inMemory => MaterializeInMemory(inMemory.rows, inMemory.elementType, scan.schema),
                            _ => throw new ArgumentOutOfRangeException(nameof(plan)),
                        };
                    case LogicalPlanSource.SubqueryRef r:
                        throw new InvalidOperationException($"Unresolved SubqueryRef: {r.subqueryId}");
                    default:
                        return CreateEmptyLazyFrame();
                }

            case LogicalPlan.Filter f when ScalarExprOps.raises(f.predicate):
                return LowerRaisingFilter(Lower(f.input), f.predicate);
            case LogicalPlan.Filter f:
                return PolarsWrapper.LazyFilter(Lower(f.input), PolarsScalarLowerer.Lower(f.predicate));

            case LogicalPlan.Project p:
            {
                var (input, projections) = p.projections.Any(x => ScalarExprOps.containsAggregation(x.Item2))
                    ? HoistSumOrNullArguments(IsComputed, p.input, p.projections)
                    : (p.input, p.projections);
                var lf = Lower(input);
                var exprs = Aliased(projections);
                // A Polars select of expressions that read no column (constants) yields a single row; a projection
                // yields one row per input row, so such constants are added as columns, which broadcast to the frame's
                // height, and then selected.
                static bool IsRowIndependent(Tuple<string, ScalarExpr> x) =>
                    ScalarExprOps.collectColumns(x.Item2).IsEmpty && !ScalarExprOps.containsAggregation(x.Item2);
                if (!projections.IsEmpty && projections.All(IsRowIndependent))
                {
                    var withConstants = PolarsWrapper.LazyWithColumns(lf, exprs);
                    return PolarsWrapper.LazySelect(withConstants, [.. projections.Select(x => PolarsWrapper.Col(x.Item1))]);
                }
                return PolarsWrapper.LazySelect(lf, exprs);
            }

            case LogicalPlan.WithColumns w:
                return PolarsWrapper.LazyWithColumns(Lower(w.input), Aliased(w.columns));

            case LogicalPlan.Join j:
                return PolarsWrapper.Join(
                    Lower(j.left),
                    Lower(j.right),
                    LowerAll(j.leftOn),
                    LowerAll(j.rightOn),
                    PolarsVocabulary.JoinType(j.how),
                    j.suffix.OrNull()!,
                    PlJoinValidation.ManyToMany,
                    PolarsVocabulary.JoinCoalesce(j.coalesce),
                    PolarsVocabulary.JoinMaintainOrder(j.how),
                    PlJoinSide.None,
                    j.nullsEqual,
                    null,
                    0UL);

            case LogicalPlan.JoinAsOf a:
            {
                var spec = a.spec;
                var left = Lower(a.left);
                var right = Lower(a.right);
                var tolerance = spec.Tolerance.OrNull();
                return PolarsWrapper.JoinAsOf(
                    left,
                    right,
                    [PolarsScalarLowerer.Lower(spec.LeftOn)],
                    [PolarsScalarLowerer.Lower(spec.RightOn)],
                    spec.LeftBy.IsEmpty ? null! : LowerAll(spec.LeftBy),
                    spec.RightBy.IsEmpty ? null! : LowerAll(spec.RightBy),
                    PolarsVocabulary.AsofStrategy(spec.Strategy),
                    tolerance switch
                    {
                        AsofTolerance.Duration d => DurationFormatter.ToPolarsString(d.Item),
                        AsofTolerance.BackendText t => t.Item,
                        _ => null!,
                    },
                    tolerance is AsofTolerance.Integer i ? i.Item : null,
                    tolerance is AsofTolerance.Float fl ? fl.Item : null,
                    spec.AllowEq,
                    spec.CheckSorted,
                    spec.Suffix.OrNull()!,
                    PlJoinValidation.ManyToMany,
                    PolarsVocabulary.JoinCoalesce(spec.Coalesce),
                    // An as-of join keeps every left row once, in left order.
                    PlJoinMaintainOrder.Left,
                    PlJoinSide.None,
                    // Null on / by keys never match (the IR contract; Polars ignores the flag for as-of joins).
                    false,
                    null,
                    0UL);
            }

            case LogicalPlan.GroupBy g:
            {
                // An argument whose count its columns give is computed once by its sum, inside the grouping: computed
                // beforehand as a column, it takes the grouping off its fast path (TPC-DS Q4: 110 ms against 300).
                var schema = new Lazy<LogicalSchema>(() => IrOps.InferSchema(g.input));
                var (input, aggs) = HoistSumOrNullArguments(a => IsComputed(a) && NullsFrom(schema, a) is null, g.input, g.aggs);
                var having = g.having.OrNull();
                if (SplitSumOrNull(input, g.keys, aggs, having, g.maintainOrder) is { } split) return Lower(split);
                var lf = Lower(input);
                var havingH = having is null ? null : PolarsScalarLowerer.Lower(having);
                return PolarsWrapper.LazyGroupByAgg(lf, LowerAll(g.keys), Aliased(aggs), havingH!, g.maintainOrder);
            }

            case LogicalPlan.GroupingSets gs:
                return LowerGroupingSets(sources, shared, plan, gs);

            case LogicalPlan.Sort s:
            {
                var lf = Lower(s.input);
                // A key that reads no column is the same for every row: it orders nothing (and the engine rejects a key
                // shorter than the frame), so it is dropped; a sort left without keys keeps the rows as they are.
                var orderings = s.orderings.Where(o => !ScalarExprOps.collectColumns(o.Expr).IsEmpty || ScalarExprOps.containsAggregation(o.Expr)).ToList();
                if (orderings.Count == 0) return lf;
                // LINQ ordering is stable: rows with equal keys keep their input order, unless the configuration lets
                // ties come out in any order.
                return PolarsWrapper.LazyFrameSort(
                    lf,
                    LowerAll(orderings.Select(o => o.Expr)),
                    [.. orderings.Select(o => o.Descending)],
                    [.. orderings.Select(o => o.NullsLast)],
                    maintainOrder: !SamidareConfig.AllowUnstableOrdering);
            }

            case LogicalPlan.Limit l when SlicesOrderedGroups(l.input):
            {
                // Polars (0.55) pushes a slice into a group_by and then, when nothing downstream observes the order of
                // the result (an aggregate, a semi join), drops the group_by's maintain_order although the slice still
                // selects groups by it. The slice is applied as a filter on a row index instead: nothing pushes into
                // the group_by, and the index observes its order. Same window as a slice: [start, start + count) within
                // the rows, start counting from the end when the offset is negative.
                var lf = Lower(l.input);
                const string indexName = "__slice_order_idx";
                var indexed = PolarsWrapper.LazyFrameWithRowIndex(lf, indexName, 0);
                ExprHandle Start() => l.offset >= 0L ? PolarsWrapper.Lit(l.offset) : PolarsWrapper.Add(PolarsWrapper.Len(), PolarsWrapper.Lit(l.offset));
                var window = PolarsWrapper.And(
                    PolarsWrapper.GtEq(PolarsWrapper.Col(indexName), Start()),
                    PolarsWrapper.Lt(PolarsWrapper.Col(indexName), PolarsWrapper.Add(Start(), PolarsWrapper.Lit((long)l.count))));
                var sliced = PolarsWrapper.LazyFilter(indexed, window);
                return PolarsWrapper.LazyFrameDrop(sliced, PolarsWrapper.SelectorCols([indexName]));
            }

            case LogicalPlan.Limit l:
                return PolarsWrapper.LazySlice(Lower(l.input), l.offset, l.count);

            case LogicalPlan.Distinct d:
            {
                var lf = Lower(d.input);
                var selector = d.subset.OrNull() is { IsEmpty: false } columns ? PolarsWrapper.SelectorCols([.. columns]) : null;
                return PolarsWrapper.LazyUnique(lf, selector!, PolarsVocabulary.UniqueKeep(d.keep), maintainOrder: d.maintainOrder);
            }

            case LogicalPlan.Explode e:
                return PolarsWrapper.LazyExplode(Lower(e.input), PolarsWrapper.SelectorCols([.. e.columns]), e.emptyAsNull, e.keepNulls);

            case LogicalPlan.Unpivot u:
            {
                var lf = Lower(u.input);
                var index = PolarsWrapper.SelectorCols([.. u.index]);
                var on = u.on.OrNull() is { IsEmpty: false } columns ? PolarsWrapper.SelectorCols([.. columns]) : null;
                return PolarsWrapper.LazyUnpivot(lf, index, on!, u.variableName, u.valueName);
            }

            case LogicalPlan.Union u:
            {
                var left = Lower(u.left);
                var right = Lower(u.right);
                var (leftFields, rightFields) = NodeSchemas(sources, plan, () => (NativeOpsApplier.GetLazySchemaFields(left), NativeOpsApplier.GetLazySchemaFields(right)));
                var alignedRight = NativeOpsApplier.AlignToFields(leftFields, rightFields, right);
                var concatenated = PolarsWrapper.LazyConcat([left, alignedRight], PlConcatType.Vertical);
                // Without isAll, the first of equal rows (null equal to null), in order.
                return u.isAll
                    ? concatenated
                    : PolarsWrapper.LazyUnique(concatenated, null!, PolarsVocabulary.UniqueKeep(DistinctKeep.First), maintainOrder: true);
            }

            case LogicalPlan.WithRowIndex r:
                return PolarsWrapper.LazyFrameWithRowIndex(Lower(r.input), r.name, r.offset.ToNullable() is { } offset ? (int)offset : 0);

            case LogicalPlan.Rename r:
                return PolarsWrapper.LazyRename(Lower(r.input), [.. r.existingNames], [.. r.newNames], strict: false);

            case LogicalPlan.Reverse r:
                return PolarsWrapper.LazyFrameReverse(Lower(r.input));

            case LogicalPlan.SkipLast s:
            {
                var withIndex = PolarsWrapper.LazyFrameWithRowIndex(Lower(s.input), "__row_idx", 0);
                var filter = PolarsWrapper.Lt(PolarsWrapper.Add(PolarsWrapper.Col("__row_idx"), PolarsWrapper.Lit((uint)s.count)), PolarsWrapper.Len());
                var filtered = PolarsWrapper.LazyFilter(withIndex, filter);
                return PolarsWrapper.LazyFrameDrop(filtered, PolarsWrapper.SelectorCols(["__row_idx"]));
            }

            case LogicalPlan.HorizontalConcat h:
                return PolarsWrapper.LazyConcat([.. h.inputs.Select(Lower)], PlConcatType.Horizontal, false, true);

            case LogicalPlan.Empty:
                return CreateEmptyLazyFrame();

            default:
                throw new ArgumentOutOfRangeException(nameof(plan));
        }
    }

    private static LazyFrameHandle LowerGroupingSets(
        SourceBinding[] sources, Dictionary<int, LazyFrameHandle> shared, LogicalPlan plan, LogicalPlan.GroupingSets node)
    {
        var spec = node.spec;
        var (input, aggs) = HoistSumOrNullArguments(IsComputed, node.input, spec.Aggs);
        // Every level groups the same input: it is computed once. (An explicit cache also turns off Polars' own
        // common-subplan elimination for the whole plan, so subplans the plan reuses must be explicit too; see
        // LogicalPlan.Shared.)
        var activeLf = PolarsWrapper.LazyFrameCache(LowerNode(sources, shared, input));
        var keyAliasedHandles = Aliased(spec.KeyAliases);
        var aggHandles = Aliased(aggs);
        var outputAliasToKeyIdx = spec.KeyAliases.Select((k, i) => (k.Item1, i)).ToDictionary(p => p.Item1, p => p.i);

        // The column types every level is cast to, read from the schema of a grouping by every key (never collected):
        // a key outside a level's subset becomes a null of the key's type. A literal column takes the type of its first
        // non-null literal across the levels.
        var typeReferenceLiterals = spec.Levels
            .SelectMany(level => level.Item2.Select(kv => (Name: kv.Key, Expr: kv.Value)))
            .OrderBy(x => x.Expr is ScalarExpr.Literal { value: LiteralValue.Null } ? 1 : 0)
            .DistinctBy(x => x.Name)
            .ToDictionary(x => x.Name, x => x.Expr);
        var colTypeMap = NodeSchemas(sources, plan, () =>
        {
            ExprHandle[] referenceKeys = [.. keyAliasedHandles.Select(PolarsWrapper.CloneExpr)];
            ExprHandle[] referenceAggs = [.. aggHandles.Select(PolarsWrapper.CloneExpr)];
            var referenceGrouped = PolarsWrapper.LazyGroupByAgg(PolarsWrapper.LazyClone(activeLf), referenceKeys, referenceAggs, null!, true);
            ExprHandle[] referenceSelect =
            [
                .. spec.OutputColumns.Select(name =>
                    typeReferenceLiterals.TryGetValue(name, out var literal)
                        ? PolarsWrapper.Alias(PolarsScalarLowerer.Lower(literal), name)
                        : PolarsWrapper.Col(name)),
            ];
            return NativeOpsApplier.GetLazySchemaFields(PolarsWrapper.LazySelect(referenceGrouped, referenceSelect)).ToDictionary(f => f.Name, f => f.Type);
        });

        // With merges a level is computed from the smallest finer level, which is cached when it feeds another.
        HashSet<int>[] subsets = [.. spec.Levels.Select(level => level.Item1.ToHashSet())];
        var merges = spec.LevelMerges.OrNull();
        int?[] parents =
        [
            .. subsets.Select(subset => merges is null
                ? (int?)null
                : subsets
                    .Select((other, index) => (other, index))
                    .Where(p => p.other.IsProperSupersetOf(subset))
                    .OrderBy(p => p.other.Count).ThenBy(p => p.index)
                    .Select(p => (int?)p.index)
                    .FirstOrDefault()),
        ];
        var cachedLevels = new Dictionary<int, LazyFrameHandle>();

        LazyFrameHandle GroupLevel(int level, FSharpList<int> subsetIndices)
        {
            if (cachedLevels.TryGetValue(level, out var cached)) return PolarsWrapper.LazyClone(cached);
            LazyFrameHandle levelInput;
            ExprHandle[] keyHandles, aggregates;
            if (parents[level] is { } parent && merges is not null)
            {
                levelInput = GroupLevel(parent, spec.Levels[parent].Item1);
                keyHandles = [.. subsetIndices.Select(i => PolarsWrapper.Col(spec.KeyAliases[i].Item1))];
                aggregates = Aliased(merges);
            }
            else
            {
                levelInput = PolarsWrapper.LazyClone(activeLf);
                keyHandles = [.. subsetIndices.Select(i => PolarsWrapper.CloneExpr(keyAliasedHandles[i]))];
                aggregates = [.. aggHandles.Select(PolarsWrapper.CloneExpr)];
            }
            var grouped = subsetIndices.IsEmpty
                ? PolarsWrapper.LazyGroupByAgg(levelInput, [PolarsWrapper.Alias(PolarsWrapper.Lit(1), "__dummy_rollup_key__")], aggregates, null!, false)
                : PolarsWrapper.LazyGroupByAgg(levelInput, keyHandles, aggregates, null!, true);
            if (parents.Contains(level))
            {
                var cachedLevel = PolarsWrapper.LazyFrameCache(grouped);
                cachedLevels[level] = cachedLevel;
                return PolarsWrapper.LazyClone(cachedLevel);
            }
            return grouped;
        }

        ExprHandle TypedNull(string name)
        {
            var dtExpr = PolarsWrapper.DataTypeExprFromDataType(colTypeMap[name]);
            return PolarsWrapper.Alias(PolarsWrapper.ExprCast(PolarsWrapper.LitNull(), dtExpr, strict: false, wrapNumerical: false), name);
        }

        LazyFrameHandle CompileLevel(int level, FSharpList<int> subsetIndices, FSharpMap<string, ScalarExpr> levelLiterals)
        {
            var grouped = GroupLevel(level, subsetIndices);
            var subsetSet = subsetIndices.ToHashSet();
            ExprHandle[] projected =
            [
                .. spec.OutputColumns.Select(name =>
                {
                    if (levelLiterals.TryGetValue(name, out var literal))
                        return literal is ScalarExpr.Literal { value: LiteralValue.Null } && colTypeMap.ContainsKey(name)
                            ? TypedNull(name)
                            : PolarsWrapper.Alias(PolarsScalarLowerer.Lower(literal), name);
                    if (outputAliasToKeyIdx.TryGetValue(name, out int keyIdx))
                        return subsetSet.Contains(keyIdx) ? PolarsWrapper.Col(name) : TypedNull(name);
                    return PolarsWrapper.Col(name);
                }),
            ];
            return PolarsWrapper.LazySelect(grouped, projected);
        }

        try
        {
            LazyFrameHandle[] levels = [.. spec.Levels.Select((level, i) => CompileLevel(i, level.Item1, level.Item2))];
            return PolarsWrapper.LazyConcat(levels, PlConcatType.Vertical, false, true);
        }
        finally
        {
            foreach (var lf in cachedLevels.Values) lf.Dispose();
        }
    }

    /// <summary>Recursively lowers a LogicalPlan into a LazyFrameHandle, reading each Scan slot from <paramref name="sources"/>.</summary>
    public static LazyFrameHandle LowerPlan(SourceBinding[] sources, LogicalPlan plan)
    {
        var shared = new Dictionary<int, LazyFrameHandle>();
        // Filter-aware sources (a table format kernel) learn which rows the plan keeps and skip what they can.
        var hinted = ScanFilterHints.apply(sources, plan);
        try
        {
            return LowerNode(hinted, shared, plan);
        }
        finally
        {
            foreach (var lf in shared.Values) lf.Dispose();
        }
    }
}
