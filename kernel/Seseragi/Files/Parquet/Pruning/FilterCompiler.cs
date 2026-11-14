using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;
using Microsoft.FSharp.Collections;

namespace Kaminari.Seseragi.Files.Parquet.Pruning;

/// <summary>
/// Compiles a row filter of the query compiler's IR (<see cref="ScalarExpr"/>, semantics in Samidare's
/// IR/SEMANTICS.md) into a <see cref="PruneNode"/> tree that bounds the filter's outcomes on a unit from its
/// statistics. Columns are top-level names of the file, struct fields reached through <c>StructFieldByName</c>;
/// only leaves with no repeated ancestor are tested. Whatever is not modelled becomes <see cref="UnknownNode"/>,
/// which only makes pruning weaker.
/// </summary>
internal sealed class FilterCompiler(ParquetSchemaTree tree, FileMetaData meta, ParquetReadOptions options)
{
    private static readonly HashSet<int> s_noGuards = [];
    private readonly Dictionary<int, LeafDomain?> _domains = [];
    private readonly HashSet<int> _equalities = [];

    /// <summary>The leaves the filter tests for equality with a value (a Bloom filter can answer those).</summary>
    public IReadOnlyCollection<int> EqualityLeaves => _equalities;

    /// <summary>The leaves the filter reads that statistics can test (after <see cref="Compile(ScalarExpr)"/>).</summary>
    public IEnumerable<LeafDomain> Leaves => _domains.Values.OfType<LeafDomain>();

    /// <summary>The compiled filter; null when it can rule nothing out.</summary>
    public PruneNode? Compile(ScalarExpr filter)
    {
        PruneNode root = Compile(filter, s_noGuards);
        return root is UnknownNode ? null : root;
    }

    /// <param name="expr">The piece of the filter.</param>
    /// <param name="nonNaN">Leaves the enclosing conjunction keeps away from NaN.</param>
    private PruneNode Compile(ScalarExpr expr, HashSet<int> nonNaN)
    {
        switch (expr)
        {
            case ScalarExpr.Alias a:
                return Compile(a.expr, nonNaN);

            case ScalarExpr.Binary b when b.op.IsAnd:
                return And([.. FilterShapes.conjuncts(expr)], nonNaN);

            case ScalarExpr.Binary b when b.op.IsOr:
                return new OrNode([.. FilterShapes.disjuncts(expr).Select(e => Compile(e, nonNaN))]);

            case ScalarExpr.Binary b when b.op.IsStartsWith:
            {
                if (Resolve(b.left) is not { Kind: DomainKind.Bytes } domain || b.right is not ScalarExpr.Literal prefix) return UnknownNode.Instance;
                if (prefix.value is LiteralValue.Null) return new ConstantNode(Outcomes.Null);
                return domain.TryConvert(prefix.value, out Bound bytes) ? new LeafNode(domain, LeafTest.StartsWith, bytes, false) : UnknownNode.Instance;
            }

            case ScalarExpr.Binary b:
                return Comparison(b.op, b.left, b.right, nonNaN);

            case ScalarExpr.Unary u when u.op.IsNot:
                return Compile(u.expr, nonNaN) is var inner && inner is UnknownNode ? inner : new NotNode(inner);

            case ScalarExpr.Unary u when u.op.IsIsNull || u.op.IsIsNotNull || u.op.IsIsNan:
            {
                if (Resolve(u.expr) is not LeafDomain domain) return UnknownNode.Instance;
                if (u.op.IsIsNan && domain.Kind != DomainKind.Floating) return UnknownNode.Instance;
                LeafTest test = u.op.IsIsNull ? LeafTest.IsNull : u.op.IsIsNotNull ? LeafTest.IsNotNull : LeafTest.IsNan;
                return new LeafNode(domain, test, default, nonNaN.Contains(domain.Leaf));
            }

            case ScalarExpr.Column when Resolve(expr) is { Shape: LiteralShape.Boolean } boolean:
                return new LeafNode(boolean, LeafTest.Eq, new Bound(Integer: 1), excludeNaN: false);

            case ScalarExpr.Literal { value: LiteralValue.Bool b }:
                return new ConstantNode(b.Item ? Outcomes.True : Outcomes.False);

            case ScalarExpr.Literal { value: LiteralValue.Null }:
                return new ConstantNode(Outcomes.Null);

            case ScalarExpr.InList list when list.source is InListSource.Literals literals:
                return InList(list.expr, literals.Item, list.negated, list.nullsEqual, nonNaN);

            case ScalarExpr.Coalesce c when c.exprs.Length == 2 && c.exprs.Tail.Head is ScalarExpr.Literal fallback:
            {
                Outcomes? outcome = fallback.value switch
                {
                    LiteralValue.Bool b => b.Item ? Outcomes.True : Outcomes.False,
                    LiteralValue.Null => Outcomes.Null,
                    _ => null,
                };
                return outcome is Outcomes o ? new CoalesceNode(Compile(c.exprs.Head, nonNaN), o) : UnknownNode.Instance;
            }

            default:
                return UnknownNode.Instance;
        }
    }

    private AndNode And(List<ScalarExpr> conjuncts, HashSet<int> outer)
    {
        // A NaN guard (x <= +inf, NOT isnan(x)) makes every row the conjunction keeps a number or null at x, so
        // the other conjuncts need not consider NaN there (the C# front end guards >, >= and != this way).
        HashSet<int> nonNaN = outer;
        foreach (ScalarExpr conjunct in conjuncts)
        {
            if (NaNGuardLeaf(conjunct) is int leaf)
            {
                if (ReferenceEquals(nonNaN, outer)) nonNaN = [.. outer];
                nonNaN.Add(leaf);
            }
        }
        return new AndNode([.. conjuncts.Select(c => Compile(c, nonNaN))]);
    }

    private PruneNode Comparison(BinaryOp op, ScalarExpr left, ScalarExpr right, HashSet<int> nonNaN)
    {
        LeafTest? test = op switch
        {
            _ when op.IsEq => LeafTest.Eq,
            _ when op.IsNotEq => LeafTest.NotEq,
            _ when op.IsLt => LeafTest.Lt,
            _ when op.IsLtEq => LeafTest.LtEq,
            _ when op.IsGt => LeafTest.Gt,
            _ when op.IsGtEq => LeafTest.GtEq,
            _ when op.IsEqMissing || op.IsNotEqMissing => LeafTest.EqMissing,
            _ => null,
        };
        if (test is not LeafTest t || !TrySplit(left, right, out LeafDomain? domain, out LiteralValue? literal, out bool flipped))
            return UnknownNode.Instance;
        bool excludeNaN = nonNaN.Contains(domain!.Leaf);

        if (t == LeafTest.EqMissing)
        {
            PruneNode eq = literal is LiteralValue.Null
                ? new LeafNode(domain, LeafTest.EqMissingNull, default, excludeNaN)
                : domain.TryConvert(literal!, out Bound b) ? EqualityNode(domain, LeafTest.EqMissing, b, excludeNaN) : UnknownNode.Instance;
            return op.IsNotEqMissing && eq is not UnknownNode ? new NotNode(eq) : eq;
        }
        // A comparison with null is null on every row.
        if (literal is LiteralValue.Null) return new ConstantNode(Outcomes.Null);
        if (!domain.TryConvert(literal!, out Bound bound)) return UnknownNode.Instance;
        if (flipped)
        {
            t = t switch
            {
                LeafTest.Lt => LeafTest.Gt,
                LeafTest.LtEq => LeafTest.GtEq,
                LeafTest.Gt => LeafTest.Lt,
                LeafTest.GtEq => LeafTest.LtEq,
                _ => t,
            };
        }
        if (t == LeafTest.Eq) _equalities.Add(domain.Leaf);
        return new LeafNode(domain, t, bound, excludeNaN);
    }

    private LeafNode EqualityNode(LeafDomain domain, LeafTest test, Bound bound, bool excludeNaN)
    {
        _equalities.Add(domain.Leaf);
        return new LeafNode(domain, test, bound, excludeNaN);
    }

    /// <summary>
    /// <c>x IN (values)</c>. Without <paramref name="nullsEqual"/> (SQL): null on a null x, and on a miss when the
    /// list holds null. With it (.NET Contains): a null x is in a list holding null; never null.
    /// </summary>
    private PruneNode InList(ScalarExpr target, FSharpList<LiteralValue> values, bool negated, bool nullsEqual, HashSet<int> nonNaN)
    {
        if (Resolve(target) is not LeafDomain domain) return UnknownNode.Instance;
        bool excludeNaN = nonNaN.Contains(domain.Leaf), hasNull = false;
        List<PruneNode> equals = [];
        foreach (LiteralValue value in values)
        {
            if (value is LiteralValue.Null) { hasNull = true; continue; }
            if (!domain.TryConvert(value, out Bound bound)) return UnknownNode.Instance;
            equals.Add(EqualityNode(domain, LeafTest.Eq, bound, excludeNaN));
        }
        PruneNode any = new OrNode([.. equals]);
        PruneNode inList = (nullsEqual, hasNull) switch
        {
            (false, false) => any,
            (false, true) => new OrNode([any, new ConstantNode(Outcomes.Null)]),
            (true, false) => new CoalesceNode(any, Outcomes.False),
            (true, true) => new OrNode([new CoalesceNode(any, Outcomes.False), new LeafNode(domain, LeafTest.EqMissingNull, default, excludeNaN)]),
        };
        return negated ? new NotNode(inList) : inList;
    }

    /// <summary>The floating leaf a conjunct keeps away from NaN (one of the front end's NaN guards).</summary>
    private int? NaNGuardLeaf(ScalarExpr conjunct) =>
        FilterShapes.tryNaNGuardOperand(conjunct) is { } operand && Resolve(operand.Value) is { Kind: DomainKind.Floating } leaf ? leaf.Leaf : null;

    /// <summary>Splits a comparison into its leaf and literal; <paramref name="flipped"/> when the literal is on the left.</summary>
    private bool TrySplit(ScalarExpr left, ScalarExpr right, out LeafDomain? domain, out LiteralValue? literal, out bool flipped)
    {
        (domain, literal, flipped) = (null, null, false);
        if (right is ScalarExpr.Literal rl && Resolve(left) is LeafDomain l) (domain, literal) = (l, rl.value);
        else if (left is ScalarExpr.Literal ll && Resolve(right) is LeafDomain r) (domain, literal, flipped) = (r, ll.value, true);
        return domain is not null;
    }

    /// <summary>
    /// The leaf an expression reads, seeing through casts that keep the order of every value (to the same type, to
    /// <see cref="FilterShapes.castKeepsValues"/>); null when it is no testable leaf.
    /// </summary>
    private LeafDomain? Resolve(ScalarExpr expr)
    {
        switch (expr)
        {
            case ScalarExpr.Cast cast:
            {
                if (Node(cast.expr) is not { IsLeaf: true } leaf || Domain(leaf) is not LeafDomain domain) return null;
                return cast.targetType is Samidare.IR.LogicalType.Arrow target && FilterShapes.castKeepsValues(LeafTypeOf(leaf).ArrowType, target.arrowType) ? domain : null;
            }
            default:
                return Node(expr) is { IsLeaf: true } node ? Domain(node) : null;
        }
    }

    /// <summary>The schema node of a column or a struct field path.</summary>
    private SchemaNode? Node(ScalarExpr expr) => expr switch
    {
        ScalarExpr.Column c => Child(tree.Root, c.name),
        ScalarExpr.Function f when f.func.IsStructFieldByName && f.args.Length == 2
                                   && f.args.Tail.Head is ScalarExpr.Literal { value: LiteralValue.String name }
                                   && Node(f.args.Head) is { IsLeaf: false } parent => Child(parent, name.Item),
        _ => null,
    };

    private static SchemaNode? Child(SchemaNode parent, string name)
    {
        foreach (SchemaNode child in parent.Children)
        {
            if (child.Name == name) return child;
        }
        return null;
    }

    private LeafType LeafTypeOf(SchemaNode leaf) => LeafTypes.Resolve(leaf.Element, options);

    private LeafDomain? Domain(SchemaNode leaf)
    {
        if (!_domains.TryGetValue(leaf.LeafIndex, out LeafDomain? domain))
        {
            try
            {
                domain = LeafDomain.Create(leaf, LeafTypeOf(leaf), meta.ColumnOrders, tree.Leaves.Length);
            }
            catch (ParquetFormatException)
            {
                domain = null; // the read reports a type it cannot map; pruning just keeps
            }
            _domains[leaf.LeafIndex] = domain;
        }
        return domain;
    }
}

/// <summary>The statistics of a row group's column chunks, by the rules of each leaf, and its Bloom filters when loaded.</summary>
internal sealed class RowGroupStatistics(RowGroup group, Func<int, Kaminari.Sazanami.Filters.SplitBlockBloomFilter?>? blooms = null) : IUnitStatistics
{
    public bool MightContain(int leaf, LeafDomain domain, in Bound literal)
    {
        if (blooms?.Invoke(leaf) is not { } bloom) return true;
        byte[][] keys = domain.BloomKeys(literal);
        if (keys.Length == 0) return true;
        foreach (byte[] key in keys)
        {
            if (bloom.MightContain(BloomFilters.Hash(key))) return true;
        }
        return false;
    }

    public bool TryGet(int leaf, LeafDomain domain, out LeafStatistics statistics)
    {
        statistics = default;
        if ((uint)leaf >= (uint)group.Columns.Length || group.Columns[leaf].MetaData is not ColumnMetaData meta) return false;
        // A flat leaf holds a value or a null per row; any other count is damage, about which nothing is assumed.
        if (meta.NumValues != group.NumRows) return false;
        Statistics? s = meta.Statistics;
        byte[]? min = null, max = null;
        if (s is not null && domain.TypedStatistics) (min, max) = (s.MinValue, s.MaxValue);
        else if (s is not null && domain.LegacyStatistics) (min, max) = (s.Min, s.Max);
        long? nulls = s?.NullCount is long n && n >= 0 && n <= meta.NumValues ? n : null;
        long? nans = s?.NanCount is long m && m >= 0 && m <= meta.NumValues ? m : null;
        statistics = new LeafStatistics(min, max, nulls, meta.NumValues, nans);
        return true;
    }
}
