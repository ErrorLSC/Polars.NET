namespace Kaminari.Seseragi.Files.Parquet.Pruning;

/// <summary>
/// The results a filter may give on the rows of a unit (a row group, later a page): a set of true, false and null
/// (Kleene logic, as the query compiler's IR defines it). A unit whose set lacks <see cref="True"/> holds no row the
/// filter keeps. Every set is an over-approximation: an outcome is left out only when it is impossible.
/// </summary>
[Flags]
internal enum Outcomes : byte
{
    None = 0,
    True = 1,
    False = 2,
    Null = 4,
    Any = True | False | Null,
}

/// <summary>The statistics of one leaf in one unit, as far as the leaf's rules allow them to be used.</summary>
/// <param name="Min">The lower bound (stored encoding), or null when none may be used.</param>
/// <param name="Max">The upper bound (stored encoding), or null when none may be used.</param>
/// <param name="NullCount">The number of nulls, or null when not written (never assumed 0).</param>
/// <param name="ValueCount">The number of values, nulls included.</param>
/// <param name="NanCount">The number of NaN values of a floating leaf, or null when not written (NaN then possible).</param>
internal readonly record struct LeafStatistics(byte[]? Min, byte[]? Max, long? NullCount, long ValueCount, long? NanCount)
{
    /// <summary>Whether a row of the unit may be null at this leaf.</summary>
    public bool NullsPossible => NullCount is not long n || n > 0;

    /// <summary>Whether a row of the unit may hold a value (not null) at this leaf.</summary>
    public bool ValuesPossible => NullCount is not long n || n < ValueCount;
}

/// <summary>The statistics of a unit, by leaf.</summary>
internal interface IUnitStatistics
{
    /// <summary>The statistics of <paramref name="leaf"/>; false when the unit has none for it.</summary>
    bool TryGet(int leaf, LeafDomain domain, out LeafStatistics statistics);
}

/// <summary>A node of a compiled filter: the outcomes it may give on the rows of a unit.</summary>
internal abstract class PruneNode
{
    public abstract Outcomes Evaluate(IUnitStatistics unit);
}

/// <summary>Anything the compiler does not model: every outcome is possible.</summary>
internal sealed class UnknownNode : PruneNode
{
    public static readonly UnknownNode Instance = new();

    public override Outcomes Evaluate(IUnitStatistics unit) => Outcomes.Any;
}

/// <summary>A constant (a literal true, false or null).</summary>
internal sealed class ConstantNode(Outcomes outcomes) : PruneNode
{
    public override Outcomes Evaluate(IUnitStatistics unit) => outcomes;
}

/// <summary>Kleene AND of its children.</summary>
internal sealed class AndNode(PruneNode[] children) : PruneNode
{
    public override Outcomes Evaluate(IUnitStatistics unit)
    {
        Outcomes result = Outcomes.True; // the empty conjunction
        foreach (PruneNode child in children)
        {
            result = And(result, child.Evaluate(unit));
            if (result == Outcomes.False) break; // false AND anything is false
        }
        return result;
    }

    public static Outcomes And(Outcomes a, Outcomes b)
    {
        Outcomes r = Outcomes.None;
        if ((a & b & Outcomes.True) != 0) r |= Outcomes.True;
        if (((a | b) & Outcomes.False) != 0) r |= Outcomes.False;
        if ((a & Outcomes.Null) != 0 && (b & (Outcomes.True | Outcomes.Null)) != 0
            || (b & Outcomes.Null) != 0 && (a & (Outcomes.True | Outcomes.Null)) != 0) r |= Outcomes.Null;
        return r;
    }
}

/// <summary>Kleene OR of its children.</summary>
internal sealed class OrNode(PruneNode[] children) : PruneNode
{
    public override Outcomes Evaluate(IUnitStatistics unit)
    {
        Outcomes result = Outcomes.False; // the empty disjunction
        foreach (PruneNode child in children)
        {
            result = Or(result, child.Evaluate(unit));
            if (result == Outcomes.True) break;
        }
        return result;
    }

    public static Outcomes Or(Outcomes a, Outcomes b)
    {
        Outcomes r = Outcomes.None;
        if (((a | b) & Outcomes.True) != 0) r |= Outcomes.True;
        if ((a & b & Outcomes.False) != 0) r |= Outcomes.False;
        if ((a & Outcomes.Null) != 0 && (b & (Outcomes.False | Outcomes.Null)) != 0
            || (b & Outcomes.Null) != 0 && (a & (Outcomes.False | Outcomes.Null)) != 0) r |= Outcomes.Null;
        return r;
    }
}

/// <summary>Kleene NOT: true and false swap, null stays.</summary>
internal sealed class NotNode(PruneNode child) : PruneNode
{
    public override Outcomes Evaluate(IUnitStatistics unit) => Not(child.Evaluate(unit));

    public static Outcomes Not(Outcomes a) =>
        (a & Outcomes.Null) | ((a & Outcomes.True) != 0 ? Outcomes.False : 0) | ((a & Outcomes.False) != 0 ? Outcomes.True : 0);
}

/// <summary><c>Coalesce [child; literal]</c>: the child's result where it is not null, else the literal's.</summary>
internal sealed class CoalesceNode(PruneNode child, Outcomes fallback) : PruneNode
{
    public override Outcomes Evaluate(IUnitStatistics unit)
    {
        Outcomes a = child.Evaluate(unit);
        return (a & ~Outcomes.Null) | ((a & Outcomes.Null) != 0 ? fallback : 0);
    }
}

/// <summary>How a leaf node tests the values of its column.</summary>
internal enum LeafTest
{
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    IsNull,
    IsNotNull,
    IsNan,
    /// <summary>Null-safe equality with a literal: true on equal values, false on others and on nulls.</summary>
    EqMissing,
    /// <summary>Null-safe equality with null: true on nulls, false on values.</summary>
    EqMissingNull,
    /// <summary>A string starts with the literal (code point order: byte-wise on UTF-8).</summary>
    StartsWith,
}

/// <summary>A test of one leaf against a literal (or of its nulls / NaNs) on the statistics of a unit.</summary>
/// <param name="domain">The leaf and its rules.</param>
/// <param name="test">The test.</param>
/// <param name="literal">The literal compared with, in the stored representation (unused by null and NaN tests).</param>
/// <param name="excludeNaN">The enclosing conjunction keeps NaN rows out (a NaN guard): NaN need not be considered.</param>
internal sealed class LeafNode(LeafDomain domain, LeafTest test, Bound literal, bool excludeNaN) : PruneNode
{
    public LeafDomain Domain => domain;

    public override Outcomes Evaluate(IUnitStatistics unit)
    {
        if (!unit.TryGet(domain.Leaf, domain, out LeafStatistics s)) return Outcomes.Any;
        bool nulls = s.NullsPossible, values = s.ValuesPossible;
        switch (test)
        {
            case LeafTest.IsNull:
                return (nulls ? Outcomes.True : 0) | (values ? Outcomes.False : 0);
            case LeafTest.IsNotNull:
                return (values ? Outcomes.True : 0) | (nulls ? Outcomes.False : 0);
            case LeafTest.EqMissingNull:
                return (nulls ? Outcomes.True : 0) | (values ? Outcomes.False : 0);
            case LeafTest.StartsWith:
            {
                var (some, other) = values ? domain.PrefixPossible(s, literal.Bytes!) : (false, false);
                return (some ? Outcomes.True : 0) | (other ? Outcomes.False : 0) | (nulls ? Outcomes.Null : 0);
            }
            case LeafTest.IsNan:
            {
                if (!values) return nulls ? Outcomes.Null : Outcomes.None;
                bool nan = domain.NaNPossible(s) && !excludeNaN;
                // A number is possible unless every value is known to be NaN.
                bool number = s.NanCount is not long n || s.NullCount is not long z || n + z < s.ValueCount;
                return (nan ? Outcomes.True : 0) | (number ? Outcomes.False : 0) | (nulls ? Outcomes.Null : 0);
            }
        }

        Signs signs = values ? domain.PossibleSigns(s, literal, excludeNaN) : Signs.None;
        Signs satisfying = test switch
        {
            LeafTest.Eq or LeafTest.EqMissing => Signs.Equal,
            LeafTest.NotEq => Signs.Less | Signs.Greater,
            LeafTest.Lt => Signs.Less,
            LeafTest.LtEq => Signs.Less | Signs.Equal,
            LeafTest.Gt => Signs.Greater,
            _ => Signs.Greater | Signs.Equal,
        };
        Outcomes r = Outcomes.None;
        if ((signs & satisfying) != 0) r |= Outcomes.True;
        if ((signs & ~satisfying) != 0) r |= Outcomes.False;
        if (nulls) r |= test == LeafTest.EqMissing ? Outcomes.False : Outcomes.Null;
        return r;
    }
}

/// <summary>Which signs of <c>value - literal</c> the values of a unit may have.</summary>
[Flags]
internal enum Signs : byte
{
    None = 0,
    Less = 1,
    Equal = 2,
    Greater = 4,
    Any = Less | Equal | Greater,
}
