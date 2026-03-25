#pragma warning disable CS1591
#pragma warning disable CS1573
using Apache.Arrow;
using Microsoft.FSharp.Core;
using Polars.NET.Core;
using Polars.NET.Core.Helpers;

namespace Polars.CSharp;

/// <summary>
/// A Polars Expr
/// </summary>
public class Expr : IDisposable
{
    internal ExprHandle Handle { get; }

    internal Expr(ExprHandle handle)
    {
        Handle = handle;
    }
    internal static Expr MakeLit(object val)
    {
        if (val is Expr e) return e;

        if (val is Selector s) return s.ToExpr();

        return val switch
        {
            // --- Integer ---
            int i => new Expr(PolarsWrapper.Lit(i)),
            uint ui => new Expr(PolarsWrapper.Lit(ui)),
            long l => new Expr(PolarsWrapper.Lit(l)),
            ulong ul => new Expr(PolarsWrapper.Lit(ul)),
            short sh => new Expr(PolarsWrapper.Lit(sh)),
            ushort ush => new Expr(PolarsWrapper.Lit(ush)),
            byte by => new Expr(PolarsWrapper.Lit(by)),
            sbyte sb => new Expr(PolarsWrapper.Lit(sb)),
            Int128 i128 => new Expr(PolarsWrapper.Lit(i128)),

            // --- Float ---
            double d => new Expr(PolarsWrapper.Lit(d)),
            float f => new Expr(PolarsWrapper.Lit(f)),
            Half h => new Expr(PolarsWrapper.Lit(h)),

            // --- String and Boolean ---
            string str => new Expr(PolarsWrapper.Lit(str)),
            bool b => new Expr(PolarsWrapper.Lit(b)),

            // --- Time ---
            DateTime dt => new Expr(PolarsWrapper.Lit(dt)),
            DateTimeOffset dtos => new Expr(PolarsWrapper.Lit(dtos)),
            DateOnly date => new Expr(PolarsWrapper.Lit(date)),
            TimeOnly time => new Expr(PolarsWrapper.Lit(time)),
            TimeSpan ts => new Expr(PolarsWrapper.Lit(ts)),

            // --- Decimal ---
            decimal dec => Polars.Lit(dec),

            // --- Null ---
            null => new Expr(PolarsWrapper.LitNull()),

            // --- Exception ---
            _ => throw new NotSupportedException($"Unsupported literal type: {val.GetType().Name}")
        };
    }

    internal ExprHandle CloneHandle() => PolarsWrapper.CloneExpr(Handle);
    /// <summary>
    /// Clone Expr
    /// </summary>
    /// <returns></returns>
    public Expr Clone() => new(CloneHandle());

    public static implicit operator Expr(int value) => Polars.Lit(value);

    public static implicit operator Expr(double value) => Polars.Lit(value);

    public static implicit operator Expr(DateTime value) => Polars.Lit(value);

    public static implicit operator Expr(bool value) => Polars.Lit(value);

    public static implicit operator Expr(float value) => Polars.Lit(value);

    public static implicit operator Expr(long value) => Polars.Lit(value);

    /// <summary>
    /// Creates an expression evaluating if the left operand is greater than the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     a = new[] { 1, 2, 3, 4, null as int? },
    ///     b = new[] { 10, 20, 30, 40, 50 }
    /// });
    /// 
    /// // Compare columns: b > 20
    /// // Note: Null comparisons propagate null (Logic: 50 > null is null)
    /// df.Select(
    ///     Col("b"),
    ///     (Col("b") > 20).Alias("b_gt_20"),
    ///     (Col("a") == 2).Alias("a_eq_2")
    /// ).Show();
    /// /* Output:
    /// shape: (5, 3)
    /// ┌─────┬─────────┬────────┐
    /// │ b   ┆ b_gt_20 ┆ a_eq_2 │
    /// │ --- ┆ ---     ┆ ---    │
    /// │ i32 ┆ bool    ┆ bool   │
    /// ╞═════╪═════════╪════════╡
    /// │ 10  ┆ false   ┆ false  │
    /// │ 20  ┆ false   ┆ true   │
    /// │ 30  ┆ true    ┆ false  │
    /// │ 40  ┆ true    ┆ false  │
    /// │ 50  ┆ true    ┆ null   │
    /// └─────┴─────────┴────────┘
    /// */
    /// </code>
    /// </example>
    public static Expr operator >(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.Gt(l, r));
    }
    public static Expr operator >(Expr left, object right) =>
        new(PolarsWrapper.Gt(left.CloneHandle(), MakeLit(right).Handle));
    public static Expr operator >(object left, Expr right) =>
        new(PolarsWrapper.Gt(MakeLit(left).Handle, right.CloneHandle()));

    /// <summary>
    /// Creates an expression evaluating if the left operand is less than the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    /// <see cref="operator >(Expr, Expr)"/>
    public static Expr operator <(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.Lt(l, r));
    }
    public static Expr operator <(Expr left, object right) =>
        new(PolarsWrapper.Lt(left.CloneHandle(), MakeLit(right).Handle));
    public static Expr operator <(object left, Expr right) =>
        new(PolarsWrapper.Lt(MakeLit(left).Handle, right.CloneHandle()));
    /// <summary>
    /// Creates an expression evaluating if the left operand is greater than or equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    /// <see cref="operator >(Expr, Expr)"/>
    public static Expr operator >=(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.GtEq(l, r));
    }
    public static Expr operator >=(Expr left, object right) =>
        new(PolarsWrapper.GtEq(left.CloneHandle(), MakeLit(right).Handle));
    public static Expr operator >=(object left, Expr right) =>
        new(PolarsWrapper.GtEq(MakeLit(left).Handle, right.CloneHandle()));
    /// <summary>
    /// Creates an expression evaluating if the left operand is less than or equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    /// <see cref="operator >(Expr, Expr)"/>
    public static Expr operator <=(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.LtEq(l, r));
    }
    public static Expr operator <=(Expr left, object right) =>
        new(PolarsWrapper.LtEq(left.CloneHandle(), MakeLit(right).Handle));
    public static Expr operator <=(object left, Expr right) =>
        new(PolarsWrapper.LtEq(MakeLit(left).Handle, right.CloneHandle()));
    /// <summary>
    /// Creates an expression evaluating if the left operand is equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    /// <see cref="operator >(Expr, Expr)"/>
    public static Expr operator ==(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.Eq(l, r));
    }
    public static Expr operator ==(Expr left, object right) =>
        new(PolarsWrapper.Eq(left.CloneHandle(), MakeLit(right).Handle));
    public static Expr operator ==(object left, Expr right) =>
        new(PolarsWrapper.Eq(MakeLit(left).Handle, right.CloneHandle()));
    /// <summary>
    /// Creates an expression evaluating if the left operand is not equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    /// <see cref="operator >(Expr, Expr)"/>
    public static Expr operator !=(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.Neq(l, r));
    }
    public static Expr operator !=(Expr left, object right) =>
        new(PolarsWrapper.Neq(left.CloneHandle(), MakeLit(right).Handle));
    public static Expr operator !=(object left, Expr right) =>
        new(PolarsWrapper.Neq(MakeLit(left).Handle, right.CloneHandle()));

    // ==========================================
    // Sort
    // ==========================================
    /// <summary>
    /// Sort the expression.
    /// </summary>
    /// <param name="descending">If true, sort in descending order. Default is false.</param>
    /// <param name="nullsLast">Whether to place null values last. Default is false.</param>
    /// <param name="multithreaded">If true, sort in multiple threads. Default is true.</param>
    /// <param name="maintainOrder">If true, maintain the order of equal elements. Default is false.</param>
    /// <param name="limit">Limit the sort output (for optimization purposes).</param>
    public Expr Sort(
        bool descending = false,
        bool nullsLast = false,
        bool multithreaded = true,
        bool maintainOrder = false,
        uint? limit = null)
    {
        return new Expr(PolarsWrapper.Sort(
            CloneHandle(), 
            descending, 
            nullsLast, 
            multithreaded, 
            maintainOrder, 
            limit
        ));
    }
    // ==========================================
    // Indexing & Searching (Arg / Index / Search)
    // ==========================================
    /// <summary>
    /// Get a single value by index. Returns a scalar.
    /// </summary>
    /// <param name="index">The index expression.</param>
    /// <param name="nullOnOutOfBounds">If true, returns Null when the index is out of bounds instead of raising an error.</param>
    public Expr Get(Expr index, bool nullOnOutOfBounds = false)
        => new(PolarsWrapper.Get(CloneHandle(), index.CloneHandle(), nullOnOutOfBounds));
    /// <summary>
    /// Get a single value by index. Returns a scalar.
    /// </summary>
    /// <param name="index">The index number.</param>
    /// <param name="nullOnOutOfBounds">If true, returns Null when the index is out of bounds instead of raising an error.</param>
    public Expr Get(ulong index, bool nullOnOutOfBounds = false)
        => new(PolarsWrapper.Get(CloneHandle(), Polars.Lit(index).Handle, nullOnOutOfBounds));
    /// <summary>
    /// Gather values by an index expression.
    /// </summary>
    public Expr Gather(Expr indices)
        => new(PolarsWrapper.Gather(CloneHandle(), indices.CloneHandle()));

    /// <summary>
    /// LINQ-like alias for Gather.
    /// </summary>
    public Expr Take(Expr indices) => Gather(indices);

    /// <summary>
    /// Take every nth value starting from an offset.
    /// </summary>
    public Expr GatherEvery(ulong n, ulong offset = 0)
       => new(PolarsWrapper.GatherEvery(CloneHandle(), (nuint)n, (nuint)offset));
    /// <summary>
    /// Get the index of the unique values.
    /// </summary>
    public Expr ArgUnique() 
        => new(PolarsWrapper.ArgUnique(CloneHandle()));

    /// <summary>
    /// Get the index of the maximum value.
    /// </summary>
    public Expr ArgMax() 
        => new(PolarsWrapper.ArgMax(CloneHandle()));

    /// <summary>
    /// Get the index of the minimum value.
    /// </summary>
    public Expr ArgMin() 
        => new(PolarsWrapper.ArgMin(CloneHandle()));

    /// <summary>
    /// Get the index values that would sort this expression.
    /// </summary>
    /// <param name="descending">If true, sort in descending order. Default is false.</param>
    /// <param name="nullsLast">If true, place null values last. Default is false.</param>
    public Expr ArgSort(bool descending = false, bool nullsLast = false)
        => new(PolarsWrapper.ArgSort(CloneHandle(), descending, nullsLast));

    /// <summary>
    /// Find the index of the first occurrence of a specific value.
    /// </summary>
    /// <param name="element">The element expression to search for.</param>
    public Expr IndexOf(Expr element) 
        => new(PolarsWrapper.IndexOf(CloneHandle(), element.CloneHandle()));

    /// <summary>
    /// Find indices where elements should be inserted to maintain order (Binary Search).
    /// </summary>
    /// <param name="element">The element expression to insert/search.</param>
    /// <param name="side">The insertion side (Any, Left, Right). Default is Any.</param>
    /// <param name="descending">Whether the target column is sorted in descending order. Default is false.</param>
    public Expr SearchSorted(Expr element, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
        => new(PolarsWrapper.SearchSorted(CloneHandle(), element.CloneHandle(), side.ToNative(), descending));

    // ==========================================
    // Arithmetic Operators
    // ==========================================

    /// <summary>
    /// Creates an expression representing the addition of two expressions.
    /// <para>
    /// Supports element-wise addition. If one operand is a scalar/literal, it is broadcast to the length of the other.
    /// </para>
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the sum.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     a = new[] { 1, 2, 3, 4, null as int? },
    ///     b = new[] { 10, 20, 30, 40, 50 }
    /// });
    /// 
    /// // Arithmetic: Column + Column, Column * Scalar
    /// df.Select(
    ///     (Col("a") + Col("b")).Alias("sum_ab"),
    ///     (Col("a") * 2).Alias("a_double")
    /// ).Show();
    /// /* Output:
    /// shape: (5, 2)
    /// ┌────────┬──────────┐
    /// │ sum_ab ┆ a_double │
    /// │ ---    ┆ ---      │
    /// │ i32    ┆ i32      │
    /// ╞════════╪══════════╡
    /// │ 11     ┆ 2        │
    /// │ 22     ┆ 4        │
    /// │ 33     ┆ 6        │
    /// │ 44     ┆ 8        │
    /// │ null   ┆ null     │
    /// └────────┴──────────┘
    /// */
    /// </code>
    /// </example>
    public static Expr operator +(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.Add(l, r));
    }
    public static Expr operator +(Expr left, object right) =>
        new(PolarsWrapper.Add(left.CloneHandle(), MakeLit(right).Handle));
    public static Expr operator +(object left, Expr right) =>
        new(PolarsWrapper.Add(MakeLit(left).Handle, right.CloneHandle()));
    /// <summary>
    /// Creates an expression representing the subtraction of the right expression from the left expression.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the difference.</returns>
    /// <see cref="operator +(Expr, Expr)"/>
    public static Expr operator -(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.Sub(l, r));
    }
    public static Expr operator -(Expr left, object right) =>
        new(PolarsWrapper.Sub(left.CloneHandle(), MakeLit(right).Handle));
    public static Expr operator -(object left, Expr right) =>
        new(PolarsWrapper.Sub(MakeLit(left).Handle, right.CloneHandle()));
    /// <summary>
    /// Creates an expression representing the multiplication of two expressions.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the product.</returns>
    /// <see cref="operator +(Expr, Expr)"/>
    public static Expr operator *(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.Mul(l, r));
    }
    public static Expr operator *(Expr left, object right) =>
        new(PolarsWrapper.Mul(left.CloneHandle(), MakeLit(right).Handle));
    public static Expr operator *(object left, Expr right) =>
        new(PolarsWrapper.Mul(MakeLit(left).Handle, right.CloneHandle()));
    /// <summary>
    /// Creates an expression representing the division of the left expression by the right expression.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the quotient.</returns>
    /// <see cref="operator +(Expr, Expr)"/>
    public static Expr operator /(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.Div(l, r));
    }
    public static Expr operator /(Expr left, object right) =>
        new(PolarsWrapper.Div(left.CloneHandle(), MakeLit(right).Handle));
    public static Expr operator /(object left, Expr right) =>
        new(PolarsWrapper.Div(MakeLit(left).Handle, right.CloneHandle()));
    /// <summary>
    /// Creates an expression representing the remaining of the left expression by the right expression.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the quotient.</returns>
    /// <see cref="operator +(Expr, Expr)"/>
    public static Expr operator %(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.Rem(l, r));
    }
    public static Expr operator %(Expr left, object right) =>
        new(PolarsWrapper.Rem(left.CloneHandle(), MakeLit(right).Handle));
    public static Expr operator %(object left, Expr right) =>
        new(PolarsWrapper.Rem(MakeLit(left).Handle, right.CloneHandle()));

    /// <summary>
    /// Integer division (floor division).
    /// </summary>
    public Expr FloorDiv(Expr other)
        => new(PolarsWrapper.FloorDiv(this.CloneHandle(), other.CloneHandle()));

    public Expr FloorDiv(object other)
        => new(PolarsWrapper.FloorDiv(this.CloneHandle(), MakeLit(other).Handle));
    // ==========================================
    // Bitwise Operators (<<, >>)
    // ==========================================

    /// <summary>
    /// Bitwise left shift operation.
    /// <para>
    /// Equivalent to C# `&lt;&lt;` operator. 
    /// Shifting left by N is equivalent to multiplying by 2^N.
    /// </para>
    /// </summary>
    /// <param name="left">The expression to shift.</param>
    /// <param name="right">The number of bits to shift.</param>
    /// <returns>A numeric expression with bits shifted left.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     vals = new[] { 1, 2, 4, 8, 16 },    // Powers of 2
    ///     neg  = new[] { -2, -4, -8, -16, -32 } // Negative numbers
    /// });
    /// 
    /// // 1. Left shift by 1 (x * 2)
    /// // 2. Right shift by 1 (x / 2)
    /// // 3. Negative Right shift (Arithmetic Shift, preserves sign)
    /// df.Select(
    ///     Col("vals"),
    ///     (Col("vals") &lt;&lt; 1).Alias("shl_1"), 
    ///     (Col("vals") &gt;&gt; 1).Alias("shr_1"), 
    ///     Col("neg"),
    ///     (Col("neg") &gt;&gt; 1).Alias("neg_shr_1") 
    /// ).Show();
    /// /* Output:
    /// shape: (5, 5)
    /// ┌──────┬───────┬───────┬─────┬───────────┐
    /// │ vals ┆ shl_1 ┆ shr_1 ┆ neg ┆ neg_shr_1 │
    /// │ ---  ┆ ---   ┆ ---   ┆ --- ┆ ---       │
    /// │ i32  ┆ i32   ┆ i32   ┆ i32 ┆ i32       │
    /// ╞══════╪═══════╪═══════╪═════╪═══════════╡
    /// │ 1    ┆ 2     ┆ 0     ┆ -2  ┆ -1        │
    /// │ 2    ┆ 4     ┆ 1     ┆ -4  ┆ -2        │
    /// │ 4    ┆ 8     ┆ 2     ┆ -8  ┆ -4        │
    /// │ 8    ┆ 16    ┆ 4     ┆ -16 ┆ -8        │
    /// │ 16   ┆ 32    ┆ 8     ┆ -32 ┆ -16       │
    /// └──────┴───────┴───────┴─────┴───────────┘
    /// */
    /// </code>
    /// </example>
    public static Expr operator <<(Expr left, int right)
        => new(PolarsWrapper.BitLeftShift(left.CloneHandle(), right));
    
    /// <summary>
    /// Bitwise right shift operation.
    /// <para>
    /// For signed integers, this is an **arithmetic shift** (preserves the sign bit).
    /// For unsigned integers, this is a logical shift (fills with zeros).
    /// </para>
    /// </summary>
    /// <param name="left">The expression to shift.</param>
    /// <param name="right">The number of bits to shift.</param>
    /// <returns>A numeric expression with bits shifted right.</returns>
    /// <seealso cref="operator &lt;&lt;(Expr, int)"/>
    public static Expr operator >>(Expr left, int right)
        =>new(PolarsWrapper.BitRightShift(left.CloneHandle(), right));

    // ==========================================
    // Logical Operators
    // ==========================================

    /// <summary>
    /// Creates an expression representing the logical AND operation.
    /// </summary>
    /// <param name="left">The left boolean expression.</param>
    /// <param name="right">The right boolean expression.</param>
    /// <returns>A boolean expression that evaluates to true if both operands are true.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     b = new[] { 10, 20, 30, 40, 50 },
    ///     cond = new[] { true, false, true, false, true }
    /// });
    /// 
    /// // Logical AND: (b > 20) AND cond
    /// df.Select(
    ///     Col("b"),
    ///     Col("cond"),
    ///     ((Col("b") &gt; 20) &amp; Col("cond")).Alias("logic_and")
    /// ).Show();
    /// /* Output:
    /// shape: (5, 3)
    /// ┌─────┬───────┬───────────┐
    /// │ b   ┆ cond  ┆ logic_and │
    /// │ --- ┆ ---   ┆ ---       │
    /// │ i32 ┆ bool  ┆ bool      │
    /// ╞═════╪═══════╪═══════════╡
    /// │ 10  ┆ true  ┆ false     │
    /// │ 20  ┆ false ┆ false     │
    /// │ 30  ┆ true  ┆ true      │
    /// │ 40  ┆ false ┆ false     │
    /// │ 50  ┆ true  ┆ true      │
    /// └─────┴───────┴───────────┘
    /// */
    /// </code>
    /// </example>
    public static Expr operator &(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.And(l, r));
    }
    public static Expr operator &(Expr left, bool right) =>
        new(PolarsWrapper.And(left.CloneHandle(), MakeLit(right).Handle));
    /// <summary>
    /// Creates an expression representing the logical OR operation.
    /// </summary>
    /// <param name="left">The left boolean expression.</param>
    /// <param name="right">The right boolean expression.</param>
    /// <returns>A boolean expression that evaluates to true if at least one operand is true.</returns>
    public static Expr operator |(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.Or(l, r));
    }
    public static Expr operator |(Expr left, bool right) =>
        new(PolarsWrapper.Or(left.CloneHandle(), MakeLit(right).Handle));
    /// <summary>
    /// Creates an expression representing the logical NOT operation.
    /// </summary>
    /// <param name="expr">The boolean expression to negate.</param>
    /// <returns>A boolean expression that evaluates to the opposite truth value.</returns>
    public static Expr operator !(Expr expr)
        => new(PolarsWrapper.Not(expr.CloneHandle()));
    /// <summary>
    /// Creates an expression representing the logical XOR operation.
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Expr operator ^(Expr left, Expr right)
    {
        var l = left.CloneHandle();
        var r = right.CloneHandle();
        return new Expr(PolarsWrapper.Xor(l, r));
    }
    public static Expr operator ^(Expr left, bool right)
        => new(PolarsWrapper.Xor(left.CloneHandle(), MakeLit(right).Handle));

    public static Expr operator ^(bool left, Expr right)
        => new(PolarsWrapper.Xor(MakeLit(left).Handle, right.CloneHandle()));

    /// <inheritdoc cref="Polars.SqlExpr(string)"/>
    public static Expr SqlExpr(string sql)
        =>Polars.SqlExpr(sql);

    /// <inheritdoc cref="Polars.SqlExprs"/>
    public static Expr[] SqlExprs(IEnumerable<string> sqls) 
            => [.. sqls.Select(SqlExpr)];
    // ---------------------------------------------------
    // Methods
    // ---------------------------------------------------
    /// <summary>
    /// Set a new name for a column
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public Expr Alias(string name) =>
        new(PolarsWrapper.Alias(CloneHandle(), name));
    /// <summary>
    /// Reverse the selection.
    /// <para>This is useful in a GroupBy context to reverse the order of the group.</para>
    /// </summary>
    /// <returns>A new expression with the order reversed.</returns>
    public Expr Reverse() => new(PolarsWrapper.Reverse(CloneHandle()));

    // ==========================================
    // Aggregation
    // ==========================================
    /// <summary>
    /// Get the first value of the group/series.
    /// </summary>
    /// <returns>A new expression representing the first value.</returns>
    public Expr First(bool ignoreNulls = false) => new(PolarsWrapper.First(CloneHandle(),ignoreNulls));
    /// <summary>
    /// Get the last value of the group/series.
    /// </summary>
    /// <returns>A new expression representing the last value.</returns>
    public Expr Last(bool ignoreNulls = false) => new(PolarsWrapper.Last(CloneHandle(),ignoreNulls));

    /// <summary>
    /// Get the first n rows.
    /// </summary>
    /// <param name="n">Number of rows to return.</param>
    /// <returns></returns>
    public Expr Head(int n = 10) => new(PolarsWrapper.Head(CloneHandle(),n));
    /// <summary>
    /// Get the last n rows.
    /// </summary>
    /// <param name="n">Number of rows to return.</param>
    /// <returns></returns>
    public Expr Tail(int n = 10) => new(PolarsWrapper.Tail(CloneHandle(),n));

    /// <summary>
    /// Check if <b>all</b> values in the boolean expression are <c>true</c>.
    /// <para>This is a boolean aggregation.</para>
    /// </summary>
    /// <param name="ignoreNulls">
    /// If <c>true</c>, null values are ignored. 
    /// If <c>false</c> (default), the result propagates nulls (i.e., if there is a null and no false, the result might be null).
    /// </param>
    /// <returns>A new expression representing the boolean result.</returns>
    public Expr All(bool ignoreNulls=false) => new(PolarsWrapper.All(CloneHandle(),ignoreNulls));
    /// <summary>
    /// Check if <b>any</b> value in the boolean expression is <c>true</c>.
    /// <para>This is a boolean aggregation.</para>
    /// </summary>
    /// <param name="ignoreNulls">
    /// If <c>true</c>, null values are ignored. 
    /// If <c>false</c> (default), the result propagates nulls.
    /// </param>
    /// <returns>A new expression representing the boolean result.</returns>
    public Expr Any(bool ignoreNulls=false) => new(PolarsWrapper.Any(CloneHandle(),ignoreNulls));
    /// <summary>
    /// Return the single value in the group or series.
    /// <para>
    /// This is strict: it expects the group/series to contain exactly <b>one</b> element.
    /// </para>
    /// </summary>
    /// <remarks>
    /// If the group contains more than one element, this will throw an error at runtime.
    /// It is safer than <see cref="First"/> when you expect uniqueness (e.g., getting the ID of a group).
    /// </remarks>
    /// <param name="allowEmpty">
    /// If <c>true</c> and the group is empty, it returns <c>null</c> instead of throwing an error.
    /// Default is <c>true</c>.
    /// </param>
    /// <returns>A new expression representing the single item.</returns>
    public Expr Item(bool allowEmpty=true) => new(PolarsWrapper.Item(CloneHandle(),allowEmpty));

    /// <summary>
    /// Calculate the sum of the values in the group or column.
    /// <para>
    /// Behavior depends on context:
    /// <list type="bullet">
    /// <item>In <see cref="DataFrame.GroupBy(Expr[])"/>: Calculates the sum for each group.</item>
    /// <item>In <see cref="DataFrame.Select(Expr[])"/>: Calculates the sum of the entire column (scalar result).</item>
    /// </list>
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B" },
    ///     val = new[] { 1, 2, 3, 4 }
    /// });
    /// 
    /// // 1. GroupBy Aggregation
    /// df.GroupBy("group").Agg(
    ///     Col("val").Sum().Alias("sum"),
    ///     Col("val").Mean().Alias("mean")
    /// ).Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌───────┬─────┬──────┐
    /// │ group ┆ sum ┆ mean │
    /// │ ---   ┆ --- ┆ ---  │
    /// │ str   ┆ i32 ┆ f64  │
    /// ╞═══════╪═════╪══════╡
    /// │ A     ┆ 3   ┆ 1.5  │
    /// │ B     ┆ 7   ┆ 3.5  │
    /// └───────┴─────┴──────┘
    /// */
    /// 
    /// // 2. Global Aggregation (Select)
    /// df.Select(
    ///     Col("val").Sum().Alias("total_sum"),
    ///     Col("val").Count().Alias("total_count")
    /// ).Show();
    /// /* Output:
    /// shape: (1, 2)
    /// ┌───────────┬─────────────┐
    /// │ total_sum ┆ total_count │
    /// │ ---       ┆ ---         │
    /// │ i32       ┆ u32         │
    /// ╞═══════════╪═════════════╡
    /// │ 10        ┆ 4           │
    /// └───────────┴─────────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Sum() => new(PolarsWrapper.Sum(CloneHandle()));

    /// <summary>
    /// Compute the mean of an expression
    /// </summary>
    public Expr Mean() => new(PolarsWrapper.Mean(CloneHandle()));

    /// <summary>
    /// Compute the max of an expression
    /// </summary>
    public Expr Max() => new(PolarsWrapper.Max(CloneHandle()));

    /// <summary>
    /// Compute the min of an expression
    /// </summary>
    public Expr Min() => new(PolarsWrapper.Min(CloneHandle()));
    /// <summary>
    /// Count the number of null.
    /// </summary>
    public Expr NullCount() => new(PolarsWrapper.NullCount(CloneHandle()));
    /// <summary>
    /// Count unique values.
    /// Notes: Null is considered to be a unique value for the purposes of this operation.
    /// </summary>
    public Expr NUnique() => new(PolarsWrapper.NUnique(CloneHandle()));
    /// <summary>
    /// Compute the product of an expression
    /// </summary>
    /// <returns></returns>
    public Expr Product() => new(PolarsWrapper.Product(CloneHandle()));
    

    // ==========================================
    // Math
    // ==========================================

    /// <summary>
    /// Calculate the absolute value of the expression.
    /// </summary>
    public Expr Abs() => new(PolarsWrapper.Abs(CloneHandle()));

    /// <summary>
    /// Calculate the square root of the expression.
    /// </summary>
    public Expr Sqrt() => new(PolarsWrapper.Sqrt(CloneHandle()));

    /// <summary>
    /// Calculate the cube root of the expression.
    /// </summary>
    public Expr Cbrt() => new(PolarsWrapper.Cbrt(CloneHandle()));

    /// <summary>
    /// Calculate the power of the expression with a given exponent expression.
    /// </summary>
    public Expr Pow(Expr exponent) => new(PolarsWrapper.Pow(CloneHandle(), exponent.CloneHandle()));

    /// <summary>
    /// Calculate the power of the expression with a given numeric exponent.
    /// </summary>
    public Expr Pow(double exponent) => new(PolarsWrapper.Pow(CloneHandle(), PolarsWrapper.Lit(exponent)));
    /// <summary>
    /// Compute the dot/inner product between two expressions.
    /// <para>
    /// The dot product is the sum of the products of the corresponding entries of the two sequences of numbers.
    /// </para>
    /// </summary>
    /// <param name="other">The other expression to compute the dot product with.</param>
    /// <returns>A scalar expression representing the dot product result.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new 
    /// {
    ///     a = new[] { 1, 2, 3 },
    ///     b = new[] { 4, 5, 6 }
    /// });
    /// 
    /// // (1*4) + (2*5) + (3*6) = 4 + 10 + 18 = 32
    /// df.Select(Col("a").Dot(Col("b"))).Show();
    /// </code>
    /// </example>
    public Expr Dot(Expr other) => new(PolarsWrapper.Dot(CloneHandle(), other.CloneHandle()));
    /// <summary>
    /// Calculate the power of the Euler's number.
    /// </summary>
    public Expr Exp() => new(PolarsWrapper.Exp(CloneHandle()));

    /// <summary>
    /// Calculate the ln of Number 
    /// </summary>
    /// <param name="baseVal"></param>
    /// <returns></returns>
    public Expr Ln(double baseVal = Math.E) => new(PolarsWrapper.Log(CloneHandle(), baseVal));

    // ==========================================
    // Trigonometry
    // ==========================================

    /// <summary>Compute the element-wise sine.</summary>
    public Expr Sin() => new(PolarsWrapper.Sin(CloneHandle()));

    /// <summary>Compute the element-wise cosine.</summary>
    public Expr Cos() => new(PolarsWrapper.Cos(CloneHandle()));

    /// <summary>Compute the element-wise tangent.</summary>
    public Expr Tan() => new(PolarsWrapper.Tan(CloneHandle()));

    /// <summary>Compute the element-wise inverse sine.</summary>
    public Expr ArcSin() => new(PolarsWrapper.ArcSin(CloneHandle()));

    /// <summary>Compute the element-wise inverse cosine.</summary>
    public Expr ArcCos() => new(PolarsWrapper.ArcCos(CloneHandle()));

    /// <summary>Compute the element-wise inverse tangent.</summary>
    public Expr ArcTan() => new(PolarsWrapper.ArcTan(CloneHandle()));

    // Hyperbolic
    public Expr Sinh() => new(PolarsWrapper.Sinh(CloneHandle()));
    public Expr Cosh() => new(PolarsWrapper.Cosh(CloneHandle()));
    public Expr Tanh() => new(PolarsWrapper.Tanh(CloneHandle()));

    public Expr ArcSinh() => new(PolarsWrapper.ArcSinh(CloneHandle()));
    public Expr ArcCosh() => new(PolarsWrapper.ArcCosh(CloneHandle()));
    public Expr ArcTanh() => new(PolarsWrapper.ArcTanh(CloneHandle()));

    // ==========================================
    // Rounding & Sign
    // ==========================================
    /// <summary>
    /// Round the number
    /// </summary>
    /// <param name="decimals"></param>
    /// <returns></returns>
    public Expr Round(uint decimals) => new(PolarsWrapper.Round(CloneHandle(), decimals));
    /// <summary>Compute the element-wise sign (-1, 0, 1).</summary>
    public Expr Sign() => new(PolarsWrapper.Sign(CloneHandle()));

    /// <summary>Rounds up to the nearest integer.</summary>
    public Expr Ceil() => new(PolarsWrapper.Ceil(CloneHandle()));

    /// <summary>Rounds down to the nearest integer.</summary>
    public Expr Floor() => new(PolarsWrapper.Floor(CloneHandle()));

    // ==========================================
    // Null Handling
    // ==========================================

    /// <summary>
    /// Fill null values with a specified value.
    /// </summary>
    /// <param name="fillValue">The expression (or literal) to replace nulls with.</param>
    public Expr FillNull(Expr fillValue) => new(PolarsWrapper.FillNull(CloneHandle(), fillValue.CloneHandle()));
    /// <summary>
    /// Fill null values with a specified literal value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Expr FillNull(object value) => FillNull(MakeLit(value));
    /// <summary>
    /// Fill null values with a specific strategy (Forward).
    /// </summary>
    /// <param name="limit">Max number of consecutive nulls to fill. (Default null = infinite)</param>
    public Expr ForwardFill(uint? limit = null) => new(PolarsWrapper.ForwardFill(CloneHandle(), limit ?? 0));
    /// <summary>
    /// Fill null values with a specific strategy (Backward).
    /// </summary>
    public Expr BackwardFill(uint? limit = null) => new(PolarsWrapper.BackwardFill(CloneHandle(), limit ?? 0));
    /// <summary>
    /// Interpolate intermediate values. The interpolation method can be configured.
    /// <para>Nulls at the beginning and end of the series remain null.</para>
    /// </summary>
    /// <param name="method">Interpolation method (Linear or Nearest).</param>
    public Expr Interpolate(InterpolationMethod method = InterpolationMethod.Linear)
        => new(PolarsWrapper.Interpolate(CloneHandle(), method.ToNative()));
    /// <summary>
    /// Interpolate intermediate values based on the values of another column.
    /// <para>
    /// This is useful when the data is not equally spaced, for example when interpolating based on a timestamp column.
    /// </para>
    /// </summary>
    /// <param name="by">The column to use for interpolation (e.g. a timestamp column).</param>
    /// <returns>A new expression with interpolated values.</returns>
    public Expr InterpolateBy(Expr by) 
        => new(PolarsWrapper.InterpolateBy(CloneHandle(), by.CloneHandle()));
    /// <summary>
    /// Evaluate whether the expression is null.
    /// </summary>
    public Expr IsNull() => new(PolarsWrapper.IsNull(CloneHandle()));
    /// <summary>
    /// Evaluate whether the expression is not null.
    /// </summary>
    public Expr IsNotNull() => new(PolarsWrapper.IsNotNull(CloneHandle()));
    /// <summary>
    /// Fill floating point NaN values with a specified value.
    /// Note: This is different from FillNull. It only handles IEEE 754 NaN.
    /// </summary>
    public Expr FillNan(object value)
        => new(PolarsWrapper.FillNan(CloneHandle(), MakeLit(value).Handle));
    /// <summary>
    /// Drop null values.
    /// </summary>
    public Expr DropNulls() => new(PolarsWrapper.DropNulls(CloneHandle()));
    /// <summary>
    /// Drop nan values.
    /// </summary>
    public Expr DropNans() => new(PolarsWrapper.DropNans(CloneHandle()));
    // ==========================================
    // Top-K & Bottom-K
    // ==========================================
    /// <summary>
    /// Get the top k largest values.
    /// <para>This is much faster than Sort().Head(k) for large datasets.</para>
    /// </summary>
    public Expr TopK(int k) => new(PolarsWrapper.TopK(CloneHandle(), (uint)k));

    /// <summary>
    /// Get the bottom k smallest values.
    /// <para>This is much faster than Sort().Tail(k) for large datasets.</para>
    /// </summary>
    public Expr BottomK(int k) => new(PolarsWrapper.BottomK(CloneHandle(), (uint)k));
    /// <summary>
    /// Get the top <paramref name="k"/> rows according to the sorting criteria defined by <paramref name="by"/>.
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">The expressions (columns) to sort by.</param>
    /// <param name="reverse">
    /// Controls the sorting direction for each expression in <paramref name="by"/>.
    /// <para>
    /// For <b>TopK</b>: 
    /// <br/>- <c>false</c> (default): Sorts <b>descending</b> (picks largest values).
    /// <br/>- <c>true</c>: Sorts <b>ascending</b> (picks smallest values, acting like BottomK for this column).
    /// </para>
    /// Length must match <paramref name="by"/>.
    /// </param>
    /// <returns>A new expression.</returns>
    /// <exception cref="ArgumentException">If the length of <paramref name="by"/> and <paramref name="reverse"/> do not match.</exception>
    public Expr TopKBy(int k, Expr[] by, bool[] reverse)
    {
        if (by.Length != reverse.Length)
            throw new ArgumentException("The length of 'by' and 'reverse' must match.");

        var byHandles = System.Array.ConvertAll(by, e => e.CloneHandle());

        return new Expr(PolarsWrapper.TopKBy(CloneHandle(), (uint)k, byHandles, reverse));
    }

    /// <summary>
    /// Get the top <paramref name="k"/> rows according to a single sorting criterion.
    /// <para>This is a convenience overload for a single expression.</para>
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">The expression (column) to sort by.</param>
    /// <param name="reverse">
    /// <inheritdoc cref="TopKBy(int, Expr[], bool[])" path="/param[@name='reverse']/node()"/>
    /// </param>
    /// <returns>A new expression.</returns>
    public Expr TopKBy(int k, Expr by, bool reverse = false)
        => TopKBy(k, [by], [reverse]);

    /// <summary>
    /// Get the bottom <paramref name="k"/> rows according to the sorting criteria defined by <paramref name="by"/>.
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">The expressions (columns) to sort by.</param>
    /// <param name="reverse">
    /// Controls the sorting direction for each expression in <paramref name="by"/>.
    /// <para>
    /// For <b>BottomK</b>: 
    /// <br/>- <c>false</c> (default): Sorts <b>ascending</b> (picks smallest values).
    /// <br/>- <c>true</c>: Sorts <b>descending</b> (picks largest values, acting like TopK for this column).
    /// </para>
    /// Length must match <paramref name="by"/>.
    /// </param>
    /// <returns>A new expression.</returns>
    public Expr BottomKBy(int k, Expr[] by, bool[] reverse)
    {
        if (by.Length != reverse.Length)
            throw new ArgumentException("The length of 'by' and 'reverse' must match.");

        var byHandles = System.Array.ConvertAll(by, e => e.CloneHandle());

        return new Expr(PolarsWrapper.BottomKBy(CloneHandle(), (uint)k, byHandles, reverse));
    }

    /// <summary>
    /// Get the bottom <paramref name="k"/> rows according to a single sorting criterion.
    /// <para>This is a convenience overload for a single expression.</para>
    /// </summary>
    /// <inheritdoc cref="BottomKBy(int, Expr[], bool[])" path="/param[@name='reverse']"/>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">The expression (column) to sort by.</param>
    /// <param name="reverse">See <see cref="BottomKBy(int, Expr[], bool[])"/>.</param>
    /// <returns>A new expression.</returns>
    public Expr BottomKBy(int k, Expr by, bool reverse = false)
        => BottomKBy(k, [by], [reverse]);
    // ==========================================
    // Unique and Duplicated
    // ==========================================
    /// <summary>
    /// Create a boolean expression indicating whether the value is unique.
    /// </summary>
    public Expr IsUnique() => new(PolarsWrapper.ExprIsUnique(CloneHandle()));

    /// <summary>
    /// Create a boolean expression indicating whether the value is duplicated.
    /// </summary>
    public Expr IsDuplicated() => new(PolarsWrapper.ExprIsDuplicated(CloneHandle()));

    /// <summary>
    /// Get unique values.
    /// </summary>
    public Expr Unique() => new(PolarsWrapper.ExprUnique(CloneHandle()));

    /// <summary>
    /// Get unique values, maintaining order.
    /// </summary>
    public Expr UniqueStable() => new(PolarsWrapper.ExprUniqueStable(CloneHandle()));
    // ==========================================
    // Statistical Ops
    // ==========================================

    /// <summary>
    /// Return the number of non-null elements in the column.
    /// </summary>
    public Expr Count() => new(PolarsWrapper.Count(CloneHandle()));
    /// <summary>
    /// Return the number of elements in the column.
    /// Null values count towards the total.
    /// </summary>
    /// <returns></returns>
    public Expr Len() => new(PolarsWrapper.ExprLen(CloneHandle()));

    /// <summary>
    /// Get the standard deviation.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Std(int ddof = 1) => new(PolarsWrapper.Std(CloneHandle(), ddof));

    /// <summary>
    /// Get the variance.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Var(int ddof = 1) => new(PolarsWrapper.Var(CloneHandle(), ddof));

    /// <summary>
    /// Get the median value.
    /// </summary>
    /// <returns>A series which length is 1</returns>
    public Expr Median() => new(PolarsWrapper.Median(CloneHandle()));
    /// <summary>
    /// Get the mode value.
    /// </summary>
    /// <returns>A series which length is 1</returns>
    public Expr Mode() => new(PolarsWrapper.Mode(CloneHandle()));
    /// <summary>
    /// Compute the sample skewness of a data set.
    /// </summary>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Skew(bool bias = true) => new(PolarsWrapper.Skew(CloneHandle(), bias));
    /// <summary>
    /// Compute the kurtosis (Fisher or Pearson) of a dataset.
    /// </summary>
    /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Kurtosis(bool fisher = true, bool bias = true) => new(PolarsWrapper.Kurtosis(CloneHandle(), fisher, bias));
    /// <summary>
    /// Get the quantile value.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="method">['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Quantile(double quantile, QuantileMethod method = QuantileMethod.Linear)
        => new(PolarsWrapper.Quantile(CloneHandle(), quantile, method.ToNative()));
    /// <summary>
    /// Computes percentage change between values.
    /// Percentage change (as fraction) between current element and most-recent non-null element at least n period(s) before the current element.
    /// Computes the change from the previous row by default.
    /// </summary>
    /// <param name="n">periods to shift for forming percent change.</param>
    /// <returns>A series which length is 1</returns>
    public Expr PctChange(int n = 1)
        => new(PolarsWrapper.PctChange(CloneHandle(), n));
    /// <summary>
    /// Assign ranks to data, dealing with ties appropriately.
    /// </summary>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="descending">Rank in descending order.</param>
    /// <param name="seed">If method="random", use this as seed.</param>
    /// <returns></returns>
    public Expr Rank(RankMethod method = RankMethod.Average, bool descending = false, ulong? seed = null)
        => new(PolarsWrapper.Rank(CloneHandle(), method.ToNative(), descending, seed));
    // ==========================================
    // Cumulative Functions
    // ==========================================
    /// <summary>
    /// Get an array with the cumulative sum computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumSum(bool reverse = false)
        => new(PolarsWrapper.CumSum(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative max computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumMax(bool reverse = false)
        => new(PolarsWrapper.CumMax(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative min computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumMin(bool reverse = false)
        => new(PolarsWrapper.CumMin(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative prod computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumProd(bool reverse = false)
        => new(PolarsWrapper.CumProd(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative count computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumCount(bool reverse = false)
        => new(PolarsWrapper.CumCount(CloneHandle(), reverse));
    // ==========================================
    // EWM Functions
    // ==========================================
    /// <summary>
    /// Compute exponentially-weighted moving average.
    /// </summary>
    /// <param name="alpha">
    /// Specify smoothing factor alpha directly. 
    /// <para>Constraint: <c>0 &lt; alpha &lt;= 1</c></para>
    /// </param>
    /// <param name="adjust">
    /// If <c>true</c>, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). 
    /// If <c>false</c>, assume infinite history.
    /// </param>
    /// <param name="bias">
    /// If <c>true</c>, use a biased estimator (Standard deviation uses <c>N</c> in denominator). 
    /// If <c>false</c>, use an unbiased estimator (Standard deviation uses <c>N-1</c>).
    /// <para>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true.</para>
    /// </param>
    /// <param name="minPeriods">Minimum number of observations in window required to have a value (otherwise result is null).</param>
    /// <param name="ignoreNulls">Ignore missing values when calculating weights.</param>
    /// <returns>A new expression representing the EWM mean.</returns>
    public Expr EwmMean(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => new(PolarsWrapper.EwmMean(CloneHandle(), alpha, adjust, bias, minPeriods, ignoreNulls));
    /// <summary>
    /// Compute exponentially-weighted moving standard deviation.
    /// </summary>
    /// <inheritdoc cref="EwmMean"/>
    /// <returns>A new expression representing the EWM standard deviation.</returns>
    public Expr EwmStd(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => new(PolarsWrapper.EwmStd(CloneHandle(), alpha, adjust, bias, minPeriods, ignoreNulls));
    /// <summary>
    /// Compute exponentially-weighted moving variance.
    /// </summary>
    /// <inheritdoc cref="EwmMean"/>
    /// <returns>A new expression representing the EWM variance.</returns>
    public Expr EwmVar(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => new(PolarsWrapper.EwmVar(CloneHandle(), alpha, adjust, bias, minPeriods, ignoreNulls));
    /// <summary>
    /// Compute exponentially-weighted moving average based on a temporal or index column.
    /// </summary>
    /// <param name="by">
    /// The column used to determine the distance between observations.
    /// <para>Supported data types: <c>Date</c>, <c>DateTime</c>, <c>UInt64</c>, <c>UInt32</c>, <c>Int64</c>, or <c>Int32</c>.</para>
    /// </param>
    /// <param name="halfLife">
    /// The unit over which an observation decays to half its value.
    /// <para>Supported string formats:</para>
    /// <list type="bullet">
    ///     <item><term>Time units</term><description><c>ns</c> (nanosecond), <c>us</c> (microsecond), <c>ms</c> (millisecond), <c>s</c> (second), <c>m</c> (minute), <c>h</c> (hour), <c>d</c> (day), <c>w</c> (week).</description></item>
    ///     <item><term>Index units</term><description><c>i</c> (index count). Example: <c>"2i"</c> means decay by half every 2 index steps.</description></item>
    ///     <item><term>Compound</term><description>Example: <c>"3d12h4m25s"</c>.</description></item>
    /// </list>
    /// <para>
    /// <b>Warning:</b> <paramref name="halfLife"/> is treated as a constant duration. 
    /// Calendar durations such as months (<c>mo</c>) or years (<c>y</c>) are <b>NOT</b> supported because they vary in length. 
    /// Please express such durations in hours (e.g. use <c>'730h'</c> instead of <c>'1mo'</c>).
    /// </para>
    /// </param>
    /// <returns>A new expression representing the time/index-based EWM mean.</returns>
    public Expr EwmMeanBy(Expr by, string halfLife)
    {
        return new Expr(PolarsWrapper.EwmMeanBy(
            CloneHandle(),
            by.CloneHandle(),
            halfLife
        ));
    }

    // ==========================================
    // Logic / Comparison
    // ==========================================

    /// <summary>
    /// Check if the value is between lower and upper bounds (inclusive).
    /// </summary>
    public Expr IsBetween(Expr lower, Expr upper)
        => new(PolarsWrapper.IsBetween(CloneHandle(), lower.CloneHandle(), upper.CloneHandle()));
    /// <summary>
    /// Check if the value is in given collection.
    /// </summary>
    public Expr IsIn(Expr other, bool nullsEqual = false)
     => new(PolarsWrapper.IsIn(CloneHandle(),other.CloneHandle(),nullsEqual));
    /// <summary>
    /// Filter a single column.
    /// <br/>
    /// Mostly useful in <c>group_by</c> context or when you want to filter an expression based on another expression within a <c>Select</c> context.
    /// </summary>
    /// <param name="predicate">Boolean expression used to filter the current expression.</param>
    /// <returns>A new expression with filtered values.</returns>
    public Expr Filter(Expr predicate)
     => new(PolarsWrapper.Filter(CloneHandle(),predicate.CloneHandle()));
    // ==========================================
    // Casting
    // ==========================================

    /// <summary>
    /// Cast the expression to a different data type.
    /// </summary>
    public Expr Cast(DataType dtype, bool strict = false)
        => new(PolarsWrapper.ExprCast(CloneHandle(), dtype.Handle, strict));

    // ==========================================
    // UDF / Map
    // ==========================================
    /// <summary>
    /// Apply a custom C# function to the expression.
    /// This runs locally in the .NET runtime, converting data between Polars and .NET.
    /// </summary>
    /// <typeparam name="TInput">Input type (e.g. int, double, string)</typeparam>
    /// <typeparam name="TOutput">Output type (e.g. int, double, string)</typeparam>
    /// <param name="function">The function to apply.</param>
    /// <param name="outputType">The Polars data type of the output column.</param>
    /// <summary>
    /// Apply a custom C# function to the expression (High-Level).
    /// </summary>
    public Expr Map<TInput, TOutput>(Func<TInput, TOutput> function, DataType outputType)
        => new(PolarsWrapper.Map(CloneHandle(), UdfUtils.Wrap(function), outputType.Handle));

    /// <summary>
    /// Apply a raw Arrow-to-Arrow UDF. (Advanced / Internal use)
    /// </summary>
    public Expr Map(Func<IArrowArray, IArrowArray> function, DataType outputType)
        => new(PolarsWrapper.Map(CloneHandle(), function, outputType.Handle));

    // ==========================================
    // Window & Offset
    // ==========================================
    #region Window & Offset Functions

    /// <summary>
    /// Apply a window function over a subgroup.
    /// <para>
    /// This is similar to SQL's `OVER (PARTITION BY ...)` clause.
    /// Unlike <see cref="DataFrame.GroupBy(Expr[])"/>, this does not reduce the number of rows.
    /// The result is broadcasted back to the original rows.
    /// </para>
    /// </summary>
    /// <param name="partitionBy">The columns to partition by.</param>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "A", "B", "B" },
    ///     val = new[] { 10, 20, 30, 100, 200 }
    /// });
    /// 
    /// // Calculate mean per group and subtract it from the value
    /// // The result has the same shape as the original DataFrame (5 rows)
    /// df.Select(
    ///     Col("group"),
    ///     Col("val"),
    ///     Col("val").Mean().Over("group").Alias("group_mean"),
    ///     (Col("val") - Col("val").Mean().Over("group")).Alias("diff_from_mean")
    /// ).Show();
    /// /* Output:
    /// shape: (5, 4)
    /// ┌───────┬─────┬────────────┬────────────────┐
    /// │ group ┆ val ┆ group_mean ┆ diff_from_mean │
    /// │ ---   ┆ --- ┆ ---        ┆ ---            │
    /// │ str   ┆ i32 ┆ f64        ┆ f64            │
    /// ╞═══════╪═════╪════════════╪════════════════╡
    /// │ A     ┆ 10  ┆ 20.0       ┆ -10.0          │
    /// │ A     ┆ 20  ┆ 20.0       ┆ 0.0            │
    /// ...
    /// └───────┴─────┴────────────┴────────────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Over(params Expr[] partitionBy)
    {
        var partitionHandles = System.Array.ConvertAll(partitionBy, e => e.CloneHandle());

        var h = PolarsWrapper.Over(CloneHandle(), partitionHandles);

        return new Expr(h);
    }

    /// <summary>
    /// Window function: Apply aggregation over specific groups.
    /// Example: Col("Amt").Sum().Over("Group", "Date")
    /// </summary>
    public Expr Over(params string[] partitionBy)
    {
        var exprs = System.Array.ConvertAll(partitionBy, Polars.Col);
        return Over(exprs);
    }

    /// <summary>
    /// Shift values by the given number of indices.
    /// Positive values shift downstream, negative values shift upstream.
    /// </summary>
    public Expr Shift(long n = 1) => new(PolarsWrapper.Shift(CloneHandle(), n));

    /// <summary>
    /// Calculate the difference with the previous value (n-th lag).
    /// Null values are propagated.
    /// </summary>
    public Expr Diff(long n = 1) => new(PolarsWrapper.Diff(CloneHandle(), n));

    #endregion

    // ==========================================
    // Rolling Window Functions
    // ==========================================

    /// <summary>
    /// Apply a rolling min (moving min) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(string,int,double[],bool)"/>
    public Expr RollingMin(string windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingMin(CloneHandle(), windowSize, minPeriods, weights,center));
    /// <summary>
    /// Apply a rolling min (moving min) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(TimeSpan,int,double[],bool)"/>
    public Expr RollingMin(TimeSpan windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => RollingMin(DurationFormatter.ToPolarsString(windowSize), minPeriods, weights,center);
    /// <summary>
    /// Apply a rolling max (moving max) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(string,int,double[],bool)"/>
    public Expr RollingMax(string windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingMax(CloneHandle(), windowSize, minPeriods,weights,center));
    /// <summary>
    /// Apply a rolling max (moving max) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(TimeSpan,int,double[],bool)"/>
    public Expr RollingMax(TimeSpan windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => RollingMax(DurationFormatter.ToPolarsString(windowSize), minPeriods,weights,center);
    /// <summary>
    /// Apply a rolling mean (moving average) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new expression representing the rolling mean.</returns>
    /// <example>
    /// <code>
    /// // Rolling mean of 3 rows ("3i"), centered
    /// df.Select(
    ///     Col("val").RollingMean("3i", minPeriods: 1, center: true).Alias("roll_mean")
    /// );
    /// </code>
    /// </example>
    public Expr RollingMean(string windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingMean(CloneHandle(), windowSize, minPeriods,weights,center));
    /// <summary>
    /// Apply a rolling mean (moving average) over a fixed time window defined by a <see cref="TimeSpan"/>.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// 
    /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// 
    /// <param name="weights">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='weights']/node()"/>
    /// </param>
    /// 
    /// <param name="center">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='center']/node()"/>
    /// </param>
    /// 
    /// <returns>
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/returns/node()"/>
    /// </returns>
    public Expr RollingMean(TimeSpan windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => RollingMean(DurationFormatter.ToPolarsString(windowSize), minPeriods, weights,center);
    /// <summary>
    /// Apply a rolling sum (moving sum) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(string,int,double[],bool)"/>
    public Expr RollingSum(string windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingSum(CloneHandle(), windowSize, minPeriods, weights,center));
    /// <summary>
    /// Apply a rolling sum (moving sum) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(TimeSpan,int,double[],bool)"/>
    public Expr RollingSum(TimeSpan windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => RollingSum(DurationFormatter.ToPolarsString(windowSize), minPeriods, weights,center);
    /// <summary>
    /// Apply a rolling standard deviation (moving std) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(string,int,double[],bool)"/>
    public Expr RollingStd(string windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingStd(CloneHandle(), windowSize, minPeriods, weights, center));
    /// <summary>
    /// Apply a rolling standard deviation (moving std) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(TimeSpan,int,double[],bool)"/>
    public Expr RollingStd(TimeSpan windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => RollingStd(DurationFormatter.ToPolarsString(windowSize), minPeriods, weights,center);
    /// <summary>
    /// Apply a rolling variance (moving var) over a window.
    /// </summary>
    /// <param name="ddof">
    /// “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// <para>By default ddof is 1.</para>
    /// </param>
    /// <param name="windowSize">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='windowSize']/node()"/>
    /// </param>
    /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// <param name="weights">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='weights']/node()"/>
    /// </param>
    /// <param name="center">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='center']/node()"/>
    /// </param>
    /// <returns>A new expression representing the rolling variance.</returns>
    public Expr RollingVar(string windowSize, int minPeriods = 1,double[]? weights = null,bool center=false, byte ddof =1)
        => new(PolarsWrapper.RollingVar(CloneHandle(), windowSize, minPeriods,weights,center, ddof));
    /// <summary>
    /// Apply a rolling variance (moving var) over a fixed time window defined by a <see cref="TimeSpan"/>.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="ddof">
    /// <inheritdoc cref="RollingVar(string, int, double[], bool, byte)" path="/param[@name='ddof']/node()"/>
    /// </param>
    /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// <param name="weights">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='weights']/node()"/>
    /// </param>
    /// <param name="center">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='center']/node()"/>
    /// </param>
    /// <returns>A new expression representing the rolling variance.</returns>
    public Expr RollingVar(TimeSpan windowSize, int minPeriods = 1,double[]? weights = null,bool center=false,byte ddof =1)
        => RollingVar(DurationFormatter.ToPolarsString(windowSize), minPeriods,weights,center, ddof);
    /// <summary>
    /// Apply a rolling median (moving median) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(string,int,double[],bool)"/>
    public Expr RollingMedian(string windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingMedian(CloneHandle(), windowSize, minPeriods, weights,center));
    /// <summary>
    /// Apply a rolling median (moving median) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(TimeSpan,int,double[],bool)"/>
    public Expr RollingMedian(TimeSpan windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => RollingMedian(DurationFormatter.ToPolarsString(windowSize), minPeriods,weights,center);
    /// <summary>
    /// Apply a rolling skew (moving skew) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(string,int,double[],bool)"/>
    public Expr RollingSkew(string windowSize, int minPeriods = 1,double[]? weights = null,bool center=false, bool bias=true)
        => new(PolarsWrapper.RollingSkew(CloneHandle(), windowSize, minPeriods, weights,center,bias));
    /// <summary>
    /// Apply a rolling skew (moving skew) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(TimeSpan,int,double[],bool)"/>
    public Expr RollingSkew(TimeSpan windowSize, int minPeriods = 1,double[]? weights = null,bool center=false, bool bias=true)
        => RollingSkew(DurationFormatter.ToPolarsString(windowSize), minPeriods, weights,center,bias);
    /// <summary>
    /// Apply a rolling kurtosis (moving kurtosis) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(string,int,double[],bool)"/>
    public Expr RollingKurtosis(string windowSize, int minPeriods = 1,double[]? weights = null,bool center=false, bool fisher = true,bool bias=true)
        => new(PolarsWrapper.RollingKurtosis(CloneHandle(), windowSize, minPeriods, weights,center,fisher, bias));
    /// <summary>
    /// Apply a rolling kurtosis (moving kurtosis) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(TimeSpan,int,double[],bool)"/>
    public Expr RollingKurtosis(TimeSpan windowSize, int minPeriods = 1,double[]? weights = null,bool center=false, bool fisher = true,bool bias=true)
        => RollingKurtosis(DurationFormatter.ToPolarsString(windowSize), minPeriods, weights,center,fisher,bias);
    /// <summary>
    /// Apply a rolling rank (moving rank) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(string,int,double[],bool)"/>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="seed">If method="random", use this as seed.</param>
    public Expr RollingRank(string windowSize, int minPeriods = 1,RankMethod method=RankMethod.Average, ulong? seed=null,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingRank(CloneHandle(), windowSize, minPeriods,method.ToNative(),seed,weights, center));
    /// <summary>
    /// Apply a rolling rank (moving rank) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(TimeSpan,int,double[],bool)"/>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="seed">If method="random", use this as seed.</param>
    public Expr RollingRank(TimeSpan windowSize,int minPeriods = 1,RankMethod method=RankMethod.Average, ulong? seed=null,double[]? weights = null, bool center= false)
        => RollingRank(DurationFormatter.ToPolarsString(windowSize), minPeriods, method,seed,weights, center);
    /// <summary>
    /// Apply a rolling quantile over a fixed window.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).</param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.</param>
    /// <param name="windowSize">
    /// The size of the window. 
    /// <para>Format: <c>"3i"</c> (3 rows) or just a number string <c>"3"</c>.</para>
    /// <para>For time-based windows (e.g. "2h"), use <see cref="RollingQuantileBy(double,QuantileMethod,string,Expr,int,ClosedWindow)"/> instead.</para>
    /// </param>
    /// <param name="weights">
    /// Optional weights for the window. The length must match the parsed window size.
    /// <para>If <c>null</c>, equal weights are used.</para>
    /// </param>
    /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// <param name="center">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='center']/node()"/>
    /// </param>
    /// <returns>A new expression representing the rolling quantile.</returns>
    public Expr RollingQuantile(
        double quantile, 
        QuantileMethod method, 
        string windowSize, 
        int minPeriods = 1, 
        double[]? weights = null,
        bool center =false)
    {
        return new Expr(PolarsWrapper.RollingQuantile(
            CloneHandle(), 
            quantile,
            method.ToNative(),
            windowSize, 
            minPeriods,
            weights,
            center
        ));
    }
    /// <summary>
    /// Apply a rolling quantile over a fixed time window defined by a <see cref="TimeSpan"/>.
    /// </summary>
    /// 
    /// <param name="quantile">
    /// <inheritdoc cref="RollingQuantile(double, QuantileMethod, string, int, double[], bool)" path="/param[@name='quantile']/node()"/>
    /// </param>
    /// 
    /// <param name="method">
    /// <inheritdoc cref="RollingQuantile(double, QuantileMethod, string, int, double[], bool)" path="/param[@name='method']/node()"/>
    /// </param>
    /// 
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// 
    /// <param name="minPeriods">
    /// <inheritdoc cref="RollingQuantile(double, QuantileMethod, string, int, double[], bool)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// 
    /// <param name="weights">
    /// <inheritdoc cref="RollingQuantile(double, QuantileMethod, string, int, double[], bool)" path="/param[@name='weights']/node()"/>
    /// </param>
    /// 
    /// <param name="center">
    /// <inheritdoc cref="RollingMean(string, int, double[], bool)" path="/param[@name='center']/node()"/>
    /// </param>
    /// 
    /// <returns>
    /// <inheritdoc cref="RollingQuantile(double, QuantileMethod, string, int, double[], bool)" path="/returns/node()"/>
    /// </returns>
    public Expr RollingQuantile(double quantile,QuantileMethod method,TimeSpan windowSize, int minPeriods = 1,double[]? weights= null, bool center=false)
        => RollingQuantile(quantile,method,DurationFormatter.ToPolarsString(windowSize), minPeriods,weights,center);

    /// <summary>
    /// Apply a rolling mean (moving average) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new expression representing the dynamic rolling mean.</returns>
    /// <example>
    /// <code>
    /// // Python: pl.col("index").rolling_mean_by("date", window_size="2h", closed="both")
    /// // C#:
    /// Col("index").RollingMeanBy("2h", Col("date"), closed: ClosedWindow.Both);
    /// </code>
    /// </example>
    public Expr RollingMeanBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return new Expr(PolarsWrapper.RollingMeanBy(
            CloneHandle(),
            windowSize,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply a rolling mean (moving average) over a dynamic window defined by a <see cref="TimeSpan"/>.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the dynamic window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g. <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// 
    /// <param name="by"><inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='by']/node()"/></param>
    /// <param name="minPeriods"><inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='minPeriods']/node()"/></param>
    /// <param name="closed"><inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='closed']/node()"/></param>
    /// 
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/remarks"/>
    /// <returns><inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/returns/node()"/></returns>
    public Expr RollingMeanBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return RollingMeanBy(
            DurationFormatter.ToPolarsString(windowSize),
            by,
            minPeriods,
            closed
        );
    }
    /// <summary>
    /// Apply a rolling sum over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling sum.</returns>
    public Expr RollingSumBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return new Expr(PolarsWrapper.RollingSumBy(
            CloneHandle(),
            windowSize,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply a rolling sum over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(TimeSpan,Expr,int,ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(TimeSpan,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling sum.</returns>
    public Expr RollingSumBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return RollingSumBy(
            DurationFormatter.ToPolarsString(windowSize),
            by,
            minPeriods,
            closed
        );
    }
    /// <summary>
    /// Apply the rolling minimum over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling minimum.</returns>
    public Expr RollingMinBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return new Expr(PolarsWrapper.RollingMinBy(
            CloneHandle(),
            windowSize,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply a rolling minimum over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(TimeSpan,Expr,int,ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(TimeSpan,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling minimum.</returns>
    public Expr RollingMinBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return RollingMinBy(
            DurationFormatter.ToPolarsString(windowSize),
            by,
            minPeriods,
            closed
        );
    }
    /// <summary>
    /// Apply the rolling maximum over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling maximum.</returns>
    public Expr RollingMaxBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return new Expr(PolarsWrapper.RollingMaxBy(
            CloneHandle(),
            windowSize,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply a rolling maximum over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(TimeSpan,Expr,int,ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(TimeSpan,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling maximum.</returns>
    public Expr RollingMaxBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return RollingMaxBy(
            DurationFormatter.ToPolarsString(windowSize),
            by,
            minPeriods,
            closed
        );
    }
    /// <summary>
    /// Apply the rolling standard deviation over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling standard deviation.</returns>
    public Expr RollingStdBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return new Expr(PolarsWrapper.RollingStdBy(
            CloneHandle(),
            windowSize,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply a rolling standard deviation over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(TimeSpan,Expr,int,ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(TimeSpan,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling standard deviation.</returns>
    public Expr RollingStdBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return RollingStdBy(
            DurationFormatter.ToPolarsString(windowSize),
            by,
            minPeriods,
            closed
        );
    }
    /// <summary>
    /// Apply the rolling variance over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// 
    /// /// <param name="windowSize">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='windowSize']/node()"/>
    /// </param>
    /// 
    /// /// <param name="by">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='by']/node()"/>
    /// </param>
    /// 
    /// /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// 
    /// /// <param name="closed">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='closed']/node()"/>
    /// </param>
    /// 
    /// /// <param name="ddof">
    /// “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// <para>By default ddof is 1.</para>
    /// </param>
    /// 
    /// <returns>A new expression representing the dynamic rolling variance.</returns>
    public Expr RollingVarBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left, byte ddof=1)
    {
        return new Expr(PolarsWrapper.RollingVarBy(
            CloneHandle(),
            windowSize,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative(),
            ddof
        ));
    }
    /// <summary>
    /// Apply a rolling variance over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// 
    /// /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// 
    /// /// <param name="by">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='by']/node()"/>
    /// </param>
    /// 
    /// /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// 
    /// /// <param name="closed">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='closed']/node()"/>
    /// </param>
    /// 
    /// /// <param name="ddof">
    /// <inheritdoc cref="RollingVarBy(string, Expr, int, ClosedWindow, byte)" path="/param[@name='ddof']/node()"/>
    /// </param>
    /// 
    /// <returns>A new expression representing the dynamic rolling variance.</returns>
    public Expr RollingVarBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left, byte ddof=1)
    {
        return RollingVarBy(
            DurationFormatter.ToPolarsString(windowSize),
            by,
            minPeriods,
            closed,
            ddof
        );
    }
    /// <summary>
    /// Apply the rolling median over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling median.</returns>
    public Expr RollingMedianBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return new Expr(PolarsWrapper.RollingMedianBy(
            CloneHandle(),
            windowSize,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply a rolling median over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(TimeSpan,Expr,int,ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(TimeSpan,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling median.</returns>
    public Expr RollingMedianBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
    {
        return RollingMedianBy(
            DurationFormatter.ToPolarsString(windowSize),
            by,
            minPeriods,
            closed
        );
    }
    /// <summary>
    /// Compute the rolling rank over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// 
    /// <param name="windowSize">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='windowSize']/node()"/>
    /// </param>
    /// 
    /// <param name="by">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='by']/node()"/>
    /// </param>
    /// 
    /// <param name="method">The method used to assign ranks to tied elements.</param>
    /// 
    /// <param name="seed">Seed for the random method (only relevant when method is Random).</param>
    /// 
    /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// 
    /// <param name="closed">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='closed']/node()"/>
    /// </param>
    /// 
    /// <returns>A new expression representing the dynamic rolling rank.</returns>
    public Expr RollingRankBy(
        string windowSize, 
        Expr by, 
        RollingRankMethod method = RollingRankMethod.Average, 
        ulong? seed = null,
        int minPeriods = 1, 
        ClosedWindow closed = ClosedWindow.Left)
    {
        return new Expr(PolarsWrapper.RollingRankBy(
            CloneHandle(),
            windowSize,
            by.CloneHandle(),
            method.ToNative(),
            seed,
            minPeriods,
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply a rolling rank over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// 
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// 
    /// <param name="by">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='by']/node()"/>
    /// </param>
    /// 
    /// <param name="method">
    /// <inheritdoc cref="RollingRankBy(string, Expr, RollingRankMethod, ulong?, int, ClosedWindow)" path="/param[@name='method']/node()"/>
    /// </param>
    /// 
    /// <param name="seed">
    /// <inheritdoc cref="RollingRankBy(string, Expr, RollingRankMethod, ulong?, int, ClosedWindow)" path="/param[@name='seed']/node()"/>
    /// </param>
    /// 
    /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// 
    /// <param name="closed">
    /// <inheritdoc cref="RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param[@name='closed']/node()"/>
    /// </param>
    /// 
    /// <returns>A new expression representing the dynamic rolling rank.</returns>
    public Expr RollingRankBy(TimeSpan windowSize,       
        Expr by, 
        RollingRankMethod method = RollingRankMethod.Average, 
        ulong? seed = null,
        int minPeriods = 1, 
        ClosedWindow closed = ClosedWindow.Left)
    {
        return RollingRankBy(
            DurationFormatter.ToPolarsString(windowSize),
            by,
            method,
            seed,
            minPeriods,
            closed
        );
    }
    /// <summary>
    /// Compute the rolling quantile over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).</param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.</param>
    ///     
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by"><inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param[@name='by']/node()"/></param>
    /// <param name="minPeriods"><inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param[@name='minPeriods']/node()"/></param>
    /// <param name="closed"><inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param[@name='closed']/node()"/></param>
    ///
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling quantile.</returns>
    public Expr RollingQuantileBy(
        double quantile,
        QuantileMethod method,
        string windowSize,
        Expr by,
        int minPeriods = 1,
        ClosedWindow closed = ClosedWindow.Left)
    {
        return new Expr(PolarsWrapper.RollingQuantileBy(
            CloneHandle(),
            quantile,
            method.ToNative(),
            windowSize,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Compute the rolling quantile over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).</param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.</param>
    ///     
    /// <param name="windowSize"><inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param[@name='windowSize']/node()"/></param>
    /// <param name="by"><inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param[@name='by']/node()"/></param>
    /// <param name="minPeriods"><inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param[@name='minPeriods']/node()"/></param>
    /// <param name="closed"><inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/param[@name='closed']/node()"/></param>
    ///
    /// <inheritdoc cref="RollingMeanBy(string,Expr,int,ClosedWindow)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling quantile.</returns>
    public Expr RollingQuantileBy(
        double quantile,
        QuantileMethod method,
        TimeSpan windowSize,
        Expr by,
        int minPeriods = 1,
        ClosedWindow closed = ClosedWindow.Left)
    {
        return new Expr(PolarsWrapper.RollingQuantileBy(
            CloneHandle(),
            quantile,
            method.ToNative(),
            DurationFormatter.ToPolarsString(windowSize),
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Explode a list expression.
    /// <para>
    /// This turns a list column into a long column (flattening).
    /// </para>
    /// <para>
    /// <b>Warning:</b> When used in <see cref="DataFrame.Select(Expr[])"/> with other columns, 
    /// it may cause a length mismatch error if the other columns are not broadcasted. 
    /// Use <see cref="DataFrame.Explode(string[])"/> for safely exploding columns while repeating others.
    /// </para>
    /// </summary>
    /// <param name="emptyAsNull">
    /// If <c>true</c>, empty lists are exploded into a single <c>null</c> value. 
    /// If <c>false</c>, rows with empty lists are removed from the result.
    /// </param>
    /// <param name="keepNulls">
    /// If <c>true</c>, <c>null</c> values in the column are preserved as <c>null</c> in the result. 
    /// If <c>false</c>, rows with <c>null</c> values are removed.
    /// </param>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2 },
    ///     tags = new[] { new[] { "a", "b" }, new[] { "c" } }
    /// });
    /// 
    /// // Example 1: Expression Explode (Flatten single column)
    /// df.Select(
    ///     Col("tags").Explode().Alias("tags_flat")
    /// ).Show();
    /// /* Output:
    /// shape: (3, 1)
    /// ┌───────────┐
    /// │ tags_flat │
    /// │ ---       │
    /// │ str       │
    /// ╞═══════════╡
    /// │ a         │
    /// │ b         │
    /// │ c         │
    /// └───────────┘
    /// */
    /// 
    /// // Example 2: To keep 'id' aligned, use DataFrame.Explode:
    /// // df.Explode("tags").Show();
    /// </code>
    /// </example>
    public Expr Explode(bool emptyAsNull=true,bool keepNulls=true) => new(PolarsWrapper.Explode(CloneHandle(),emptyAsNull,keepNulls));
    /// <summary>
    /// Aggregate values into a list.
    /// <para>
    /// This is the opposite of <see cref="Explode"/>. It collects values from multiple rows into a single list.
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// // Collect all IDs and Tags into single lists
    /// df.Select(
    ///     Col("id").Implode().Alias("all_ids"), 
    ///     Col("tags").Implode().Alias("nested_tags")
    /// ).Show();
    /// /* Output:
    /// shape: (1, 2)
    /// ┌───────────┬─────────────────────┐
    /// │ all_ids   ┆ nested_tags         │
    /// │ ---       ┆ ---                 │
    /// │ list[i32] ┆ list[list[str]]     │
    /// ╞═══════════╪═════════════════════╡
    /// │ [1, 2]    ┆ [["a", "b"], ["c"]] │
    /// └───────────┴─────────────────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Implode() => new(PolarsWrapper.Implode(CloneHandle()));
    // ==========================================
    // Namespaces
    // ==========================================

    /// <summary>
    /// Access temporal (Date/Time) operations.
    /// </summary>
    public DtOps Dt => new(this);

    /// <summary>
    /// Access string manipulation operations.
    /// </summary>
    public StringOps Str => new(this);

    /// <summary>
    /// Access list operations.
    /// </summary>
    public ListOps List => new(this);

    /// <summary>
    /// Access struct operations.
    /// </summary>
    public StructOps Struct => new(this);

    /// <summary>
    /// Access column renaming operations.
    /// </summary>
    public NameOps Name => new(this);

    public ArrayOps Array => new(this);

    // ---------------------------------------------------
    // Clean Up
    // ---------------------------------------------------
    /// <summary>
    /// Dispose a handle.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
        GC.SuppressFinalize(this); 
    }

    /// <summary>
    /// Decide whether two Expr objects are the same instance
    /// </summary>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj)) return true;

        if (obj is not Expr other) return false;

        if (Handle.IsInvalid || other.Handle.IsInvalid) return false;

        return Handle.DangerousGetHandle() == other.Handle.DangerousGetHandle();
    }

    /// <summary>
    /// Get hashcode based on handles
    /// </summary>
    public override int GetHashCode()
    {
        if (Handle.IsInvalid) return 0;

        return Handle.DangerousGetHandle().GetHashCode();
    }
}
// ==========================================
// DtOps Helper Class
// ==========================================

/// <summary>
/// Contains methods for temporal (Date/Time) operations.
/// Access this via <see cref="Expr.Dt"/>.
/// </summary>
public class DtOps
{
    private readonly Expr _expr;

    internal DtOps(Expr expr)
    {
        _expr = expr;
    }

    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(op(h));
    }

    /// <summary>Get the year from the underlying date/datetime.</summary>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     dt = new[] { new DateTime(2024, 1, 1, 10, 15, 30) } 
    /// });
    /// 
    /// df.Select(
    ///     Col("dt").Dt.Year(),    // 2024
    ///     Col("dt").Dt.Month(),   // 1
    ///     Col("dt").Dt.Weekday(), // 1 (Monday)
    ///     Col("dt").Dt.ToString("%Y-%m-%d") // "2024-01-01"
    /// ).Show();
    /// </code>
    /// </example>
    public Expr Year() => Wrap(PolarsWrapper.DtYear);

    /// <summary>Get the quarter from the underlying date/datetime.</summary>
    public Expr Quarter() => Wrap(PolarsWrapper.DtQuarter);

    /// <summary>Get the month from the underlying date/datetime.</summary>
    public Expr Month() => Wrap(PolarsWrapper.DtMonth);

    /// <summary>Get the day from the underlying date/datetime.</summary>
    public Expr Day() => Wrap(PolarsWrapper.DtDay);

    /// <summary>Get the ordinal day (day of year) from the underlying date/datetime.</summary>
    public Expr OrdinalDay() => Wrap(PolarsWrapper.DtOrdinalDay);

    /// <summary>Get the weekday from the underlying date/datetime.</summary>
    public Expr Weekday() => Wrap(PolarsWrapper.DtWeekday);

    /// <summary>Get the hour from the underlying datetime.</summary>
    public Expr Hour() => Wrap(PolarsWrapper.DtHour);

    /// <summary>Get the minute from the underlying datetime.</summary>
    public Expr Minute() => Wrap(PolarsWrapper.DtMinute);

    /// <summary>Get the second from the underlying datetime.</summary>
    public Expr Second() => Wrap(PolarsWrapper.DtSecond);

    /// <summary>Get the millisecond from the underlying datetime.</summary>
    public Expr Millisecond() => Wrap(PolarsWrapper.DtMillisecond);

    /// <summary>Get the microsecond from the underlying datetime.</summary>
    public Expr Microsecond() => Wrap(PolarsWrapper.DtMicrosecond);

    /// <summary>Get the nanosecond from the underlying datetime.</summary>
    public Expr Nanosecond() => Wrap(PolarsWrapper.DtNanosecond);

    /// <summary>
    /// Format the date/datetime as a string using the given format string.
    /// <para>Format codes follow the Rust `chrono` crate syntax (similar to strftime).</para>
    /// </summary>
    public Expr ToString(string format)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtToString(h, format));
    }
    /// <summary>
    /// Alias for <see cref="ToString(string)"/>.
    /// </summary>
    public Expr Strftime(string format)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtToString(h, format));
    }

    /// <summary>
    /// Format the date/datetime as a string using the default format "%Y-%m-%dT%H:%M:%S%.f".
    /// </summary>
    public override string ToString() => "DtOps";
    /// <summary>
    /// Cast to Date (remove time component).
    /// </summary>
    /// <returns></returns>
    public Expr Date() => Wrap(PolarsWrapper.DtDate);
    /// <summary>
    /// Cast to Time (remove Date component).
    /// </summary>
    /// <returns></returns>
    public Expr Time() => Wrap(PolarsWrapper.DtTime);

    // ==========================================
    // Truncate & Round
    // ==========================================

    /// <summary>       
    /// Truncate the datetimes to the given interval (e.g. "1d", "1h", "15m").
    /// <para>This behaves like a "floor" operation for time.</para>
    /// </summary>
    /// <example>
    /// <code>
    /// // Input: 2024-02-29 23:59:59
    /// df.Select(
    ///     Col("dt").Dt.Truncate("1h"), // Result: 23:00:00
    ///     Col("dt").Dt.Round("1h")     // Result: 2024-03-01 00:00:00
    /// );
    /// </code>
    /// </example>
    public Expr Truncate(string every)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtTruncate(h, every));
    }
    /// <summary>
    /// Truncate the datetimes to the given timespan
    /// </summary>
    /// <param name="every"></param>
    /// <returns></returns>
    public Expr Truncate(TimeSpan every)
        => Truncate(DurationFormatter.ToPolarsString(every));
    /// <summary>
    /// Round the datetimes to the given interval.
    /// </summary>
    public Expr Round(string every)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtRound(h, every));
    }
    /// <summary>
    /// Round the datetimes to the given timespan interval.
    /// </summary>
    /// <param name="every"></param>
    /// <returns></returns>
    public Expr Round(TimeSpan every)
        => Round(DurationFormatter.ToPolarsString(every));
    // ==========================================
    // Offset
    // ==========================================

    /// <summary>
    /// Offset the datetimes by a given duration expression.
    /// </summary>
    public Expr OffsetBy(Expr by) => new(PolarsWrapper.DtOffsetBy(_expr.Handle, by.Handle));
    /// <summary>
    /// Offset the datetimes by a constant duration string (e.g., "1d", "-2h").
    /// </summary>
    /// <example>
    /// <code>
    /// df.Select(
    ///     // Add 2 days
    ///     Col("dt").Dt.OffsetBy("2d"),
    ///     // Subtract 1 hour
    ///     Col("dt").Dt.OffsetBy("-1h")
    /// );
    /// </code>
    /// </example>
    public Expr OffsetBy(string duration) => OffsetBy(Polars.Lit(duration));
    /// <summary>
    /// Offset the datetimes by TimeSpan
    /// </summary>
    /// <param name="duration"></param>
    /// <returns></returns>
    public Expr OffsetBy(TimeSpan duration)
    {
        string durationStr = DurationFormatter.ToPolarsString(duration);
        return OffsetBy(Polars.Lit(durationStr));
    }

    // ==========================================
    // Timestamp
    // ==========================================

    /// <summary>
    /// Convert the datetime to an integer timestamp (Unix epoch).
    /// </summary>
    /// <param name="timeUnit">
    /// The desired TimeUnit for the resulting Datetime.
    /// <para><b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.</para>
    /// </param>
    public Expr Timestamp(TimeUnit timeUnit = TimeUnit.Microseconds)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtTimestamp(h, timeUnit.ToNative()));
    }
    /// <summary>
    /// Combine the date from the underlying date/datetime with the time from another expression.
    /// <para>The resulting Series will have the specified TimeUnit.</para>
    /// </summary>
    /// <param name="time">An expression yielding the Time component.</param>
    /// <param name="timeUnit">
    /// The desired TimeUnit for the resulting Datetime.
    /// <para><b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.</para>
    /// </param>
    public Expr Combine(Expr time, TimeUnit timeUnit = TimeUnit.Microseconds)
    {

        var hExpr = PolarsWrapper.CloneExpr(_expr.Handle);
        var hTime = PolarsWrapper.CloneExpr(time.Handle);
        
        return new Expr(PolarsWrapper.DtCombine(hExpr, hTime, timeUnit.ToNative()));
    }
    // ==========================================
    // TimeZone
    // ==========================================
    /// <summary>
    /// Convert from one timezone to another.
    /// <para>
    /// This changes the physical time (wall clock time) to match the target timezone.
    /// The input Series must already have a timezone assigned (e.g. via <see cref="ReplaceTimeZone"/>).
    /// </para>
    /// </summary>
    /// <param name="timeZone">Target time zone string (IANA database, e.g. "Asia/Shanghai", "America/New_York").</param>
    /// <returns>A new expression with the converted time.</returns>
    /// <example>
    /// <code>
    /// // 1. Start with a naive datetime (noon)
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     dt = new[] { new DateTime(2024, 1, 1, 12, 0, 0) } 
    /// });
    /// 
    /// // 2. Operations:
    /// // - ReplaceTimeZone("UTC"): Define metadata (it is now UTC noon)
    /// // - ConvertTimeZone("Asia/Shanghai"): Shift to +8 hours (20:00)
    /// // - ConvertTimeZone("America/New_York"): Shift to -5 hours (07:00)
    /// df.Select(
    ///     Col("dt").Alias("naive"),
    ///     
    ///     Col("dt").Dt.ReplaceTimeZone("UTC").Alias("utc_tagged"),
    ///     
    ///     Col("dt").Dt.ReplaceTimeZone("UTC")
    ///               .Dt.ConvertTimeZone("Asia/Shanghai")
    ///               .Alias("shanghai_time"),
    /// 
    ///     Col("dt").Dt.ReplaceTimeZone("UTC")
    ///               .Dt.ConvertTimeZone("America/New_York")
    ///               .Alias("ny_time")
    /// ).Show();
    /// /* Output:
    /// shape: (1, 4)
    /// ┌─────────────────────┬─────────────────────────┬─────────────────────────────┬────────────────────────────────┐
    /// │ naive               ┆ utc_tagged              ┆ shanghai_time               ┆ ny_time                        │
    /// │ ---                 ┆ ---                     ┆ ---                         ┆ ---                            │
    /// │ datetime[μs]        ┆ datetime[μs, UTC]       ┆ datetime[μs, Asia/Shanghai] ┆ datetime[μs, America/New_York] │
    /// ╞═════════════════════╪═════════════════════════╪═════════════════════════════╪════════════════════════════════╡
    /// │ 2024-01-01 12:00:00 ┆ 2024-01-01 12:00:00 UTC ┆ 2024-01-01 20:00:00 CST     ┆ 2024-01-01 07:00:00 EST        │
    /// └─────────────────────┴─────────────────────────┴─────────────────────────────┴────────────────────────────────┘
    /// */
    /// </code>
    /// </example>
    public Expr ConvertTimeZone(string timeZone)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtConvertTimeZone(h, timeZone));
    }
    /// <summary>
    /// Replace the time zone of a Series.
    /// <para>
    /// This sets the time zone metadata without changing the underlying physical time (wall clock).
    /// Use this to assign a timezone to a naive datetime.
    /// </para>
    /// </summary>
    /// <param name="timeZone">The time zone to assign (e.g. "UTC", "Asia/Shanghai"). If null, removes timezone info.</param>
    /// <param name="ambiguous">How to handle ambiguous times (e.g. DST transitions). Default "raise".</param>
    /// <param name="nonExistent">How to handle non-existent times. Default "raise".</param>
    /// <seealso cref="ConvertTimeZone(string)"/>
    public Expr ReplaceTimeZone(string? timeZone, string? ambiguous = null, string? nonExistent = "raise")
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtReplaceTimeZone(h, timeZone, ambiguous, nonExistent));
    }
    // ==========================================
    // BusinessDays
    // ==========================================
    private static readonly bool[] DefaultWeekMask = [true, true, true, true, true, false, false];

    /// <summary>
    /// Add business days to the date column.
    /// <para>
    /// Automatically skips weekends (by default) and specified holidays.
    /// </para>
    /// </summary>
    /// <param name="n">Number of business days to add (can be negative).</param>
    /// <param name="holidays">List of holidays to skip.</param>
    /// <param name="weekMask">
    /// Array of 7 bools indicating business days, starting from Monday. 
    /// Default is Mon-Fri [true, true, true, true, true, false, false].
    /// </param>
    /// <param name="roll">Strategy for handling non-business days.</param>
    /// <example>
    /// <code>
    /// // Add 1 business day (skipping weekends)
    /// // Fri 2024-03-01 -> Mon 2024-03-04
    /// df.Select(
    ///     Col("dt").Dt.AddBusinessDays(1)
    /// );
    /// </code>
    /// </example>
    public Expr AddBusinessDays(
        int n,
        IEnumerable<DateOnly>? holidays = null,
        bool[]? weekMask = null,
        Roll roll = Roll.Raise)
    {
        return AddBusinessDays(Polars.Lit(n), holidays, weekMask, roll);
    }

    /// <summary>
    /// Add business days to the date column.
    /// </summary>
    public Expr AddBusinessDays(
        Expr n,
        IEnumerable<DateOnly>? holidays = null,
        bool[]? weekMask = null,
        Roll roll = Roll.Raise)
    {
        var mask = weekMask ?? DefaultWeekMask;

        int[] holidayInts;
        if (holidays == null)
        {
            holidayInts = [];
        }
        else
        {
            // Polars using 1970-01-01 as 0
            // DateOnly.DayNumber is days from 0001-01-01
            // 1970-01-01 DayNumber is 719162
            const int EpochDayNumber = 719162;
            holidayInts = [.. holidays.Select(d => d.DayNumber - EpochDayNumber)];
        }

        var nHandle = PolarsWrapper.CloneExpr(n.Handle);
        var handle = PolarsWrapper.CloneExpr(_expr.Handle);

        return new Expr(PolarsWrapper.DtAddBusinessDays(
            handle,
            nHandle,
            mask,
            holidayInts,
            roll.ToNative()
        ));
    }

    /// <summary>
    /// Check if the date is a business day.
    /// </summary>
    public Expr IsBusinessDay(IEnumerable<DateOnly>? holidays = null, bool[]? weekMask = null)
    {
        var mask = weekMask ?? DefaultWeekMask;

        int[] holidayInts;
        if (holidays == null)
        {
            holidayInts = [];
        }
        else
        {
            const int EpochDayNumber = 719162;
            holidayInts = [.. holidays.Select(d => d.DayNumber - EpochDayNumber)];
        }
        var handle = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtIsBusinessDay(handle, mask, holidayInts));
    }
}

// ==========================================
// StringOps Helper Class
// ==========================================
/// <summary>
/// Offers multiple methods for checking, parsing, and transforming string columns.
/// Access this via <see cref="Expr.Str"/>.
/// </summary>
public class StringOps
{
    private readonly Expr _expr;
    internal StringOps(Expr expr) { _expr = expr; }

    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(op(h));
    }
    /// <summary>
    /// Convert string to uppercase.
    /// </summary>
    /// <example>
    /// <code>
    /// df.Select(Col("text").Str.ToUpper());
    /// </code>
    /// </example>
    public Expr ToUpper() => Wrap(PolarsWrapper.StrToUpper);
    /// <summary>
    /// Convert String to LowerClass.
    /// </summary>
    public Expr ToLower() => Wrap(PolarsWrapper.StrToLower);
    /// <summary>
    /// Get length in bytes.
    /// <para>Note: Multi-byte characters (like emojis or CJK) count as > 1 byte.</para>
    /// </summary>
    public Expr Len() => Wrap(PolarsWrapper.StrLenBytes);
    /// <summary>
    /// Slice the string by offset and length.
    /// </summary>
    /// <param name="offset">Start index.</param>
    /// <param name="length">Length of the slice.</param>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     text = new[] { "Polars", "  Data  ", "Rust", null, "123-abc" }
    /// });
    /// 
    /// // 1. ToUpper
    /// // 2. Len (bytes)
    /// // 3. Slice (offset=1, len=3)
    /// df.Select(
    ///     Col("text"),
    ///     Col("text").Str.ToUpper().Alias("upper"),
    ///     Col("text").Str.Len().Alias("len_bytes"), 
    ///     Col("text").Str.Slice(1, 3).Alias("sliced") 
    /// ).Show();
    /// /* Output:
    /// shape: (5, 4)
    /// ┌──────────┬──────────┬───────────┬────────┐
    /// │ text     ┆ upper    ┆ len_bytes ┆ sliced │
    /// │ ---      ┆ ---      ┆ ---       ┆ ---    │
    /// │ str      ┆ str      ┆ u32       ┆ str    │
    /// ╞══════════╪══════════╪═══════════╪════════╡
    /// │ Polars   ┆ POLARS   ┆ 6         ┆ ola    │
    /// │   Data   ┆   DATA   ┆ 8         ┆  Da    │
    /// │ Rust     ┆ RUST     ┆ 4         ┆ ust    │
    /// │ null     ┆ null     ┆ null      ┆ null   │
    /// │ 123-abc  ┆ 123-ABC  ┆ 7         ┆ 23-    │
    /// └──────────┴──────────┴───────────┴────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Slice(long offset, ulong length)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrSlice(h, offset, length));
    }
    /// <summary>
    /// Replace all occurrences of a pattern with a value.
    /// </summary>
    /// <param name="pattern">The pattern to search for.</param>
    /// <param name="value">The value to replace with.</param>
    /// <param name="useRegex">Whether to interpret the pattern as a Regex.</param>
    public Expr ReplaceAll(string pattern, string value, bool useRegex = false)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrReplaceAll(h, pattern, value, useRegex));
    }
    /// <summary>
    /// Extract the first match of a regex pattern.
    /// </summary>
    /// <param name="pattern">Regex pattern with capture groups.</param>
    /// <param name="groupIndex">The index of the capture group to extract (usually 1).</param>
    public Expr Extract(string pattern, uint groupIndex)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrExtract(h, pattern, groupIndex));
    }
    /// <summary>
    /// Check if the string contains a substring or regex pattern.
    /// </summary>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     code = new[] { "ID-001", "ID-002", "ID-999", "XX-000", "ID-ABC" },
    ///     text = new[] { "Polars", "Data", "Rust", null, "123" }
    /// });
    /// 
    /// df.Select(
    ///     Col("code"),
    ///     // Replace "-" with "_"
    ///     Col("code").Str.ReplaceAll("-", "_").Alias("replaced"),
    ///     // Extract numbers using Regex group 1
    ///     Col("code").Str.Extract(@"(\d+)", 1).Alias("extracted_num"),
    ///     // Check if text contains "a"
    ///     Col("text").Str.Contains("a").Alias("has_a")
    /// ).Show();
    /// /* Output:
    /// shape: (5, 4)
    /// ┌────────┬──────────┬───────────────┬───────┐
    /// │ code   ┆ replaced ┆ extracted_num ┆ has_a │
    /// │ ---    ┆ ---      ┆ ---           ┆ ---   │
    /// │ str    ┆ str      ┆ str           ┆ bool  │
    /// ╞════════╪══════════╪═══════════════╪═══════╡
    /// │ ID-001 ┆ ID_001   ┆ 001           ┆ true  │
    /// │ ID-002 ┆ ID_002   ┆ 002           ┆ true  │
    /// │ ID-999 ┆ ID_999   ┆ 999           ┆ false │
    /// │ XX-000 ┆ XX_000   ┆ 000           ┆ null  │
    /// │ ID-ABC ┆ ID_ABC   ┆ null          ┆ false │
    /// └────────┴──────────┴───────────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public Expr Contains(string pattern)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrContains(h, pattern));
    }
    /// <summary>
    /// Split the string by a separator. Returns a List&lt;String&gt;.
    /// </summary>
    public Expr Split(string separator)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrSplit(h, separator));
    }
    // ==========================================
    // Strip / Clean
    // ==========================================
    /// <summary>
    /// Remove leading and trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    /// <param name="matches">The set of characters to be removed.</param>
    /// <example>
    /// <code>
    /// df.Select(
    ///     // "  Data  " -> "Data"
    ///     Col("text").Str.StripChars().Alias("trimmed"),
    ///     // Remove "ID-" prefix
    ///     Col("code").Str.StripPrefix("ID-").Alias("no_prefix")
    /// );
    /// </code>
    /// </example>
    public Expr StripChars(string? matches = null)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrStripChars(h, matches));
    }

    /// <summary>
    /// Remove leading characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    public Expr StripCharsStart(string? matches = null)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrStripCharsStart(h, matches));
    }

    /// <summary>
    /// Remove trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    public Expr StripCharsEnd(string? matches = null)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrStripCharsEnd(h, matches));
    }

    /// <summary>
    /// Remove a specific prefix string.
    /// </summary>
    public Expr StripPrefix(string prefix)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrStripPrefix(h, prefix));
    }

    /// <summary>
    /// Remove a specific suffix string.
    /// </summary>
    public Expr StripSuffix(string suffix)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrStripSuffix(h, suffix));
    }

    // ==========================================
    // Boolean Checks (检查)
    // ==========================================

    /// <summary>
    /// Check if the string starts with the given prefix.
    /// </summary>
    public Expr StartsWith(string prefix)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrStartsWith(h, prefix));
    }

    /// <summary>
    /// Check if the string ends with the given suffix.
    /// </summary>
    public Expr EndsWith(string suffix)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrEndsWith(h, suffix));
    }

    // ==========================================
    // Temporal Parsing
    // ==========================================

    /// <summary>       
    /// Convert string to Date using the specified format.
    /// <para>
    /// Strict parsing: Format mismatches result in null.
    /// Format string is similar to `strftime` (e.g. "%Y-%m-%d").
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// var dateDf = DataFrame.FromColumns(new { 
    ///     raw = new[] { "2024-01-01", "2024/02/01", "invalid" } 
    /// });
    /// 
    /// dateDf.Select(
    ///     Col("raw"),
    ///     Col("raw").Str.ToDate("%Y-%m-%d").Alias("fmt_dash"), 
    ///     Col("raw").Str.ToDate("%Y/%m/%d").Alias("fmt_slash")
    /// ).Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌────────────┬────────────┬────────────┐
    /// │ raw        ┆ fmt_dash   ┆ fmt_slash  │
    /// │ ---        ┆ ---        ┆ ---        │
    /// │ str        ┆ date       ┆ date       │
    /// ╞════════════╪════════════╪════════════╡
    /// │ 2024-01-01 ┆ 2024-01-01 ┆ null       │
    /// │ 2024/02/01 ┆ null       ┆ 2024-02-01 │
    /// │ invalid    ┆ null       ┆ null       │
    /// └────────────┴────────────┴────────────┘
    /// */
    /// </code>
    /// </example>
    public Expr ToDate(string format)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrToDate(h, format));
    }

    /// <summary>
    /// Convert string to Datetime using the specified format.
    /// </summary>
    public Expr ToDatetime(string format)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrToDatetime(h, format));
    }
}

// ==========================================
// ListOps Helper Class
// ==========================================
/// <summary>
/// Operations on List columns. Access via <see cref="Expr.List"/>.
/// </summary>
public class ListOps
{
    private readonly Expr _expr;
    internal ListOps(Expr expr) { _expr = expr; }

    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(op(h));
    }
    /// <summary>
    /// Get the first element of the list.
    /// </summary>
    /// <returns></returns>
    public Expr First() => Wrap(PolarsWrapper.ListFirst);
    /// <summary>
    /// Get the value at a specific index.
    /// </summary>
    /// <param name="index">The index to retrieve (can be negative for reverse indexing).</param>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     student = new[] { "Alice", "Bob", "Charlie" },
    ///     scores = new[] { 
    ///         new[] { 100, 90, 80 },
    ///         new[] { 60, 60 },
    ///         new int[] { }
    ///     }
    /// });
    /// 
    /// df.Select(
    ///     Col("student"),
    ///     Col("scores").List.Len().Alias("course_count"),
    ///     Col("scores").List.Sum().Alias("total_score"),
    ///     Col("scores").List.Get(0).Alias("first_score")
    /// ).Show();
    /// /* Output:
    /// shape: (3, 4)
    /// ┌─────────┬──────────────┬─────────────┬─────────────┐
    /// │ student ┆ course_count ┆ total_score ┆ first_score │
    /// │ ---     ┆ ---          ┆ ---         ┆ ---         │
    /// │ str     ┆ u32          ┆ i32         ┆ i32         │
    /// ╞═════════╪══════════════╪═════════════╪═════════════╡
    /// │ Alice   ┆ 3            ┆ 270         ┆ 100         │
    /// │ Bob     ┆ 2            ┆ 120         ┆ 60          │
    /// │ Charlie ┆ 0            ┆ 0           ┆ null        │
    /// └─────────┴──────────────┴─────────────┴─────────────┘
    /// */
    /// 
    /// // To Explode (Flatten) the list, use DataFrame.Explode:
    /// // df.Explode(Col("scores")).Show();
    /// </code>
    /// </example>
    public Expr Get(int index)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.ListGet(h, index));
    }
    /// <summary>
    /// Get the length of the lists.
    /// </summary>
    /// <example>
    /// <code>
    /// // Input: [100, 90, 80]
    /// df.Select(Col("scores").List.Len()); // Output: 3
    /// </code>
    /// </example>
    public Expr Len() => Wrap(PolarsWrapper.ListLen);
    /// <summary>
    /// Join the list elements into a single string with a separator.
    /// </summary>
    /// <param name="separator"></param>
    /// <returns></returns>
    public Expr Join(string separator)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.ListJoin(h, separator));
    }
    /// <summary>
    /// Sort the list elements.
    /// </summary>
    /// <param name="descending"></param>
    /// <param name="nullsLast"></param>
    /// <param name="maintainOrder"></param>
    /// <returns></returns>
    public Expr Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.ListSort(h, descending, nullsLast, maintainOrder));
    }
    /// <summary>
    /// Calculate the sum of the values in the list (row-wise).
    /// </summary>
    /// <example>
    /// <code>
    /// // Input: [100, 90, 80]
    /// df.Select(Col("scores").List.Sum()); // Output: 270
    /// </code>
    /// </example>
    public Expr Sum() => Wrap(PolarsWrapper.ListSum);
    /// <summary>
    /// Calculate the minimum of the list elements.
    /// </summary>
    /// <returns></returns>
    public Expr Min() => Wrap(PolarsWrapper.ListMin);
    /// <summary>
    /// Calculate the maximum of the list elements.
    /// </summary>
    /// <returns></returns>
    public Expr Max() => Wrap(PolarsWrapper.ListMax);
    /// <summary>
    /// Calculate the mean of the list elements.
    /// </summary>
    /// <returns></returns>
    public Expr Mean() => Wrap(PolarsWrapper.ListMean);
    /// <summary>
    /// Check if the list contains a specific item.
    /// </summary>
    /// <param name="item"></param>
    /// <param name="nullsEqual"></param>
    /// <returns></returns>
    public Expr Contains(Expr item, bool nullsEqual=false)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        var i = PolarsWrapper.CloneExpr(item.Handle);
        return new Expr(PolarsWrapper.ListContains(h, i,nullsEqual));
    }
    /// <summary>
    /// Check if the list contains a specific integer or string item.
    /// </summary>
    /// <param name="item"></param>
    /// <param name="nullsEqual"></param>
    /// <returns></returns>
    public Expr Contains(int item, bool nullsEqual=false) => Contains(Polars.Lit(item), nullsEqual);
    /// <summary>
    /// Check if the list contains a specific string item.
    /// </summary>
    /// <param name="item"></param>
    /// <param name="nullsEqual"></param>
    /// <returns></returns>
    public Expr Contains(string item, bool nullsEqual=false) => Contains(Polars.Lit(item), nullsEqual);
    /// <summary>
    /// Concat this list expression with other list expressions.
    /// </summary>
    /// <param name="others">Other list expressions to append.</param>
    public Expr Concat(params Expr[] others)
    {
        var allExprs = new ExprHandle[others.Length + 1];

        allExprs[0] = PolarsWrapper.CloneExpr(_expr.Handle);

        for (int i = 0; i < others.Length; i++)
        {
            allExprs[i + 1] = PolarsWrapper.CloneExpr(others[i].Handle);
        }

        return new Expr(PolarsWrapper.ConcatList(allExprs));
    }

    public Expr Concat(Expr other) => Concat([other]);
    public Expr Reverse() => Wrap(PolarsWrapper.ListReverse);
}
// ==========================================
// ArrayOps Helper Class
// ==========================================
/// <summary>
/// Offers methods for accessing fields within array columns.
/// </summary>
public class ArrayOps
{
    private readonly Expr _expr;
    internal ArrayOps(Expr expr) { _expr = expr; }

    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(op(h));
    }
    // --- Aggregations ---
    public Expr Max() => Wrap(PolarsWrapper.ArrayMax);
    public Expr Min() => Wrap(PolarsWrapper.ArrayMin);
    public Expr Sum() => Wrap(PolarsWrapper.ArraySum);
    public Expr Mean() => Wrap(PolarsWrapper.ArrayMean); 
    public Expr Median() => Wrap(PolarsWrapper.ArrayMedian); 
    public Expr Std(byte ddof = 1) => new(PolarsWrapper.ArrayStd(PolarsWrapper.CloneExpr(_expr.Handle), ddof)); 
    public Expr Var(byte ddof = 1) => new(PolarsWrapper.ArrayVar(PolarsWrapper.CloneExpr(_expr.Handle), ddof)); 

    // --- Boolean ---
    public Expr Any() => Wrap(PolarsWrapper.ArrayAny); 
    public Expr All() => Wrap(PolarsWrapper.ArrayAll); 

    // --- Sort & Search ---
    public Expr Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.ArraySort(h, descending, nullsLast, maintainOrder));
    }
    public Expr Reverse() => Wrap(PolarsWrapper.ArrayReverse); 
    public Expr ArgMin() => Wrap(PolarsWrapper.ArrayArgMin); 
    public Expr ArgMax() => Wrap(PolarsWrapper.ArrayArgMax); 

    // --- Structure ---
    public Expr Get(int index, bool nullOnOob = true) => Get(Polars.Lit(index), nullOnOob);
    public Expr Get(Expr index, bool nullOnOob = true)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        var idx = PolarsWrapper.CloneExpr(index.Handle);
        return new Expr(PolarsWrapper.ArrayGet(h, idx, nullOnOob));
    }

    public Expr Join(string separator, bool ignoreNulls = true)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.ArrayJoin(h, separator, ignoreNulls));
    }

    public Expr Explode(bool emptyAsNull=true,bool keepNulls = true)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.ArrayExplode(h,emptyAsNull,keepNulls)); 
    }

    /// <summary>
    /// Convert array to struct. Fields will be named field_0, field_1, etc.
    /// </summary>
    public Expr ToStruct() => Wrap(PolarsWrapper.ArrayToStruct); 

    public Expr ToList() => Wrap(PolarsWrapper.ArrayToList);

    // [Update] Updated Contains signature
    public Expr Contains(Expr item, bool nullsEqual = false)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        var i = PolarsWrapper.CloneExpr(item.Handle);
        return new Expr(PolarsWrapper.ArrayContains(h, i, nullsEqual));
    }
    public Expr Contains(int item, bool nullsEqual = false) => Contains(Polars.Lit(item), nullsEqual);
    public Expr Contains(double item, bool nullsEqual = false) => Contains(Polars.Lit(item), nullsEqual);

    // Unique
    public Expr Unique(bool stable = false)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.ArrayUnique(h, stable));
    }
    /// <summary>
    /// Concat this Array expression with other Array expressions.
    /// </summary>
    /// <param name="others">Other list expressions to append.</param>
    public Expr Concat(params Expr[] others)
    {
        var allExprs = new ExprHandle[others.Length + 1];

        allExprs[0] = PolarsWrapper.CloneExpr(_expr.Handle);

        for (int i = 0; i < others.Length; i++)
        {
            allExprs[i + 1] = PolarsWrapper.CloneExpr(others[i].Handle);
        }

        return new Expr(PolarsWrapper.ConcatArray(allExprs));
    }
}

// ==========================================
// StructOps Helper Class
// ==========================================
/// <summary>
/// Operations on Struct columns. Access via <see cref="Expr.Struct"/>.
/// </summary>
public class StructOps
{
    private readonly Expr _expr;
    internal StructOps(Expr expr) { _expr = expr; }

    /// <summary>
    /// Retrieve a field from the struct by name.
    /// </summary>
    /// <param name="name">The name of the field.</param>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2 },
    ///     product = new[] 
    ///     { 
    ///         new { Name = "Laptop", Specs = new { Ram = 16, SSD = 512 } },
    ///         new { Name = "Mouse",  Specs = new { Ram = 0,  SSD = 0   } }
    ///     }
    /// });
    /// 
    /// df.Select(
    ///     Col("id"),
    ///     Col("product").Struct.Field("Name").Alias("prod_name"),
    ///     // Nested Access
    ///     Col("product").Struct.Field("Specs").Struct.Field("Ram").Alias("ram_gb")
    /// ).Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬───────────┬────────┐
    /// │ id  ┆ prod_name ┆ ram_gb │
    /// │ --- ┆ ---       ┆ ---    │
    /// │ i32 ┆ str       ┆ i32    │
    /// ╞═════╪═══════════╪════════╡
    /// │ 1   ┆ Laptop    ┆ 16     │
    /// │ 2   ┆ Mouse     ┆ 0      │
    /// └─────┴───────────┴────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Field(string name)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StructFieldByName(h, name));
    }
    /// <summary>
    /// Retrieve a field by its index.
    /// </summary>
    public Expr Field(int index)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StructFieldByIndex(h, index));
    }
    /// <summary>
    /// Rename the fields of the struct.
    /// </summary>
    /// <param name="names">The new names for the fields.</param>
    /// <example>
    /// <code>
    /// df.Select(
    ///     Col("product").Struct.RenameFields(new[] { "NewName", "NewSpecs" })
    /// );
    /// </code>
    /// </example>
    public Expr RenameFields(params string[] names)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StructRenameFields(h, names));
    }
    /// <summary>
    /// Convert the struct column into a JSON string column.
    /// Useful for debugging or exporting to systems that support JSON strings.
    /// </summary>
    public Expr JsonEncode()
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StructJsonEncode(h));
    }
}

/// <summary>
/// Offers methods for renaming columns.
/// </summary>
public class NameOps
{
    private readonly Expr _expr;
    internal NameOps(Expr expr) { _expr = expr; }
    /// <summary>
    /// Prefix the column name with a specified string.
    /// </summary>
    /// <param name="prefix"></param>
    /// <returns></returns>
    public Expr Prefix(string prefix)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new(PolarsWrapper.Prefix(h, prefix));
    }

    /// <summary>
    /// Suffix the column name with a specified string.
    /// </summary>
    /// <param name="suffix"></param>
    /// <returns></returns>
    public Expr Suffix(string suffix)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new(PolarsWrapper.Suffix(h, suffix));
    }
}