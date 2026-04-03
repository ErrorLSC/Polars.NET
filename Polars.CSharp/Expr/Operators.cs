#pragma warning disable CS1591
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// A Polars Expr
/// </summary>
public partial class Expr : IDisposable
{
    public static implicit operator Expr(int value) => Polars.Lit(value);

    public static implicit operator Expr(double value) => Polars.Lit(value);

    public static implicit operator Expr(DateTime value) => Polars.Lit(value);
    public static implicit operator Expr(DateTimeOffset value) => Polars.Lit(value);
    public static implicit operator Expr(DateOnly value) => Polars.Lit(value);
    public static implicit operator Expr(TimeOnly value) => Polars.Lit(value);
    public static implicit operator Expr(TimeSpan value) => Polars.Lit(value);

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
}