#pragma warning disable CS1591 // 缺少对公共可见类型或成员的 XML 注释
using Apache.Arrow;
using Polars.NET.Core;

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
    private static Expr MakeLit(object val)
    {
        // 1. 如果传进来的已经是 Expr，直接返回（防止套娃）
        if (val is Expr e) return e;

        // 2. 如果是 Selector，隐式转为 Expr
        if (val is Selector s) return s.ToExpr();

        // 3. 根据运行时类型分发给具体的 Wrapper 方法
        return val switch
        {
            // --- 整数 ---
            int i => new Expr(PolarsWrapper.Lit(i)),
            long l => new Expr(PolarsWrapper.Lit(l)),
            // short, byte 也可以转为 int 或 long 处理
            short sh => new Expr(PolarsWrapper.Lit(sh)),
            byte by => new Expr(PolarsWrapper.Lit(by)),

            // --- 浮点 ---
            double d => new Expr(PolarsWrapper.Lit(d)),
            float f => new Expr(PolarsWrapper.Lit(f)),
            // decimal 通常需要转 double，因为 Polars 内核没有 decimal128 的直接 Lit 入口(通常)
            // 或者如果有 pl_lit_decimal 再改
            // decimal dec => new Expr(PolarsWrapper.Lit((double)dec)), 

            // --- 基础 ---
            string str => new Expr(PolarsWrapper.Lit(str)),
            bool b => new Expr(PolarsWrapper.Lit(b)),
            
            // --- 时间 ---
            // 假设 Wrapper 里有对应实现
            DateTime dt => new Expr(PolarsWrapper.Lit(dt)),
            
            // --- Null ---
            null => new Expr(PolarsWrapper.LitNull()),

            // --- 报错 ---
            _ => throw new NotSupportedException($"Unsupported literal type: {val.GetType().Name}")
        };
    }
    private ExprHandle CloneHandle() => PolarsWrapper.CloneExpr(Handle);

    public static implicit operator Expr(int value) => Polars.Lit(value);

    public static implicit operator Expr(double value) => Polars.Lit(value);

    public static implicit operator Expr(string value) => Polars.Lit(value);

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
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the sum.</returns>
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
    {
        return new Expr(PolarsWrapper.FloorDiv(this.CloneHandle(), other.CloneHandle()));
    }

    public Expr FloorDiv(object other)
    {
        return new Expr(PolarsWrapper.FloorDiv(this.CloneHandle(), MakeLit(other).Handle));
    }
    // ==========================================
    // Logical Operators
    // ==========================================

    /// <summary>
    /// Creates an expression representing the logical AND operation.
    /// </summary>
    /// <param name="left">The left boolean expression.</param>
    /// <param name="right">The right boolean expression.</param>
    /// <returns>A boolean expression that evaluates to true if both operands are true.</returns>
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
    {
        var e = expr.CloneHandle();
        return new Expr(PolarsWrapper.Not(e));
    }
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
    // ---------------------------------------------------
    // 基础方法
    // ---------------------------------------------------

    /// <summary>
    /// Set a new name for a column
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public Expr Alias(string name) => 
        new(PolarsWrapper.Alias(Handle, name));

    // ==========================================
    // Aggregation (聚合函数)
    // ==========================================

    /// <summary>
    /// Sum
    /// </summary>
    public Expr Sum() => new(PolarsWrapper.Sum(CloneHandle()));

    /// <summary>
    /// Mean
    /// </summary>
    public Expr Mean() => new(PolarsWrapper.Mean(CloneHandle()));

    /// <summary>
    /// Max
    /// </summary>
    public Expr Max() => new(PolarsWrapper.Max(CloneHandle()));

    /// <summary>
    /// Min
    /// </summary>
    public Expr Min() => new(PolarsWrapper.Min(CloneHandle()));

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
    /// Calculate the power of the expression with a given exponent expression.
    /// </summary>
    public Expr Pow(Expr exponent) => new(PolarsWrapper.Pow(CloneHandle(), exponent.CloneHandle()));

    /// <summary>
    /// Calculate the power of the expression with a given numeric exponent.
    /// </summary>
    public Expr Pow(double exponent) => new(PolarsWrapper.Pow(CloneHandle(),PolarsWrapper.Lit(exponent)));

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
    /// <summary>
    /// Round the number
    /// </summary>
    /// <param name="decimals"></param>
    /// <returns></returns>
    public Expr Round(uint decimals) => new(PolarsWrapper.Round(CloneHandle(), decimals));

    // ==========================================
    // Null Handling
    // ==========================================

    /// <summary>
    /// Fill null values with a specified value.
    /// </summary>
    /// <param name="fillValue">The expression (or literal) to replace nulls with.</param>
    public Expr FillNull(Expr fillValue) => new (PolarsWrapper.FillNull(CloneHandle(), fillValue.CloneHandle()));
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
    public Expr BackwardFill(uint? limit = null)=> new(PolarsWrapper.BackwardFill(CloneHandle(), limit ?? 0));
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
    {
        // 自动把 value 转为 Lit Expr
        return new Expr(PolarsWrapper.FillNan(CloneHandle(), MakeLit(value).Handle));
    }
    // ==========================================
    // Statistical Ops
    // ==========================================

    /// <summary>
    /// Count the number of values in this expression.
    /// </summary>
    public Expr Count() => new(PolarsWrapper.Count(CloneHandle()));

    /// <summary>
    /// Get the standard deviation.
    /// </summary>
    public Expr Std(int ddof = 1) => new(PolarsWrapper.Std(CloneHandle(), ddof));

    /// <summary>
    /// Get the variance.
    /// </summary>
    public Expr Var(int ddof = 1) => new(PolarsWrapper.Var(CloneHandle(), ddof));

    /// <summary>
    /// Get the median value.
    /// </summary>
    public Expr Median() => new(PolarsWrapper.Median(CloneHandle()));

    /// <summary>
    /// Get the quantile value.
    /// </summary>
    public Expr Quantile(double quantile, string method = "nearest") 
        => new(PolarsWrapper.Quantile(CloneHandle(), quantile, method)); // CloneHandle 因为 Quantile 消耗 Expr
    // ==========================================
    // Logic / Comparison
    // ==========================================

    /// <summary>
    /// Check if the value is between lower and upper bounds (inclusive).
    /// </summary>
    public Expr IsBetween(Expr lower, Expr upper)  
        => new(PolarsWrapper.IsBetween(CloneHandle(), lower.CloneHandle(), upper.CloneHandle()));

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

    #region Window & Offset Functions

    /// <summary>
    /// Window function: Apply aggregation over specific groups.
    /// Example: Col("Amt").Sum().Over(Col("Group"))
    /// </summary>
    public Expr Over(params Expr[] partitionBy)
    {
        // 1. 准备 Partition Handles (需要 Clone，因为 Wrapper 会接管所有权)
        var partitionHandles = System.Array.ConvertAll(partitionBy, e => e.CloneHandle());

        // 2. 调用 Wrapper (注意：this.CloneHandle() 防止当前对象被消费)
        var h = PolarsWrapper.Over(CloneHandle(), partitionHandles);
        
        return new Expr(h);
    }

    /// <summary>
    /// 语法糖: 允许直接传字符串列名
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
    /// Static Rolling Minimum
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Expr RollingMin(string windowSize, int minPeriods = 1) 
        => new(PolarsWrapper.RollingMin(CloneHandle(), windowSize, minPeriods));
    /// <summary>
    /// Stastic Rolling Minumum by timespan
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Expr RollingMin(TimeSpan windowSize, int minPeriods = 1) 
        => RollingMin(DurationFormatter.ToPolarsString(windowSize),minPeriods);
    /// <summary>
    /// Static Rolling Maximum
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Expr RollingMax(string windowSize,int minPeriods=1) 
        => new(PolarsWrapper.RollingMax(CloneHandle(), windowSize, minPeriods));
    /// <summary>
    /// Static Rolling Maximum by timespan
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Expr RollingMax(TimeSpan windowSize, int minPeriods = 1) 
        => RollingMax(DurationFormatter.ToPolarsString(windowSize),minPeriods);
    /// <summary>
    /// Static Rolling Mean
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Expr RollingMean(string windowSize,int minPeriods=1) 
        => new(PolarsWrapper.RollingMean(CloneHandle(), windowSize,minPeriods));
    /// <summary>
    /// Static Rolling Mean by timespan
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Expr RollingMean(TimeSpan windowSize, int minPeriods = 1) 
        => RollingMean(DurationFormatter.ToPolarsString(windowSize),minPeriods);
    /// <summary>
    /// Static Rolling Sum
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Expr RollingSum(string windowSize,int minPeriods=1) 
        => new(PolarsWrapper.RollingSum(CloneHandle(), windowSize,minPeriods));
    /// <summary>
    /// Static Rolling Sum by timespan
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Expr RollingSum(TimeSpan windowSize, int minPeriods = 1) 
        => RollingSum(DurationFormatter.ToPolarsString(windowSize),minPeriods);
    /// <summary>
    /// Dynamic Rolling Mean By
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingMeanBy(string windowSize, Expr by, int minPeriods=1,string closed = "left")
    {
        return new Expr(PolarsWrapper.RollingMeanBy(
            CloneHandle(), 
            windowSize,
            minPeriods, 
            by.CloneHandle(), 
            closed
        ));
    }
    /// <summary>
    /// Dynamic Rolling Mean By timespan
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingMeanBy(TimeSpan windowSize, Expr by, int minPeriods=1,string closed = "left")
    {
        return RollingMeanBy( 
            DurationFormatter.ToPolarsString(windowSize),
            by, 
            minPeriods, 
            closed
        );
    }
    /// <summary>
    /// Dynamic Rolling Sum By
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingSumBy(string windowSize, Expr by,int minPeriods=1, string closed = "left")
    {
        return new Expr(PolarsWrapper.RollingSumBy(
            CloneHandle(), 
            windowSize, 
            minPeriods,
            by.CloneHandle(), 
            closed
        ));
    }
    /// <summary>
    /// Dynamic Rolling Sum By timespan
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingSumBy(TimeSpan windowSize, Expr by, int minPeriods=1,string closed = "left")
    {
        return RollingSumBy( 
            DurationFormatter.ToPolarsString(windowSize),
            by, 
            minPeriods, 
            closed
        );
    }
    /// <summary>
    /// Dynamic Rolling Min By
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingMinBy(string windowSize, Expr by,int minPeriods=1, string closed = "left")
    {
        return new Expr(PolarsWrapper.RollingMinBy(
            CloneHandle(), 
            windowSize, 
            minPeriods,
            by.CloneHandle(), 
            closed
        ));
    }
    /// <summary>
    /// Dynamic Rolling Min By timespan
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingMinBy(TimeSpan windowSize, Expr by, int minPeriods=1,string closed = "left")
    {
        return RollingMinBy( 
            DurationFormatter.ToPolarsString(windowSize),
            by, 
            minPeriods, 
            closed
        );
    }
    /// <summary>
    /// Dynamic Rolling Max By
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingMaxBy(string windowSize, Expr by,int minPeriods=1,string closed = "left")
    {
        return new Expr(PolarsWrapper.RollingMaxBy(
            CloneHandle(), 
            windowSize, 
            minPeriods,
            by.CloneHandle(), 
            closed
        ));
    }
    /// <summary>
    /// Dynamic Rolling Max By timespan
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingMaxBy(TimeSpan windowSize, Expr by, int minPeriods=1,string closed = "left")
    {
        return RollingMaxBy( 
            DurationFormatter.ToPolarsString(windowSize),
            by, 
            minPeriods, 
            closed
        );
    }
    /// <summary>
    /// Explode a list column into multiple rows.
    /// </summary>
    public Expr Explode() => new(PolarsWrapper.Explode(CloneHandle()));
    /// <summary>
    /// Aggregate values into a list.
    /// </summary>
    public Expr Implode() => new Expr(PolarsWrapper.Implode(CloneHandle()));
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
    // ---------------------------------------------------
    // Clean Up
    // ---------------------------------------------------
    /// <summary>
    /// Dispose a handle.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
    }

    /// <summary>
    /// 判断当前 Expr 对象是否与另一个对象是同一个实例（基于底层指针地址）。
    /// <para>注意：Polars 的 Expr 是不透明指针。即使逻辑相同的两个表达式（如 col("a") 和 col("a")），
    /// 它们在底层也是不同的内存对象，因此 Equals 会返回 false。</para>
    /// </summary>
    public override bool Equals(object? obj)
    {
        // 1. 快速检查引用是否指向同一托管对象
        if (ReferenceEquals(this, obj)) return true;

        // 2. 类型检查 (Pattern Matching)
        if (obj is not Expr other) return false;

        // 3. 句柄有效性检查 (防止操作已释放的句柄)
        if (Handle.IsInvalid || other.Handle.IsInvalid) return false;

        // 4. 严谨的底层指针比较
        // 两个 Expr 即使逻辑相同，如果它们对应 Rust 侧不同的内存地址，也被视为不等。
        // 这对于防止 Dictionary<Expr, T> 产生意外行为至关重要。
        return Handle.DangerousGetHandle() == other.Handle.DangerousGetHandle();
    }

    /// <summary>
    /// 获取基于底层指针地址的哈希码。
    /// </summary>
    public override int GetHashCode()
    {
        // 如果句柄无效，返回 0 或抛出异常（这里选择返回 0 以保证稳定性）
        if (Handle.IsInvalid) return 0;
        
        // 使用 IntPtr 自身的 GetHashCode，它是基于内存地址的
        return Handle.DangerousGetHandle().GetHashCode();
    }
}
// ==========================================
// DtOps Helper Class
// ==========================================

/// <summary>
/// Contains methods for temporal (Date/Time) operations.
/// </summary>
public class DtOps
{
    private readonly Expr _expr;
    
    internal DtOps(Expr expr)
    {
        _expr = expr;
    }

    // 辅助函数：自动 Clone 并调用 Wrapper
    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(op(h));
    }

    /// <summary>Get the year from the underlying date/datetime.</summary>
    public Expr Year() => Wrap(PolarsWrapper.DtYear);

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
    /// Format the date/datetime as a string using the given format string (strftime).
    /// </summary>
    public Expr ToString(string format)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtToString(h, format));
    }

    /// <summary>
    /// Format the date/datetime as a string using the default format "%Y-%m-%dT%H:%M:%S%.f".
    /// </summary>
    public override string ToString()
    {
        // 注意：这里重写的是 C# 对象的 ToString，不是生成 Expr。
        // 如果你想生成 Expr，应该用 ToString(format)
        return "DtOps"; 
    }

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
    // Truncate & Round (时间对齐)
    // ==========================================

    /// <summary>
    /// Truncate the datetimes to the given interval (e.g. "1d", "1h", "15m").
    /// </summary>
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
    // Offset (时间平移)
    // ==========================================

    /// <summary>
    /// Offset the datetimes by a given duration expression.
    /// </summary>
    public Expr OffsetBy(Expr by)
    {
        return new Expr(PolarsWrapper.DtOffsetBy(_expr.Handle, by.Handle));
    }

    /// <summary>
    /// Offset the datetimes by a constant duration string (e.g., "1d", "-2h").
    /// </summary>
    public Expr OffsetBy(string duration)
    {
        return OffsetBy(Polars.Lit(duration));
    }
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
    // Timestamp (转整数)
    // ==========================================

    /// <summary>
    /// Convert the datetime to an integer timestamp (Unix epoch).
    /// </summary>
    public Expr Timestamp(TimeUnit unit = TimeUnit.Microseconds)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtTimestamp(h, (int)unit));
    }

    // ==========================================
    // TimeZone
    // ==========================================
    /// <summary>
    /// Convert from one timezone to another.
    /// Resulting Series will have the given time zone.
    /// </summary>
    /// <param name="timeZone">Target time zone (e.g. "Asia/Shanghai")</param>
    public Expr ConvertTimeZone(string timeZone)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtConvertTimeZone(h, timeZone));
    }

    /// <summary>
    /// Replace the time zone of a Series.
    /// This does not change the underlying timestamp, only the metadata.
    /// </summary>
    public Expr ReplaceTimeZone(string? timeZone, string? ambiguous = null, string? nonExistent = "raise")
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtReplaceTimeZone(h, timeZone, ambiguous, nonExistent));
    }
    
}

// ==========================================
// StringOps Helper Class
// ==========================================
/// <summary>
/// Offers multiple methods for checking and parsing elements of a string column.
/// </summary>
public class StringOps
{
    private readonly Expr _expr;
    internal StringOps(Expr expr) { _expr = expr; }

    // 辅助函数：Clone 并调用
    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(op(h));
    }
    /// <summary>
    /// Transfer String to UpperClass.
    /// </summary>
    public Expr ToUpper() => Wrap(PolarsWrapper.StrToUpper);
    /// <summary>
    /// Transfer String to LowerClass.
    /// </summary>
    public Expr ToLower() => Wrap(PolarsWrapper.StrToLower);
    
    /// <summary>
    /// Get length in bytes.
    /// </summary>
    public Expr Len() => Wrap(PolarsWrapper.StrLenBytes);
    /// <summary>
    /// Slice string by length.
    /// </summary>
    /// <param name="offset"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public Expr Slice(long offset, ulong length)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrSlice(h, offset, length));
    }
    /// <summary>
    /// Replace charaters in a string.
    /// </summary>
    /// <param name="pattern"></param>
    /// <param name="value"></param>
    /// <param name="useRegex"></param>
    /// <returns></returns>
    public Expr ReplaceAll(string pattern, string value, bool useRegex = false)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrReplaceAll(h, pattern, value, useRegex));
    }
    /// <summary>
    /// Extract charaters in string by Regex.
    /// </summary>
    /// <param name="pattern"></param>
    /// <param name="groupIndex"></param>
    /// <returns></returns>
    public Expr Extract(string pattern, uint groupIndex)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrExtract(h, pattern, groupIndex));
    }
    /// <summary>
    /// Check if the string contains a substring that matches a pattern.
    /// </summary>
    /// <param name="pattern"></param>
    /// <returns></returns>
    public Expr Contains(string pattern)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrContains(h, pattern));
    }
    /// <summary>
    /// Split the string by a substring.
    /// </summary>
    /// <param name="separator"></param>
    /// <returns></returns>
    public Expr Split(string separator)
    {
         var h = PolarsWrapper.CloneExpr(_expr.Handle);
         return new Expr(PolarsWrapper.StrSplit(h, separator));
    }
    // ==========================================
    // Strip / Clean (去除字符)
    // ==========================================

    /// <summary>
    /// Remove leading and trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    /// <param name="matches">The set of characters to be removed.</param>
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
    // Temporal Parsing (日期转换)
    // ==========================================

    /// <summary>
    /// Convert string to Date using the specified format.
    /// </summary>
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
/// Offers methods for list operations.
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
    /// Get the last element of the list.
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    public Expr Get(int index) 
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.ListGet(h, index));
    }
    /// <summary>
    /// Get the length of the list.
    /// </summary>
    /// <returns></returns>
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
    /// <returns></returns>
    public Expr Sort(bool descending)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.ListSort(h, descending));
    }
    /// <summary>
    /// Calculate the sum of the list elements.
    /// </summary>
    /// <returns></returns>
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
    /// <returns></returns>
    public Expr Contains(Expr item)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        var i = PolarsWrapper.CloneExpr(item.Handle);
        return new Expr(PolarsWrapper.ListContains(h, i));
    }
    
    /// <summary>
    /// Check if the list contains a specific integer or string item.
    /// </summary>
    /// <param name="item"></param>
    /// <returns></returns>
    public Expr Contains(int item) => Contains(Polars.Lit(item));
    /// <summary>
    /// Check if the list contains a specific string item.
    /// </summary>
    /// <param name="item"></param>
    /// <returns></returns>
    public Expr Contains(string item) => Contains(Polars.Lit(item));
}

// ==========================================
// StructOps Helper Class
// ==========================================
/// <summary>
/// Offers methods for accessing fields within struct columns.
/// </summary>
public class StructOps
{
    private readonly Expr _expr;
    internal StructOps(Expr expr) { _expr = expr; }

    /// <summary>
    /// Retrieve a field from the struct by name.
    /// </summary>
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
        return new(PolarsWrapper.Prefix(h, prefix)); // Wrapper 需确认签名 
    }

    /// <summary>
    /// Suffix the column name with a specified string.
    /// </summary>
    /// <param name="suffix"></param>
    /// <returns></returns>
    public Expr Suffix(string suffix) 
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new(PolarsWrapper.Suffix(h, suffix)); // Wrapper 需确认签名 
    }
}