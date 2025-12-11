using System;
using Apache.Arrow;
using Polars.Native;

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
    private ExprHandle CloneHandle() => PolarsWrapper.CloneExpr(Handle);
    /// <summary>
    /// Creates an expression evaluating if the left operand is greater than the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator >(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.Gt(l, r));
    }

    /// <summary>
    /// Creates an expression evaluating if the left operand is less than the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator <(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.Lt(l, r));
    }

    /// <summary>
    /// Creates an expression evaluating if the left operand is greater than or equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator >=(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.GtEq(l, r)); 
    }

    /// <summary>
    /// Creates an expression evaluating if the left operand is less than or equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator <=(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.LtEq(l, r)); 
    }

    /// <summary>
    /// Creates an expression evaluating if the left operand is equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator ==(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.Eq(l, r));   
    }

    /// <summary>
    /// Creates an expression evaluating if the left operand is not equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator !=(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.Neq(l, r));  
    }

    // ==========================================
    // Arithmetic Operators
    // ==========================================

    /// <summary>
    /// Creates an expression representing the addition of two expressions.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the sum.</returns>
    public static Expr operator +(Expr left, Expr right) => 
        new(PolarsWrapper.Add(left.Handle, right.Handle));

    /// <summary>
    /// Creates an expression representing the subtraction of the right expression from the left expression.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the difference.</returns>
    public static Expr operator -(Expr left, Expr right) => 
        new(PolarsWrapper.Sub(left.Handle, right.Handle));

    /// <summary>
    /// Creates an expression representing the multiplication of two expressions.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the product.</returns>
    public static Expr operator *(Expr left, Expr right) => 
        new(PolarsWrapper.Mul(left.Handle, right.Handle));

    /// <summary>
    /// Creates an expression representing the division of the left expression by the right expression.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the quotient.</returns>
    public static Expr operator /(Expr left, Expr right) => 
        new(PolarsWrapper.Div(left.Handle, right.Handle));
    
    // ==========================================
    // Logical Operators
    // ==========================================

    /// <summary>
    /// Creates an expression representing the logical AND operation.
    /// </summary>
    /// <param name="left">The left boolean expression.</param>
    /// <param name="right">The right boolean expression.</param>
    /// <returns>A boolean expression that evaluates to true if both operands are true.</returns>
    public static Expr operator &(Expr left, Expr right) => 
        new(PolarsWrapper.And(left.Handle, right.Handle));

    /// <summary>
    /// Creates an expression representing the logical OR operation.
    /// </summary>
    /// <param name="left">The left boolean expression.</param>
    /// <param name="right">The right boolean expression.</param>
    /// <returns>A boolean expression that evaluates to true if at least one operand is true.</returns>
    public static Expr operator |(Expr left, Expr right) => 
        new(PolarsWrapper.Or(left.Handle, right.Handle));

    /// <summary>
    /// Creates an expression representing the logical NOT operation.
    /// </summary>
    /// <param name="expr">The boolean expression to negate.</param>
    /// <returns>A boolean expression that evaluates to the opposite truth value.</returns>
    public static Expr operator !(Expr expr) => 
        new(PolarsWrapper.Not(expr.Handle));

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
    public Expr Sum()
    {
        // 1. 克隆 Handle (防止当前对象被底层消耗)
        var cloned = PolarsWrapper.CloneExpr(Handle);
        // 2. 调用底层 Wrapper
        return new Expr(PolarsWrapper.Sum(cloned));
    }

    /// <summary>
    /// Mean
    /// </summary>
    public Expr Mean()
    {
        var cloned = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Mean(cloned));
    }

    /// <summary>
    /// Max
    /// </summary>
    public Expr Max()
    {
        var cloned = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Max(cloned));
    }

    /// <summary>
    /// Min
    /// </summary>
    public Expr Min()
    {
        var cloned = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Min(cloned));
    }

    // ==========================================
    // Math
    // ==========================================

    /// <summary>
    /// Abs
    /// </summary>
    public Expr Abs()
    {
        var cloned = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Abs(cloned));
    }

    /// <summary>
    /// Calculate the square root of the expression.
    /// </summary>
    public Expr Sqrt()
    {
        var e = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Sqrt(e));
    }

    /// <summary>
    /// Calculate the power of the expression with a given exponent expression.
    /// </summary>
    public Expr Pow(Expr exponent)
    {
        var b = PolarsWrapper.CloneExpr(Handle);
        var e = PolarsWrapper.CloneExpr(exponent.Handle);
        return new Expr(PolarsWrapper.Pow(b, e));
    }

    /// <summary>
    /// Calculate the power of the expression with a given numeric exponent.
    /// </summary>
    public Expr Pow(double exponent)
    {
        var b = PolarsWrapper.CloneExpr(Handle);
        var e = PolarsWrapper.Lit(exponent); 
        return new Expr(PolarsWrapper.Pow(b, e));
    }

    /// <summary>
    /// Calculate the power of the Euler's number.
    /// </summary>
    public Expr Exp()
    {
        var e = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Exp(e));
    }
    /// <summary>
    /// Calculate the ln of Number 
    /// </summary>
    /// <param name="baseVal"></param>
    /// <returns></returns>
    public Expr Ln(double baseVal = Math.E)
    {
        var e = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Log(e, baseVal));
    }
    /// <summary>
    /// Round the number
    /// </summary>
    /// <param name="decimals"></param>
    /// <returns></returns>
    public Expr Round(uint decimals)
    {
        var e = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Round(e, decimals));
    }

    // ==========================================
    // Null Handling
    // ==========================================

    /// <summary>
    /// Fill null values with a specified value.
    /// </summary>
    /// <param name="fillValue">The expression (or literal) to replace nulls with.</param>
    public Expr FillNull(Expr fillValue)
    {
        var e = PolarsWrapper.CloneExpr(Handle);
        var f = PolarsWrapper.CloneExpr(fillValue.Handle);
        return new Expr(PolarsWrapper.FillNull(e, f));
    }
    /// <summary>
    /// Fill null values with a specified literal value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Expr FillNull(int value) => FillNull(Polars.Lit(value));
    /// <summary>
    /// Fill null values with a specified literal value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Expr FillNull(double value) => FillNull(Polars.Lit(value));
    /// <summary>
    /// Fill null values with a specified literal value.    
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Expr FillNull(string value) => FillNull(Polars.Lit(value));
    /// <summary>
    /// Fill null values with a specified literal value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Expr FillNull(bool value) => FillNull(Polars.Lit(value));
    /// <summary>
    /// Fill null values with a specified literal value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Expr FillNull(long value) => FillNull(Polars.Lit(value));
    /// <summary>
    /// Fill null values with a specified literal value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Expr FillNull(float value) => FillNull(Polars.Lit(value));
    /// <summary>
    /// Fill null values with a specified literal value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Expr FillNull(DateTime value) => FillNull(Polars.Lit(value));
    /// <summary>
    /// Fill null values with a specific strategy (Forward).
    /// </summary>
    /// <param name="limit">Max number of consecutive nulls to fill. (Default null = infinite)</param>
    public Expr ForwardFill(uint? limit = null)
    {
        return new Expr(PolarsWrapper.ForwardFill(Handle, limit ?? 0));
    }

    /// <summary>
    /// Fill null values with a specific strategy (Backward).
    /// </summary>
    public Expr BackwardFill(uint? limit = null)
    {
        return new Expr(PolarsWrapper.BackwardFill(Handle, limit ?? 0));
    }
    /// <summary>
    /// Evaluate whether the expression is null.
    /// </summary>
    public Expr IsNull()
    {
        var e = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.IsNull(e));
    }

    /// <summary>
    /// Evaluate whether the expression is not null.
    /// </summary>
    public Expr IsNotNull()
    {
        var e = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.IsNotNull(e));
    }
    // ==========================================
    // Statistical Ops
    // ==========================================

    /// <summary>
    /// Count the number of values in this expression.
    /// </summary>
    public Expr Count() => new Expr(PolarsWrapper.Count(Handle));

    /// <summary>
    /// Get the standard deviation.
    /// </summary>
    public Expr Std(int ddof = 1) => new Expr(PolarsWrapper.Std(Handle, ddof));

    /// <summary>
    /// Get the variance.
    /// </summary>
    public Expr Var(int ddof = 1) => new Expr(PolarsWrapper.Var(Handle, ddof));

    /// <summary>
    /// Get the median value.
    /// </summary>
    public Expr Median() => new Expr(PolarsWrapper.Median(Handle));

    /// <summary>
    /// Get the quantile value.
    /// </summary>
    public Expr Quantile(double quantile, string method = "nearest") 
        => new Expr(PolarsWrapper.Quantile(CloneHandle(), quantile, method)); // CloneHandle 因为 Quantile 消耗 Expr
    // ==========================================
    // Logic / Comparison
    // ==========================================

    /// <summary>
    /// Check if the value is between lower and upper bounds (inclusive).
    /// </summary>
    public Expr IsBetween(Expr lower, Expr upper)
    {
        var e = PolarsWrapper.CloneExpr(Handle);
        var l = PolarsWrapper.CloneExpr(lower.Handle);
        var u = PolarsWrapper.CloneExpr(upper.Handle);
        
        return new Expr(PolarsWrapper.IsBetween(e, l, u));
    }
    // ==========================================
    // Casting
    // ==========================================

    /// <summary>
    /// Cast the expression to a different data type.
    /// </summary>
    public Expr Cast(DataType dtype, bool strict = false)
    {
        var h = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.ExprCast(h, dtype.Handle, strict));
    }
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
    {
        // 1. 将用户的强类型 Func 转换为 Arrow Func
        var arrowFunc = UdfUtils.Wrap(function);

        // 2. Clone Handle
        var h = PolarsWrapper.CloneExpr(Handle);

        // 3. 调用底层 Wrapper (传递 DataType.Handle)
        return new Expr(PolarsWrapper.Map(h, arrowFunc, outputType.Handle));
    }

    /// <summary>
    /// Apply a raw Arrow-to-Arrow UDF. (Advanced / Internal use)
    /// </summary>
    public Expr Map(Func<IArrowArray, IArrowArray> function, DataType outputType)
    {
        // 1. 直接使用用户提供的 Arrow 函数
        // 2. Clone Handle
        var h = PolarsWrapper.CloneExpr(Handle);

        // 3. 调用底层 Wrapper (传递 DataType.Handle)
        return new Expr(PolarsWrapper.Map(h, function, outputType.Handle));
    }
    // ==========================================
    // Rolling Window Functions
    // ==========================================
    /// <summary>
    /// Rolling Minimum
    /// </summary>
    /// <param name="windowSize"></param>
    /// <returns></returns>
    public Expr RollingMin(string windowSize) 
        => new Expr(PolarsWrapper.RollingMin(CloneHandle(), windowSize));
    /// <summary>
    /// Rolling Maximum
    /// </summary>
    /// <param name="windowSize"></param>
    /// <returns></returns>
    public Expr RollingMax(string windowSize) 
        => new Expr(PolarsWrapper.RollingMax(CloneHandle(), windowSize));
    /// <summary>
    /// Rolling Mean
    /// </summary>
    /// <param name="windowSize"></param>
    /// <returns></returns>
    public Expr RollingMean(string windowSize) 
        => new Expr(PolarsWrapper.RollingMean(CloneHandle(), windowSize));
    /// <summary>
    /// Rolling Sum
    /// </summary>
    /// <param name="windowSize"></param>
    /// <returns></returns>
    public Expr RollingSum(string windowSize) 
        => new Expr(PolarsWrapper.RollingSum(CloneHandle(), windowSize));
    /// <summary>
    /// Rolling Mean By
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingMeanBy(string windowSize, Expr by, string closed = "left")
    {
        return new Expr(PolarsWrapper.RollingMeanBy(
            CloneHandle(), 
            windowSize, 
            by.CloneHandle(), 
            closed
        ));
    }
    /// <summary>
    /// Rolling Sum By
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingSumBy(string windowSize, Expr by, string closed = "left")
    {
        return new Expr(PolarsWrapper.RollingSumBy(
            CloneHandle(), 
            windowSize, 
            by.CloneHandle(), 
            closed
        ));
    }
    /// <summary>
    /// Rolling Min By
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingMinBy(string windowSize, Expr by, string closed = "left")
    {
        return new Expr(PolarsWrapper.RollingMinBy(
            CloneHandle(), 
            windowSize, 
            by.CloneHandle(), 
            closed
        ));
    }
    /// <summary>
    /// Rolling Max By
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="by"></param>
    /// <param name="closed"></param>
    /// <returns></returns>
    public Expr RollingMaxBy(string windowSize, Expr by, string closed = "left")
    {
        return new Expr(PolarsWrapper.RollingMaxBy(
            CloneHandle(), 
            windowSize, 
            by.CloneHandle(), 
            closed
        ));
    }
    // ==========================================
    // Namespaces
    // ==========================================

    /// <summary>
    /// Access temporal (Date/Time) operations.
    /// </summary>
    public DtOps Dt => new DtOps(this);

    /// <summary>
    /// Access string manipulation operations.
    /// </summary>
    public StringOps Str => new StringOps(this);

    /// <summary>
    /// Access list operations.
    /// </summary>
    public ListOps List => new ListOps(this);

    /// <summary>
    /// Access struct operations.
    /// </summary>
    public StructOps Struct => new StructOps(this);

    /// <summary>
    /// Access column renaming operations.
    /// </summary>
    public NameOps Name => new NameOps(this);
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
        if (this.Handle.IsInvalid || other.Handle.IsInvalid) return false;

        // 4. 严谨的底层指针比较
        // 两个 Expr 即使逻辑相同，如果它们对应 Rust 侧不同的内存地址，也被视为不等。
        // 这对于防止 Dictionary<Expr, T> 产生意外行为至关重要。
        return this.Handle.DangerousGetHandle() == other.Handle.DangerousGetHandle();
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
        => new Expr(PolarsWrapper.Prefix(_expr.Handle, prefix)); // Wrapper 需确认签名
    /// <summary>
    /// Suffix the column name with a specified string.
    /// </summary>
    /// <param name="suffix"></param>
    /// <returns></returns>
    public Expr Suffix(string suffix) 
        => new Expr(PolarsWrapper.Suffix(_expr.Handle, suffix));
}