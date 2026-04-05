#pragma warning disable CS1591
using System.Runtime.CompilerServices;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Helpers;
using Cs = Polars.CSharp.Polars.Selectors;
namespace Polars.CSharp;

/// <summary>
/// Polars Static Helpers
/// </summary>
public readonly partial struct Polars
{
    /// <summary>
    /// Column Expr (name: string)
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public static Expr Col(string name)
        => new(PolarsWrapper.Col(name));
    /// <summary>
    /// Column Exprs (name: string)
    /// </summary>
    /// <param name="names"></param>
    /// <returns></returns>
    public static Expr Col(params string[] names) 
    { 
        using Selector sel = Cs.ByName(names);
        return sel.ToExpr();
    }
    /// <summary>
    /// Select all columns, same as Col("*")
    /// </summary>
    /// <returns></returns>
    public static Expr All()
        => Col("*");
    /// <summary>
    /// Return the lines count of current context.
    /// </summary>
    public static Expr Len()
        => new(PolarsWrapper.Len());
    // --- Literals ---
    public static Expr Lit(string? value)
    {
        if (value is null)
        {
            return new Expr(PolarsWrapper.LitNull());
        }
        return new Expr(PolarsWrapper.Lit(value));
    }
    public static Expr Lit(sbyte value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(byte value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(short value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(ushort value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(int value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(uint value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(long value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(ulong value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(Int128 value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(double value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(DateTime value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(DateTimeOffset value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(DateOnly value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(TimeOnly value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(TimeSpan value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(bool value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(Half value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(float value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(decimal value) => new(PolarsWrapper.Lit(value));
    public static Expr LitNull() => new(PolarsWrapper.LitNull());
    /// <summary>
    /// Convert Series into Literal Expr.
    /// <para>The Series is cloned implicitly, so the original Series remains valid.</para>
    /// </summary>
    public static Expr Lit(Series series)
    {
        var clonedHandle = PolarsWrapper.CloneSeries(series.Handle);
        return new Expr(PolarsWrapper.Lit(clonedHandle));
    }
    // -------------------------------------------------------------------------
    // Struct Literals
    // -------------------------------------------------------------------------

    /// <summary>
    /// Create a Struct Expression from a single anonymous object or class instance.
    /// <para>Example: <c>LitStruct(new { A = 1, B = "hi" })</c></para>
    /// </summary>
    public static Expr LitStruct<T>(T value) where T : class
        => LitStruct([value]);

    /// <summary>
    /// Create a Struct Expression from an array of objects.
    /// <para>The properties of the objects become the fields of the struct.</para>
    /// </summary>
    public static Expr LitStruct<T>(T[] values)
    {
        SeriesHandle sHandle = StructPacker.Pack("literal", values);
        ExprHandle eHandle = PolarsWrapper.Lit(sHandle);
        return new Expr(eHandle);
    }
    // =========================================================================
    // The "Magic" Lit
    // =========================================================================

    public static Expr Lit<T>(T[] values)
    {
        using var s = CSharp.Series.From("", values);
        
        return Lit(s);
    }
    
    public static Expr Lit<T>(IEnumerable<T> values)
    {
        if (values is T[] arr) return Lit(arr);
        return Lit(values.ToArray());
    }
    
    // ==========================================
    // Control Flow
    // ==========================================

    /// <summary>
    /// If-Else control flow: if predicate evaluates to true, return trueExpr, otherwise return falseExpr.
    /// Similar to SQL's CASE WHEN ... THEN ... ELSE ... END.
    /// </summary>
    public static Expr IfElse(Expr predicate, Expr trueExpr, Expr falseExpr)
        => new(PolarsWrapper.IfElse(predicate.CloneHandle(), trueExpr.CloneHandle(), falseExpr.CloneHandle()));

    // ==========================================
    // List Operations
    // ==========================================
    /// <summary>
    /// Concat multiple list expressions into a single list expression.
    /// </summary>
    public static Expr ConcatList(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.ConcatList(handles));
    }
    // ==========================================
    // Array Operations
    // ==========================================
    /// <summary>
    /// Concat multiple array expressions into a single array expression.
    /// </summary>
    public static Expr ConcatArray(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.ConcatArray(handles));
    }
    // ==========================================
    // String Operations
    // ==========================================
    /// <summary>
    /// Concat multiple string expressions into a single string expression.
    /// </summary>
    public static Expr ConcatString(string separator=",",bool ignoreNulls=false,params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.ConcatString(handles,separator,ignoreNulls));
    }
    /// <summary>
    /// Format multiple string expressions into a single formated string expression.
    /// </summary>
    public static Expr FormatString(string format,params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.FormatString(format,handles));
    }
    // ==========================================
    // Concat Exprs
    // ==========================================
    /// <summary>
    /// Concat multiple expressions into a single expression.
    /// </summary>
    public static Expr ConcatExpr(bool rechunk=false,params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.ConcatExprs(handles,rechunk));
    }
    // ==========================================
    // Struct Operations
    // ==========================================

    /// <summary>
    /// Combine multiple expressions into a Struct expression.
    /// </summary>
    public static Expr AsStruct(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.AsStruct(handles));
    }
    // ==========================================
    // SQL
    // ==========================================
    /// <summary>
    /// Create a new SQL Context.
    /// </summary>
    public static SqlContext Sql() => new();
    /// <summary>
    /// Create a Polars Expr from a SQL string.
    /// </summary>
    /// <param name="sql">The SQL expression string.</param>
    /// <returns>A Polars Expr representing the SQL logic.</returns>
    /// <exception cref="ArgumentException">Thrown when the provided SQL string is null, empty, or consists only of white-space characters.</exception>
    public static Expr SqlExpr(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException("SQL expression can not be null", nameof(sql));

        return new Expr(PolarsWrapper.SqlExpr(sql));
    }
    /// <summary>
    /// Create an array of Polars Exprs from a collection of SQL strings.
    /// </summary>
    /// <param name="sqls">The collection of SQL expression strings.</param>
    /// <returns>An array of Polars Expr objects.</returns>
    public static Expr[] SqlExprs(IEnumerable<string> sqls) 
            => [.. sqls.Select(SqlExpr)];

    // ==========================================
    // Temporal
    // ==========================================

    /// <summary>
    /// Combine a Date expression and a Time expression into a Datetime expression.
    /// <para>
    /// Useful for combining a Date column with a Time column, or combining literal arrays.
    /// </para>
    /// <para>
    /// <b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.
    /// </para>
    /// </summary>
    /// <param name="date">Expression for the Date component (can be a Column, Literal, or calculation).</param>
    /// <param name="time">Expression for the Time component.</param>
    /// <param name="tu">The desired TimeUnit for the resulting Datetime (default: Microseconds).</param>
    /// <returns>A new expression evaluating to Datetime.</returns>
    /// <example>
    /// <code>
    /// // 1. Combine Columns
    /// df.Select(Polars.Combine(Col("date_col"), Col("time_col")));
    /// 
    /// // 2. Combine Arrays (Literals)
    /// var dates = new[] { new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 2) };
    /// var times = new[] { new TimeOnly(10, 0), new TimeOnly(11, 0) };
    /// 
    /// df.Select(
    ///     Polars.Combine(Polars.Lit(dates), Polars.Lit(times)).Alias("combined")
    /// );
    /// </code>
    /// </example>
    public static Expr CombineDateAndTime(Expr date, Expr time, TimeUnit tu = TimeUnit.Microseconds)
        => date.Dt.Combine(time, tu);

    // ==========================================
    // Fold & Reduce
    // ==========================================

    /// <summary>
    /// Accumulate over multiple columns horizontally/row-wise with a left fold.
    /// </summary>
    public static Expr Fold(Expr acc, Func<Expr, Expr, Expr> f, IEnumerable<Expr> exprs)
    {
        Expr current = acc;
        foreach (var expr in exprs)
        {
            current = f(current, expr);
        }
        return current;
    }

    /// <summary>
    /// Reduce multiple columns horizontally/row-wise with a left fold.
    /// </summary>
    public static Expr Reduce(Func<Expr, Expr, Expr> f, IEnumerable<Expr> exprs)
    {
        using var enumerator = exprs.GetEnumerator();
        if (!enumerator.MoveNext())
            throw new ArgumentException("exprs cannot be empty for Reduce");

        Expr current = enumerator.Current;
        while (enumerator.MoveNext())
        {
            current = f(current, enumerator.Current);
        }
        return current;
    }
    /// <summary>
    /// Gets the DataType of an expression.
    /// Equivalent to Python's polars.dtype_of()
    /// </summary>
    public static DataTypeExpr DataTypeOf(Expr expr)
    {
        var h = PolarsWrapper.DataTypeExprDtypeOf(expr.CloneHandle());
        return new DataTypeExpr(h);
    }

    /// <summary>
    /// Represents the intrinsic data type of the current element.
    /// Equivalent to Python's polars.self_dtype()
    /// </summary>
    public static DataTypeExpr SelfDataType()
    {
        var h = PolarsWrapper.DataTypeExprSelfDtype();
        return new DataTypeExpr(h);
    }
    /// <summary>
    /// Create a DataFrame from a collection of Series.
    /// Example: Pl.DataFrame(Pl.Series("a", new[] {1, 2}), Pl.Series("b", new[] {3, 4}))
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame(params Series[] series)
        => new(series);

    /// <summary>
    /// Create a DataFrame from a collection of strongly-typed objects (POCOs).
    /// Example: Pl.DataFrame(studentList)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame<T>(IEnumerable<T> data)
        => CSharp.DataFrame.FromRows(data);

    /// <summary>
    /// Create a DataFrame from an anonymous object where properties represent columns.
    /// Example: Pl.DataFrame(new { A = new[] { 1, 2 }, B = new[] { "x", "y" } })
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame(object columns)
        => CSharp.DataFrame.FromColumns(columns);

    /// <summary>
    /// Create a DataFrame from explicitly named column tuples.
    /// Example: Pl.DataFrame(("A", new[] { 1, 2 }), ("B", new[] { "x", "y" }))
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame(params (string Name, object Data)[] columns)
        => CSharp.DataFrame.FromColumns(columns);

    /// <summary>
    /// Create a Series from an IEnumerable of objects, primitives, or nested lists.
    /// Example: Pl.Series("Name", list)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series<T>(string name, IEnumerable<T> data)
        => CSharp.Series.From(name, data);

    /// <summary>
    /// Create a Series directly from an array (Fast Path).
    /// Example: Pl.Series("Age", new int[] { 25, 30 })
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series<T>(string name, T[] data)
        => CSharp.Series.From(name, data);

    /// <summary>
    /// Create a Series from a 2D matrix.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series<T>(string name, T[,] data)
        => CSharp.Series.From(name, data);

    /// <summary>
    /// Create a Series from a ReadOnlySpan (Zero allocation path).
    /// </summary>
    public static Series Series<T>(string name, ReadOnlySpan<T> data)
        => CSharp.Series.FromSpan(name, data);

    /// <summary>
    /// Materialize a single logical Expression into a physical Series.
    /// Example: Pl.Series(Pl.Lit(42).RepeatBy(5))
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series(Expr expr)
        => CSharp.Series.FromExpr(expr);
    // ---------------------------------------------------------
    // Selectors Entry Points
    // ---------------------------------------------------------

    /// <summary>
    /// String matching selectors namespace.
    /// Usage: Polars.Selectors.StartsWith("A") or Pl.Cs.StartWith("A")
    /// </summary>
    public readonly struct Selectors
    {
        /// <summary>
        /// Select all columns.
        /// </summary>
        public static Selector All() => new(PolarsWrapper.SelectorAll());
        /// <summary>
        /// Select columns.
        /// </summary>
        public static Selector ByName(params string[] columns) => new(PolarsWrapper.SelectorCols(columns));
        /// <summary>
        /// Select columns by their index. 
        /// Usage: Cs.ByIndex(0, 2, 4)
        /// </summary>
        public static Selector ByIndex(params ReadOnlySpan<long> indices) => ByIndex(indices, strict: true);

        /// <summary>
        /// Select columns by their index with strictness control.
        /// </summary>
        public static Selector ByIndex(ReadOnlySpan<long> indices, bool strict)=> new(PolarsWrapper.SelectorByIndex(indices, strict));
        /// <summary>
        /// Select columns by specific DataType.
        /// </summary>
        public static Selector ByDtype(DataType type) 
        {
            var typeKind = type.Kind;
            return new(PolarsWrapper.SelectorByDtype(typeKind.ToNative()));
        }
        /// <summary>
        /// Select columns by specific DataType.
        /// </summary>
        public static Selector ByDtype(Type type) 
        {
            var arrowType = ArrowTypeResolver.GetArrowTypeFromNetType(type);
            var plType = DataType.FromArrowType(arrowType);
            return ByDtype(plType);
        }
        /// <summary>
        /// Select columns by Generic Type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static Selector ByDtype<T>() 
            => ByDtype(typeof(T));
        /// <summary>
        /// Select all columns EXCEPT the specified Selectors.
        /// </summary>
        public static Selector Exclude(params ReadOnlySpan<Selector> selectors) => All().Exclude(selectors);
        /// <summary>
        /// Select all columns EXCEPT the specified Data Types.
        /// </summary>
        public static Selector Exclude(params ReadOnlySpan<DataType> dtypes) => All().Exclude(dtypes);
        /// <summary>
        /// Select the first column.
        /// </summary>
        public static Selector First() => ByIndex([0L]);
        /// <summary>
        /// Select the last column.
        /// </summary>
        public static Selector Last() => ByIndex([-1L]);
        /// <summary>
        /// Select all numeric columns (Int, Float, etc.).
        /// </summary>
        public static Selector Numeric()  => new(PolarsWrapper.SelectorNumeric());
        /// <summary>
        /// Select all string/utf8 columns.
        /// </summary>
        public static Selector String() => ByDtype(DataType.String);
        /// <summary>
        /// Select all date columns.
        /// </summary>
        public static Selector Date() => ByDtype(DataType.Date);
        public static Selector Boolean() => ByDtype(DataType.Boolean);
        public static Selector Binary() => ByDtype(DataType.Binary);
        public static Selector Empty() => new(PolarsWrapper.SelectorEmpty());
        public static Selector Integer() => new(PolarsWrapper.SelectorInteger());
        public static Selector UnsignedInteger() => new(PolarsWrapper.SelectorUnsignedInteger());
        public static Selector SignedInteger() => new(PolarsWrapper.SelectorSignedInteger());
        public static Selector Float() => new(PolarsWrapper.SelectorFloat());
        public static Selector Decimal() => new(PolarsWrapper.SelectorDecimal());
        public static Selector Enum() => new(PolarsWrapper.SelectorEnum());
        public static Selector Nested() => new(PolarsWrapper.SelectorNested());
        public static Selector Struct() => new(PolarsWrapper.SelectorStruct());
        public static Selector Temporal() => new(PolarsWrapper.SelectorTemporal());
        /// <summary>
        /// Select list columns. Optionally filter by the inner data type.
        /// Example: Cs.List(Cs.Integer())
        /// </summary>
        public static Selector List(Selector? inner = null) => new(PolarsWrapper.SelectorList(inner?.CloneHandle()));
        /// <summary>
        /// Select array columns. Optionally filter by inner data type and fixed width.
        /// </summary>
        public static Selector Array(Selector? inner = null, long? width = null) => new(PolarsWrapper.SelectorArray(inner?.CloneHandle(), width));
        private static PlTimeUnit GetNativeTimeUnit(TimeUnit? unit)
            => unit.HasValue ? unit.Value.ToNative() : PlTimeUnit.All;
        private static Selector DatetimeInternal(TimeUnit? timeUnit, string? tzString)
            =>new (PolarsWrapper.SelectorDatetime(GetNativeTimeUnit(timeUnit), tzString));
        /// <summary>
        /// Select all datetime columns (both with and without timezones).
        /// </summary>
        public static Selector Datetime(TimeUnit? timeUnit = null) => DatetimeInternal(timeUnit, null); // TimeZoneSet::Any
        /// <summary>
        /// Select ONLY timezone-naive datetime columns (no timezone set).
        /// </summary>
        public static Selector DatetimeNaive(TimeUnit? timeUnit = null) => DatetimeInternal(timeUnit, "");
        /// <summary>
        /// Select ONLY timezone-aware datetime columns (any timezone).
        /// </summary>
        public static Selector DatetimeAware(TimeUnit? timeUnit = null) => DatetimeInternal(timeUnit, "*"); 
        /// <summary>
        /// Select datetime columns matching a specific timezone (e.g., "UTC", "Asia/Shanghai").
        /// </summary>
        public static Selector DatetimeExact(string timeZone, TimeUnit? timeUnit = null) 
        {
            ArgumentException.ThrowIfNullOrEmpty(timeZone);
            return DatetimeInternal(timeUnit, timeZone);
        }
        /// <summary>
        /// Select all duration columns. Optionally match a specific TimeUnit.
        /// </summary>
        public static Selector Duration(TimeUnit? timeUnit = null) => new (PolarsWrapper.SelectorDuration(GetNativeTimeUnit(timeUnit)));
        /// <summary>
        /// Select column whose name starts with given prefix.
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static Selector StartsWith(string prefix) => new(PolarsWrapper.SelectorStartsWith(prefix));
        /// <summary>
        /// Select column whose name ends with given suffix.
        /// </summary>
        /// <param name="suffix"></param>
        /// <returns></returns>
        public static Selector EndsWith(string suffix) => new(PolarsWrapper.SelectorEndsWith(suffix));
        /// <summary>
        /// Select column whose name contains given string.
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static Selector Contains(string str) => new(PolarsWrapper.SelectorContains(str));
        /// <summary>
        /// Select column whose name matches given string.
        /// </summary>
        /// <param name="regex">Regular Expression</param>
        /// <returns></returns>
        public static Selector Matches(string regex) => new(PolarsWrapper.SelectorMatch(regex));
        /// <summary>
        /// Select all columns with alphabetic names.
        /// </summary>
        public static Selector Alpha(bool asciiOnly = false, bool ignoreSpaces = false)
        {
            // asciiOnly ? [a-zA-Z] : \p{L} (Unicode characters)
            string charClass = asciiOnly ? "a-zA-Z" : @"\p{L}";
            if (ignoreSpaces) charClass += " ";
            
            string pattern = $"^[{charClass}]+$";
            return Matches(pattern);
        }
        /// <summary>
        /// <para>[EN] Select columns whose names consist entirely of CJK scripts (Han, Hiragana, Katakana, Hangul).
        /// The 'chinese' option enables \p{Han}, which also includes Japanese Kanji and Korean Hanja.</para>
        /// <para>[ZH] 选择列名完全由中日韩字符（Han / 平假名 / 片假名 / 韩文）组成的列。
        /// 注意：'chinese' 实际匹配 \p{Han}，包含日文汉字与韩文汉字。</para>
        /// <para>[JA] 列名がCJK文字（漢字・ひらがな・カタカナ・ハングル）のみで構成される列を選択します。
        /// ※ 'chinese' は \p{Han}（日本・韓国の漢字を含む）を有効にします。</para>
        /// <para>[KO] 열 이름이 CJK 문자(한자, 히라가나, 가타카나, 한글)로만 구성된 열을 선택합니다.
        /// ※ 'chinese'는 \p{Han}을 의미하며 일본/한국 한자도 포함합니다.</para>
        /// </summary>
        public static Selector CJK(
            bool chinese = true, 
            bool japanese = true, 
            bool korean = true, 
            bool ignoreSpaces = false)
        {
            if (!chinese && !japanese && !korean)
                throw new ArgumentException("At least one CJK script must be enabled.");

            string charClass = "";
            
            if (chinese)  charClass += @"\p{Han}";
            if (japanese) charClass += @"\p{Hiragana}\p{Katakana}";
            if (korean)   charClass += @"\p{Hangul}";
            
            if (ignoreSpaces) charClass += " ";

            string pattern = $"^[{charClass}]+$";
            return Matches(pattern);
        }
        /// <summary>
        /// <para>[EN] Select columns whose names consist of CJK scripts, Unicode digits (\p{N}),
        /// and optionally ASCII/full-width Latin letters.</para>
        /// <para>[ZH] 选择列名由中日韩字符、数字（\p{N}，含全/半角）以及可选英文字母（全/半角）组成的列。</para>
        /// <para>[JA] 列名がCJK文字・数字（\p{N}、全角/半角）および英字（全角/半角）で構成される列を選択します。</para>
        /// <para>[KO] 열 이름이 CJK 문자, 숫자(\p{N}, 전각/반각) 및 영문자(전각/반각)로 구성된 열을 선택합니다.</para>
        /// </summary>
        public static Selector CJKAlphanumeric(
            bool chinese = true, 
            bool japanese = true, 
            bool korean = true, 
            bool includeLetters = true, 
            bool ignoreSpaces = false)
        {
            if (!chinese && !japanese && !korean)
                throw new ArgumentException("At least one CJK script must be enabled.");

            string charClass = @"\p{N}"; 
            
            if (includeLetters) charClass += "a-zA-ZＡ-Ｚａ-ｚ";
            if (chinese)  charClass += @"\p{Han}";
            if (japanese) charClass += @"\p{Hiragana}\p{Katakana}";
            if (korean)   charClass += @"\p{Hangul}";
            
            if (ignoreSpaces) charClass += " ";

            string pattern = $"^[{charClass}]+$";
            return Matches(pattern);
        }

        /// <summary>
        /// Select all columns with alphanumeric names.
        /// </summary>
        public static Selector Alphanumeric(bool asciiOnly = false, bool ignoreSpaces = false)
        {
            // asciiOnly ? [a-zA-Z0-9] : [\p{L}\p{N}]
            string charClass = asciiOnly ? "a-zA-Z0-9" : @"\p{L}\p{N}";
            if (ignoreSpaces) charClass += " ";

            string pattern = $"^[{charClass}]+$";
            return Matches(pattern);
        }
        /// <summary>
        /// Expand a Selector against a DataFrame to get the matched column names.
        /// </summary>
        public static string[] ExpandSelector(DataFrame target, Selector selector)
        {
            ArgumentNullException.ThrowIfNull(target);
            ArgumentNullException.ThrowIfNull(selector);

            using var emptyDf = target.Clear(); 
            
            using var result = emptyDf.Select(selector);
            
            return [.. result.Columns];
        }

        /// <summary>
        /// Expand an Expr against a DataFrame to get the matched column names.
        /// </summary>
        public static string[] ExpandSelector(DataFrame target, Expr expr)
        {
            ArgumentNullException.ThrowIfNull(target);
            ArgumentNullException.ThrowIfNull(expr);

            using var emptyDf = target.Clear();
            using var result = emptyDf.Select(expr);
            return [.. result.Columns];
        }
        public static string[] ExpandSelector(LazyFrame target, Selector selector)
           => [.. target.Select(selector).Schema.Names];
        
    }
}

internal static class InterfaceUnwrapperExtensions
{
    /// <summary>
    /// Unwrap IPolarsDataFrame as DataFrame 
    /// </summary>
    internal static DataFrame AsDataFrame(this IPolarsDataFrame idf)
    {
        if (idf is DataFrame df)
            return df;
        
        throw new InvalidCastException("Not Standard Polars DataFrame");
    }
    /// <summary>
    /// Unwrap IPolarsLazyFrame as LazyFrame 
    /// </summary>
    /// <param name="ilf">A IPolarsLazyFrame</param>
    /// <returns></returns>
    /// <exception cref="InvalidCastException"></exception>
    internal static LazyFrame AsLazyFrame(this IPolarsLazyFrame ilf)
    {
        if (ilf is LazyFrame lf)
            return lf;
        
        throw new InvalidCastException("Not Standard Polars LazyFrame");
    }
    /// <summary>
    /// Unwrap IPolarsLazyFrame as LazyFrame 
    /// </summary>
    /// <param name="iS">A IPolarsSeries</param>
    /// <returns></returns>
    /// <exception cref="InvalidCastException"></exception>
    internal static Series AsSeries(this IPolarsSeries iS)
    {
        if (iS is Series s)
            return s;
        
        throw new InvalidCastException("Not Standard Polars Series");
    }
}