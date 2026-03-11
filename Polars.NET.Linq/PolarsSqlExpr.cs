#pragma warning disable CS1591 
using System.Collections.Concurrent;
using System.Linq.Expressions;
using LinqToDB;
using LinqToDB.Internal.Linq;

namespace Polars.NET.Linq;

internal static class PolarsSqlTranslator
{
    private static readonly ConcurrentDictionary<Type, string[]> _aliasCache = new();

    public static string[] Translate<T, TResult>(PolarsDataContext db, Expression<Func<T, TResult>> expr) where T : class
    {
        var dummyQuery = db.GetTable<T>().Select(expr);
        var sqlQueries = ((IExpressionQuery)dummyQuery).GetSqlQueries(null);
        var fullSql = sqlQueries[0].Sql;

        var rawSnippet = SqlSanitizer.ExtractSelectSnippet(fullSql);
        var cleanSnippet = SqlSanitizer.RemoveTableAliases(rawSnippet);
        var snippets = SplitSqlExpressions(cleanSnippet);

        Expression body = expr.Body;

        while (body.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs)
        {
            body = ((UnaryExpression)body).Operand;
        }

        string[]? aliases = null; 

        if (body is NewExpression newExpr && newExpr.Members != null && newExpr.Members.Count == snippets.Length)
        {
            // C# Anonymous Object
            aliases = new string[snippets.Length];
            for (int i = 0; i < snippets.Length; i++) aliases[i] = newExpr.Members[i].Name;
        }
        else
        {
            // F# AnonRecord 
            var type = body.Type;
            if (type.Name.Contains("AnonymousType") || type.Name.Contains("AnonymousObject") || type.Name.Contains("AnonRecord"))
            {
                var cachedAliases = _aliasCache.GetOrAdd(type, ExtractRealColumnNames); 
                
                if (cachedAliases.Length == snippets.Length)
                {
                    aliases = cachedAliases; 
                }
            }
        }

        if (aliases != null)
        {
            for (int i = 0; i < snippets.Length; i++)
            {
                ReadOnlySpan<char> strippedSpan = SqlSanitizer.RemoveTrailingAliasSpan(snippets[i].AsSpan());
                snippets[i] = string.Concat(strippedSpan, " AS \"", aliases[i].AsSpan(), "\"");
            }
        }
        else
        {
            for (int i = 0; i < snippets.Length; i++)
            {
                snippets[i] = SqlSanitizer.RemoveTrailingAliasSpan(snippets[i].AsSpan()).ToString();
            }
        }
        
        return snippets;
    }
    public static string InjectAliases(string fullSql, Type elementType)
    {
        var aliases = _aliasCache.GetOrAdd(elementType, ExtractRealColumnNames);
        if (aliases.Length == 0) return fullSql;

        var rawSnippet = SqlSanitizer.ExtractSelectSnippet(fullSql);
        var snippets = SplitSqlExpressions(rawSnippet);

        if (snippets.Length == aliases.Length)
        {
            int snippetStartIndex = fullSql.IndexOf(rawSnippet, StringComparison.Ordinal);
            if (snippetStartIndex < 0) return fullSql;

            // stackalloc here
            Span<char> initialBuffer = stackalloc char[Math.Min(fullSql.Length + 64, 512)]; 
            var vsb = new ValueStringBuilder(initialBuffer);

            vsb.Append(fullSql.AsSpan(0, snippetStartIndex));

            for (int i = 0; i < snippets.Length; i++)
            {
                ReadOnlySpan<char> strippedSpan = SqlSanitizer.RemoveTrailingAliasSpan(snippets[i].AsSpan());
                vsb.Append(strippedSpan);      
                vsb.Append(" AS \"");          
                vsb.Append(aliases[i]);        
                vsb.Append('"');

                if (i < snippets.Length - 1)
                {
                    vsb.Append(",\n        "); 
                }
            }

            int snippetEndIndex = snippetStartIndex + rawSnippet.Length;
            vsb.Append(fullSql.AsSpan(snippetEndIndex));

            // 生成最终字符串，并内部调用 Dispose 还给 ArrayPool
            return vsb.ToString(); 
        }
        
        return fullSql; 
    }

    private static string[] ExtractRealColumnNames(Type t)
    {
        if (t.Name.Contains("AnonymousType") || t.Name.Contains("AnonymousObject"))
        {
            var ctor = t.GetConstructors().FirstOrDefault();
            if (ctor != null) return ctor.GetParameters().Select(p => p.Name!).ToArray();
        }
        
        if (t.Name.Contains("AnonRecord"))
        {
            var namePart = t.Name;
            int backtickIdx = namePart.IndexOf('`');
            if (backtickIdx > 0) namePart = namePart.Substring(0, backtickIdx); 
            
            var prefix = "<>f__AnonymousRecord_";
            if (namePart.StartsWith(prefix))
            {
                var fieldsPart = namePart.Substring(prefix.Length); 
                var parts = fieldsPart.Split('_', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length > 0) return parts; 
            }
        }

        return []; 
    }
    // ====================================================================
    // Split string by span
    // ====================================================================
    private static string[] SplitSqlExpressions(string sql)
    {
        ReadOnlySpan<char> span = sql.AsSpan();
        
        // ==========================================
        // Pass 1: Pre-Scan
        // ==========================================
        int segmentCount = 1;
        int parens = 0;
        bool inSingleQuote = false;
        bool inDoubleQuote = false;

        foreach (char c in span)
        {
            if (c == '\'') inSingleQuote = !inSingleQuote;
            else if (c == '"') inDoubleQuote = !inDoubleQuote;
            else if (!inSingleQuote && !inDoubleQuote) 
            {
                if (c == '(') parens++;
                else if (c == ')') parens--;
                else if (c == ',' && parens == 0) segmentCount++;
            }
        }

        string[] result = new string[segmentCount];
        
        // ==========================================
        // Pass 2: Slice without alloc
        // ==========================================
        parens = 0;
        inSingleQuote = false;
        inDoubleQuote = false;
        int lastSplit = 0;
        int currentIndex = 0;

        for (int i = 0; i < span.Length; i++)
        {
            char c = span[i];
            
            if (c == '\'') inSingleQuote = !inSingleQuote;
            else if (c == '"') inDoubleQuote = !inDoubleQuote;
            else if (!inSingleQuote && !inDoubleQuote)
            {
                if (c == '(') parens++;
                else if (c == ')') parens--;
                else if (c == ',' && parens == 0)
                {
                    result[currentIndex++] = span[lastSplit..i].Trim().ToString();
                    lastSplit = i + 1;
                }
            }
        }
        
        result[currentIndex] = span[lastSplit..].Trim().ToString();
        
        return result;
    }
}
/// <summary>
/// Provides utility methods to translate standalone .NET Lambda expressions into Polars-compatible SQL snippets.
/// </summary>
/// <remarks>
/// This class is useful for scenarios where you need to generate SQL fragments from strongly-typed expressions 
/// without executing a full LINQ query. For example, generating a SQL string to pass into <c>Expr.SqlExpr()</c>.
/// </remarks>
public static class PolarsExpr
{
    private static readonly Lazy<PolarsDataContext> DummyDb = new(() => 
        new PolarsDataContext(null!)); 

    /// <summary>
    /// Translates a single .NET Lambda expression into its corresponding SQL string.
    /// </summary>
    /// <typeparam name="T">The input entity type used in the expression.</typeparam>
    /// <typeparam name="TResult">The return type of the expression.</typeparam>
    /// <param name="expr">The Lambda expression to translate (e.g., <c>p => p.Price * 1.2</c>).</param>
    /// <returns>A SQL string snippet representing the logic within the expression.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the expression translates into multiple SQL fragments. 
    /// In such cases, use <see cref="ToSqls{T, TResult}"/> instead.
    /// </exception>
    public static string ToSql<T, TResult>(Expression<Func<T, TResult>> expr) where T : class
    {
        var sqls = PolarsSqlTranslator.Translate(DummyDb.Value, expr);
        if (sqls.Length != 1) throw new InvalidOperationException("Expressions Translated to multiple SQLs, Please use ToSqls() instead.");
        return sqls[0];
    }

    /// <summary>
    /// Translates a .NET Lambda expression into an array of SQL string fragments.
    /// Useful for expressions that return complex types or multiple columns.
    /// </summary>
    /// <typeparam name="T">The input entity type.</typeparam>
    /// <typeparam name="TResult">The return type of the expression.</typeparam>
    /// <param name="expr">The Lambda expression to translate.</param>
    /// <returns>An array of SQL string snippets.</returns>
    public static string[] ToSqls<T, TResult>(Expression<Func<T, TResult>> expr) where T : class
        =>PolarsSqlTranslator.Translate(DummyDb.Value, expr);
}

/// <summary>
/// Provides strongly-typed mappings to Polars-specific SQL functions.
/// These methods are strictly for use within LINQ queries and will throw if executed in memory.
/// </summary>
public static class PolarsSql
{
    // ==========================================
    // 1. Array / Nested List 
    // ==========================================
    
    /// <summary>
    /// Aggregates a sequence of strings into a single comma-separated string using Polars ARRAY_AGG and ARRAY_TO_STRING.
    /// </summary>
    [Sql.Extension("ARRAY_TO_STRING(ARRAY_AGG({selector}), ', ')", IsAggregate = true, ServerSideOnly = true)]
    public static string ListAgg<T>(this IEnumerable<T> source, [ExprParameter] Expression<Func<T, string>> selector)
        => throw new InvalidOperationException("[Polars.NET] ListAgg can only be used within a LINQ to Polars query.");

    // ==========================================
    // 2. Regex
    // ==========================================
    
    /// <summary>
    /// Performs a regular expression match using the Polars REGEXP operator.
    /// </summary>
    [Sql.Expression("{0} REGEXP {1}", ServerSideOnly = true)]
    public static bool RegexMatch(string input, string pattern)
        => throw new InvalidOperationException("[Polars.NET] RegexMatch can only be used within a LINQ to Polars query.");

    // ==========================================
    // 3. STAT 
    // ==========================================
    
    /// <summary>
    /// Calculates the sample variance using Polars VARIANCE.
    /// </summary>
    [Sql.Expression("VARIANCE({1})", ServerSideOnly = true, IsAggregate = true)]
    public static double Variance<T>(this IEnumerable<T> source, [ExprParameter] Expression<Func<T, double>> selector)
        => throw new InvalidOperationException("[Polars.NET] Variance can only be used within a LINQ to Polars query.");

    /// <summary>
    /// Calculates the continuous quantile (interpolated) using Polars QUANTILE_CONT.
    /// </summary>
    [Sql.Expression("QUANTILE_CONT({1}, {2})", ServerSideOnly = true, IsAggregate = true)]
    public static double QuantileCont<T>(this IEnumerable<T> source, [ExprParameter] Expression<Func<T, double>> selector, double quantile)
        => throw new InvalidOperationException("[Polars.NET] QuantileCont can only be used within a LINQ to Polars query.");

    /// <summary>
    /// Calculates the discrete quantile (exact value from data) using Polars QUANTILE_DISC.
    /// </summary>
    [Sql.Expression("QUANTILE_DISC({1}, {2})", ServerSideOnly = true, IsAggregate = true)]
    public static double QuantileDisc<T>(this IEnumerable<T> source, [ExprParameter] Expression<Func<T, double>> selector, double quantile)
        => throw new InvalidOperationException("[Polars.NET] QuantileDisc can only be used within a LINQ to Polars query.");
    // ==========================================
    // 3. BITWISE
    // ==========================================
    /// <summary>
    /// Calculates the bitwise XOR using BIT_XOR.
    /// </summary>
    [Sql.Expression("BIT_XOR({0}, {1})", ServerSideOnly = true)]
    public static int BitXor(int a, int b) 
        => throw new InvalidOperationException("Only for LINQ to Polars.");
    // /// <summary>
    // /// Calculates the bitwise count(the number of 1) using BIT_COUNT.
    // /// </summary>
    // [Sql.Expression("BIT_COUNT({0})", ServerSideOnly = true)]
    // public static int BitCount(int a) 
    //     => throw new InvalidOperationException("Only for LINQ to Polars.");

    // ==========================================
    // Degree <=> Radians
    // ==========================================
    [Sql.Function("DEGREES", ServerSideOnly = true)]
    public static double Degrees(double radians) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Function("RADIANS", ServerSideOnly = true)]
    public static double Radians(double degrees) => throw new InvalidOperationException("Only for LINQ to Polars.");

    // ==========================================
    // Cot
    // ==========================================
    [Sql.Function("COT", ServerSideOnly = true)]
    public static double Cot(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Function("COTD", ServerSideOnly = true)]
    public static double Cotd(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    // ==========================================
    // Degree Trag
    // ==========================================
    [Sql.Function("SIND", ServerSideOnly = true)]
    public static double Sind(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Function("COSD", ServerSideOnly = true)]
    public static double Cosd(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Function("TAND", ServerSideOnly = true)]
    public static double Tand(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    // ==========================================
    // Degree Arc Trag
    // ==========================================
    [Sql.Function("ACOSD", ServerSideOnly = true)]
    public static double Acosd(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Function("ASIND", ServerSideOnly = true)]
    public static double Asind(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Function("ATAND", ServerSideOnly = true)]
    public static double Atand(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Function("ATAN2D", ServerSideOnly = true)]
    public static double Atan2d(double y, double x) => throw new InvalidOperationException("Only for LINQ to Polars.");

    // ==========================================
    // Base Math
    // ==========================================
    
    [Sql.Expression("MOD({0}, {1})", ServerSideOnly = true)]
    public static T Mod<T>(T a, T b) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Expression("CEIL({0})", ServerSideOnly = true)]
    public static T Ceil<T>(T value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Expression("ROUND({0}, {1})", ServerSideOnly = true)]
    public static T Round<T>(T value, int decimals) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Function("DIV", ServerSideOnly = true)]
    public static long Div(long a, long b) => throw new InvalidOperationException("Only for LINQ to Polars.");

    // ==========================================
    // Cbrt
    // ==========================================

    [Sql.Function("CBRT", ServerSideOnly = true)]
    public static double Cbrt(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    // ==========================================
    // Log
    // ==========================================
    
    [Sql.Function("LOG10", ServerSideOnly = true)]
    public static double Log10(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Function("LOG2", ServerSideOnly = true)]
    public static double Log2(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");

    [Sql.Function("LOG1P", ServerSideOnly = true)]
    public static double Log1p(double value) => throw new InvalidOperationException("Only for LINQ to Polars.");
    
    // ==========================================
    // Constant Generator
    // ==========================================
    
    [Sql.Function("PI", ServerSideOnly = true)]
    public static double Pi() => throw new InvalidOperationException("Only for LINQ to Polars.");
    // ==========================================
    // Array Functions
    // ==========================================
    
    /// <summary>
    /// Aggregate Columnar data into Array as ARRAY_AGG(column)
    /// </summary>
    [Sql.Extension("ARRAY_AGG({selector})", IsAggregate = true, ServerSideOnly = true)]
    public static TResult[] ArrayAgg<TSource, TResult>(
        this IEnumerable<TSource> source, 
        [ExprParameter] Expression<Func<TSource, TResult>> selector)
        => throw new InvalidOperationException("[Polars.NET] ArrayAgg is only for LINQ to Polars.");

    // ==========================================
    // Array Query
    // ==========================================

    /// <summary>
    /// Check whether array contains element: ARRAY_CONTAINS(array, item)
    /// </summary>
    [Sql.Expression("ARRAY_CONTAINS({0}, {1})", ServerSideOnly = true)]
    public static bool ArrayContains<T>(this IEnumerable<T> array, T item)
        => throw new InvalidOperationException();

    /// <summary>
    /// Get array element by index: ARRAY_GET(array, index)
    /// </summary>
    [Sql.Expression("ARRAY_GET({0}, {1})", ServerSideOnly = true)]
    public static T ArrayGet<T>(this IEnumerable<T> array, int index)
        => throw new InvalidOperationException();

    /// <summary>
    /// Get the length of array: ARRAY_LENGTH(array)
    /// </summary>
    [Sql.Expression("ARRAY_LENGTH({0})", ServerSideOnly = true)]
    public static int ArrayLength<T>(this IEnumerable<T> array)
        => throw new InvalidOperationException();

    /// <summary>
    /// Returns the lower bound (min value) in an array: ARRAY_LOWER(array)
    /// </summary>
    [Sql.Expression("ARRAY_LOWER({0})", ServerSideOnly = true)]
    public static T ArrayMin<T>(this IEnumerable<T> array)
        => throw new InvalidOperationException();

    /// <summary>
    /// Returns the upper bound (max value) in an array: ARRAY_UPPER(array)
    /// </summary>
    [Sql.Expression("ARRAY_UPPER({0})", ServerSideOnly = true)]
    public static T ArrayMax<T>(this IEnumerable<T> array)
        => throw new InvalidOperationException();
    /// <summary>
    /// Array mean value: ARRAY_MEAN(array)
    /// </summary>
    [Sql.Expression("ARRAY_MEAN({0})", ServerSideOnly = true)]
    public static double ArrayMean<T>(this IEnumerable<T> array)
        => throw new InvalidOperationException();

    /// <summary>
    /// Array sum: ARRAY_SUM(array)
    /// </summary>
    [Sql.Expression("ARRAY_SUM({0})", ServerSideOnly = true)]
    public static T ArraySum<T>(this IEnumerable<T> array)
        => throw new InvalidOperationException();
    /// <summary>
    /// Reverse the array: ARRAY_REVERSE(array)
    /// </summary>
    [Sql.Expression("ARRAY_REVERSE({0})", ServerSideOnly = true)]
    public static T[] ArrayReverse<T>(this IEnumerable<T> array)
        => throw new InvalidOperationException();

    /// <summary>
    /// Array to string: ARRAY_TO_STRING(array, separator)
    /// </summary>
    [Sql.Expression("ARRAY_TO_STRING({0}, {1})", ServerSideOnly = true)]
    public static string ArrayToString<T>(this IEnumerable<T> array, string separator)
        => throw new InvalidOperationException();

    /// <summary>
    /// Deduplicate the array: ARRAY_UNIQUE(array)
    /// </summary>
    [Sql.Expression("ARRAY_UNIQUE({0})", ServerSideOnly = true)]
    public static T[] ArrayUnique<T>(this IEnumerable<T> array)
        => throw new InvalidOperationException();

    // ==========================================
    // UNNEST
    // ==========================================

    /// <summary>
    /// Explode the array (1 row becomes N rows): UNNEST(array)
    /// </summary>
    [Sql.Expression("UNNEST({0})", ServerSideOnly = true)]
    public static T Unnest<T>(this IEnumerable<T> array)
        => throw new InvalidOperationException();
}