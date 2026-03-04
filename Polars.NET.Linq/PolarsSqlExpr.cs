using System.Linq.Expressions;
using LinqToDB;
using LinqToDB.Internal.Linq;

namespace Polars.NET.Linq;

internal static class PolarsSqlTranslator
{

    public static string[] Translate<T, TResult>(PolarsDataContext db, Expression<Func<T, TResult>> expr) where T : class
    {
        var dummyQuery = db.GetTable<T>().Select(expr);
        var sqlQueries = ((IExpressionQuery)dummyQuery).GetSqlQueries(null);
        var fullSql = sqlQueries[0].Sql;

        var rawSnippet = SqlSanitizer.ExtractSelectSnippet(fullSql);
        var cleanSnippet = SqlSanitizer.RemoveTableAliases(rawSnippet);
        
        var snippets = SplitSqlExpressions(cleanSnippet);

        if (expr.Body is NewExpression newExpr && newExpr.Members != null)
        {
            for (int i = 0; i < snippets.Length; i++)
            {
                var stripped = SqlSanitizer.RemoveTrailingAlias(snippets[i]);
                
                snippets[i] = $"{stripped} AS {newExpr.Members[i].Name}";
            }
        }
        else
        {
            for (int i = 0; i < snippets.Length; i++)
            {
                snippets[i] = SqlSanitizer.RemoveTrailingAlias(snippets[i]);
            }
        }
        return snippets;
    }

    private static string[] SplitSqlExpressions(string sql)
    {
        var result = new List<string>();
        int parens = 0, lastSplit = 0;
        for (int i = 0; i < sql.Length; i++)
        {
            if (sql[i] == '(') parens++;
            else if (sql[i] == ')') parens--;
            else if (sql[i] == ',' && parens == 0)
            {
                result.Add(sql[lastSplit..i].Trim());
                lastSplit = i + 1;
            }
        }
        result.Add(sql[lastSplit..].Trim());
        return [.. result];
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
