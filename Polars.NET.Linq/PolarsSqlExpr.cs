#pragma warning disable CS1591 
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

public static class PolarsExpr
{
    private static readonly Lazy<PolarsDataContext> DummyDb = new(() => 
        new PolarsDataContext(null!)); 

    /// <summary>
    /// Translate Single Expression to SQL
    /// </summary>
    public static string ToSql<T, TResult>(Expression<Func<T, TResult>> expr) where T : class
    {
        var sqls = PolarsSqlTranslator.Translate(DummyDb.Value, expr);
        if (sqls.Length != 1) throw new InvalidOperationException("Expressions Translated to multiple SQLs, Please use ToSqls() instead.");
        return sqls[0];
    }

    /// <summary>
    /// Translate Multiple Expressions to SQL
    /// </summary>
    public static string[] ToSqls<T, TResult>(Expression<Func<T, TResult>> expr) where T : class
        =>PolarsSqlTranslator.Translate(DummyDb.Value, expr);
}
