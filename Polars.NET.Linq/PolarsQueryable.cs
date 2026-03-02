using System.Collections;
using System.Linq.Expressions;

namespace Polars.NET.Linq;

/// <summary>
/// Container for LINQ AST
/// </summary>
public class PolarsQueryable<T> : IOrderedQueryable<T>
{
    public string? TableName { get; }
    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider { get; }

    public PolarsQueryable(IQueryProvider provider, string tableName)
    {
        Provider = provider;
        Expression = Expression.Constant(this);
        TableName = tableName;
    }

    public PolarsQueryable(IQueryProvider provider, Expression expression)
    {
        Provider = provider;
        Expression = expression;
    }

    public IEnumerator<T> GetEnumerator()
    {
        return Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}