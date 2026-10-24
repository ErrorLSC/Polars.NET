using System.Linq.Expressions;
using Polars.NET.Linq;

namespace Polars.CSharp.Tests;

public class QueryCacheKeyTests
{
    private class Person
    {
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    private class Order
    {
        public int Id { get; set; }
        public decimal Amount { get; set; }
    }

    [Fact]
    [Trait("LINQ", "QueryCacheKey")]
    public void AlphaEquivalence_DifferentParameterNames_AreEqual()
    {
        Expression<Func<int, int>> expr1 = x => x + 1;
        Expression<Func<int, int>> expr2 = y => y + 1;

        var key1 = QueryCacheKey.Create(typeof(int), expr1);
        var key2 = QueryCacheKey.Create(typeof(int), expr2);

        Assert.Equal(key1, key2);
        Assert.Equal(key1.GetHashCode(), key2.GetHashCode());
    }

    [Fact]
    [Trait("LINQ", "QueryCacheKey")]
    public void AlphaEquivalence_MultiParameter_AreEqual()
    {
        Expression<Func<int, int, int>> expr1 = (a, b) => (a * 2) + b;
        Expression<Func<int, int, int>> expr2 = (x, y) => (x * 2) + y;

        var key1 = QueryCacheKey.Create(typeof(int), expr1);
        var key2 = QueryCacheKey.Create(typeof(int), expr2);

        Assert.Equal(key1, key2);
        Assert.Equal(key1.GetHashCode(), key2.GetHashCode());
    }

    [Fact]
    [Trait("LINQ", "QueryCacheKey")]
    public void ExtractedParameters_SameNormalizedShape_AreEqual()
    {
        var threshold1 = 25;
        var threshold2 = 50;

        Expression<Func<Person, bool>> expr1 = p => p.Age > threshold1;
        Expression<Func<Person, bool>> expr2 = p => p.Age > threshold2;

        var extracted1 = PartialEvaluator.extract(expr1);
        var extracted2 = PartialEvaluator.extract(expr2);

        var key1 = QueryCacheKey.Create(typeof(Person), extracted1.NormalizedExpression);
        var key2 = QueryCacheKey.Create(typeof(Person), extracted2.NormalizedExpression);

        Assert.Equal(key1, key2);
        Assert.Equal(key1.GetHashCode(), key2.GetHashCode());
    }

    [Fact]
    [Trait("LINQ", "QueryCacheKey")]
    public void DifferentConstants_AreNotEqual()
    {
        Expression<Func<int, int>> expr1 = x => x + 1;
        Expression<Func<int, int>> expr2 = x => x + 2;

        var key1 = QueryCacheKey.Create(typeof(int), expr1);
        var key2 = QueryCacheKey.Create(typeof(int), expr2);

        Assert.NotEqual(key1, key2);
    }

    [Fact]
    [Trait("LINQ", "QueryCacheKey")]
    public void DifferentOperations_AreNotEqual()
    {
        Expression<Func<int, int>> expr1 = x => x + 1;
        Expression<Func<int, int>> expr2 = x => x - 1;

        var key1 = QueryCacheKey.Create(typeof(int), expr1);
        var key2 = QueryCacheKey.Create(typeof(int), expr2);

        Assert.NotEqual(key1, key2);
    }

    [Fact]
    [Trait("LINQ", "QueryCacheKey")]
    public void DifferentSourceTypes_AreNotEqual()
    {
        Expression<Func<int, int>> expr = x => x;

        var key1 = QueryCacheKey.Create(typeof(Person), expr);
        var key2 = QueryCacheKey.Create(typeof(Order), expr);

        Assert.NotEqual(key1, key2);
    }

    [Fact]
    [Trait("LINQ", "QueryCacheKey")]
    public void QueryableRoots_SameElementType_AreEqual()
    {
        IQueryable<Person> q1 = new List<Person>().AsQueryable();
        IQueryable<Person> q2 = new List<Person>().AsQueryable();

        var expr1 = Expression.Constant(q1, typeof(IQueryable<Person>));
        var expr2 = Expression.Constant(q2, typeof(IQueryable<Person>));

        var key1 = QueryCacheKey.Create(typeof(Person), expr1);
        var key2 = QueryCacheKey.Create(typeof(Person), expr2);

        Assert.Equal(key1, key2);
        Assert.Equal(key1.GetHashCode(), key2.GetHashCode());
    }

    [Fact]
    [Trait("LINQ", "QueryCacheKey")]
    public void ComplexQuery_NormalizedParameterExtraction_CacheHit()
    {
        var brand1 = "Brand#23";
        var container1 = "MED BOX";

        var brand2 = "Brand#45";
        var container2 = "SM PKG";

        Expression<Func<Person, bool>> q1 = p => p.Name == brand1 && p.Age > container1.Length;
        Expression<Func<Person, bool>> q2 = p => p.Name == brand2 && p.Age > container2.Length;

        var ext1 = PartialEvaluator.extract(q1);
        var ext2 = PartialEvaluator.extract(q2);

        var key1 = QueryCacheKey.Create(typeof(Person), ext1.NormalizedExpression);
        var key2 = QueryCacheKey.Create(typeof(Person), ext2.NormalizedExpression);

        Assert.Equal(key1, key2);
        Assert.Equal(key1.GetHashCode(), key2.GetHashCode());
    }
}
