using Polars.NET.Linq;

namespace Polars.CSharp.Tests;

public class LinqReflectionCacheTests
{
    [Fact]
    [Trait("LINQ", "ReflectionCache")]
    public void Enumerable_And_Queryable_Methods_With_Same_Signature_Are_Cached_Separately()
    {
        // Same name, arity and type arguments on both Enumerable and Queryable: a shared cache key
        // would hand back whichever declaring type was resolved first.
        var enumerableCount = LinqReflectionCache.GetEnumerableMethod("Count", 1, [typeof(int)]);
        var queryableCount = LinqReflectionCache.GetQueryableMethod("Count", 1, [typeof(int)]);

        Assert.Equal(typeof(Enumerable), enumerableCount.DeclaringType);
        Assert.Equal(typeof(Queryable), queryableCount.DeclaringType);
        Assert.Equal(typeof(Enumerable), LinqReflectionCache.GetEnumerableMethod("Count", 1, [typeof(int)]).DeclaringType);
    }
}
