using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Polars.CSharp.Linq;
using Polars.NET.Core;
using Polars.NET.Linq;
using Xunit;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

[Collection("QueryPlanCacheTests")]
public class QueryPlanCacheTests
{
    private class Customer
    {
        public int Id { get; set; }
        public string City { get; set; } = "";
        public decimal Balance { get; set; }
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheHits_MultipleExecutionsWithDifferentParameters_IncrementsHitsAndReturnsCorrectResults()
    {
        using var df = DataFrame.FromRows(
        [
            new Customer { Id = 1, City = "Beijing", Balance = 100m },
            new Customer { Id = 2, City = "Shanghai", Balance = 200m },
            new Customer { Id = 3, City = "Tokyo", Balance = 300m },
        ]);

        IQueryable<Customer> FilterByCity(string city)
        {
            return df.AsQueryable<Customer>().Where(c => c.City == city);
        }

        // Run 1: First time compilation (Cache Miss)
        var q1 = FilterByCity("Beijing");
        var res1 = q1.ToList();
        _ = Assert.Single(res1);
        Assert.Equal("Beijing", res1[0].City);

        var ext1 = PartialEvaluator.extract(q1.Expression);
        var key1 = QueryCacheKey.Create(typeof(Customer), ext1.NormalizedExpression);
        bool found1 = QueryPlanCache.TryGetPlan(key1, out var cachedPlan);
        Assert.True(found1);
        Assert.NotNull(cachedPlan);

        // Run 2: Second execution with different parameter (Cache Hit!)
        var q2 = FilterByCity("Shanghai");
        var ext2 = PartialEvaluator.extract(q2.Expression);
        var key2 = QueryCacheKey.Create(typeof(Customer), ext2.NormalizedExpression);
        Assert.Equal(key1, key2);
        Assert.Equal(key1.GetHashCode(), key2.GetHashCode());

        var res2 = q2.ToList();
        _ = Assert.Single(res2);
        Assert.Equal("Shanghai", res2[0].City);

        // Run 3: Third execution with different parameter (Cache Hit!)
        var q3 = FilterByCity("Tokyo");
        var ext3 = PartialEvaluator.extract(q3.Expression);
        var key3 = QueryCacheKey.Create(typeof(Customer), ext3.NormalizedExpression);
        Assert.Equal(key1, key3);

        var res3 = q3.ToList();
        _ = Assert.Single(res3);
        Assert.Equal("Tokyo", res3[0].City);
        Assert.True(QueryPlanCache.Hits > 0);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheMiss_DifferentQueryStructure_AddsNewKeys()
    {
        using var df = DataFrame.FromRows(
        [
            new Customer { Id = 1, City = "Beijing", Balance = 100m },
            new Customer { Id = 2, City = "Shanghai", Balance = 200m },
        ]);

        int initialCount = QueryPlanCache.Count;

        // Query 1: Where City == param
        const string city = "Beijing";
        var q1 = df.AsQueryable<Customer>().Where(c => c.City == city).ToList();

        // Query 2: Where Balance > param (different property & condition)
        const decimal minBalance = 150m;
        var q2 = df.AsQueryable<Customer>().Where(c => c.Balance > minBalance).ToList();

        // Query 3: Where City == param and Balance > param
        var q3 = df.AsQueryable<Customer>().Where(c => c.City == city && c.Balance > minBalance).ToList();

        Assert.True(QueryPlanCache.Count >= initialCount + 2);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheDisabled_BypassesCache()
    {
        QueryPlanCache.Clear();
        QueryPlanCache.Enabled = false;

        try
        {
            using var df = DataFrame.FromRows(
            [
                new Customer { Id = 1, City = "Beijing", Balance = 100m },
            ]);

            const string city1 = "Beijing";
            var q1 = df.AsQueryable<Customer>().Where(c => c.City == city1).ToList();

            const string city2 = "Tokyo";
            var q2 = df.AsQueryable<Customer>().Where(c => c.City == city2).ToList();

            Assert.Equal(0, QueryPlanCache.Hits);
            Assert.Equal(0, QueryPlanCache.Count);
        }
        finally
        {
            QueryPlanCache.Enabled = true;
        }
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheCapacityAndEviction_ExceedingLimit_PrunesOldestEntries()
    {
        QueryPlanCache.Clear();
        int originalCapacity = QueryPlanCache.MaxCapacity;

        try
        {
            // Set max capacity to 5
            QueryPlanCache.MaxCapacity = 5;
            Assert.Equal(5, QueryPlanCache.MaxCapacity);

            using var df = DataFrame.FromRows(
            [
                new Customer { Id = 1, City = "Beijing", Balance = 100m },
            ]);

            // Insert 6 structurally different queries
            _ = df.AsQueryable<Customer>().Where(c => c.Id == 1).ToList();
            _ = df.AsQueryable<Customer>().Where(c => c.Id > 1).ToList();
            _ = df.AsQueryable<Customer>().Where(c => c.Id < 1).ToList();
            _ = df.AsQueryable<Customer>().Where(c => c.Id >= 1).ToList();
            _ = df.AsQueryable<Customer>().Where(c => c.Id <= 1).ToList();

            Assert.True(QueryPlanCache.Count <= 5);

            // Adding a 6th unique query triggers prune
            _ = df.AsQueryable<Customer>().Where(c => c.Id != 1).ToList();

            // After prune of oldest 20%, cache should remain <= 5
            Assert.True(QueryPlanCache.Count <= 5);
        }
        finally
        {
            QueryPlanCache.MaxCapacity = originalCapacity;
            QueryPlanCache.Clear();
        }
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void PolarsConfig_SetQueryCacheSettings_AppliesSuccessfully()
    {
        int originalCapacity = QueryPlanCache.MaxCapacity;
        bool originalEnabled = QueryPlanCache.Enabled;

        try
        {
            using (Pl.Config.SetQueryCacheCapacity(256).BeginScope())
            {
                Assert.Equal(256, QueryPlanCache.MaxCapacity);
                Assert.Equal("256", Pl.Config["POLARS_QUERY_CACHE_CAPACITY"]);
            }
            // After scope disposal, it restores previous value
            Assert.Equal(originalCapacity, QueryPlanCache.MaxCapacity);

            _ = Pl.Config.SetQueryCacheCapacity(512);
            Assert.Equal(512, QueryPlanCache.MaxCapacity);

            _ = Pl.Config.SetQueryCacheEnabled(false);
            Assert.False(QueryPlanCache.Enabled);
            Assert.Equal("0", Pl.Config["POLARS_QUERY_CACHE_ENABLED"]);

            _ = Pl.Config.SetQueryCacheEnabled(true);
            Assert.True(QueryPlanCache.Enabled);
            Assert.Equal("1", Pl.Config["POLARS_QUERY_CACHE_ENABLED"]);
        }
        finally
        {
            QueryPlanCache.MaxCapacity = originalCapacity;
            QueryPlanCache.Enabled = originalEnabled;
        }
    }
}

