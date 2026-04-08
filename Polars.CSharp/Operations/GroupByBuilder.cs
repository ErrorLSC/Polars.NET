#pragma warning disable CS1591 
using System.Collections;
using Cs = Polars.CSharp.Polars.Selectors;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
/// <summary>
/// Unified builder for DataFrame GroupBy operations (Standard, Dynamic, Rolling)
/// </summary>
public class GroupByBuilder:IEnumerable<(object[] Key, DataFrame Group)>
{
    private readonly DataFrame _df;
    private readonly LazyGroupBy _lazyGrouped;

    internal GroupByBuilder(DataFrame df, LazyGroupBy lazyGrouped)
    {
        _df = df;
        _lazyGrouped = lazyGrouped;
    }

    /// <summary>
    /// Filter groups with a predicate after aggregation.
    /// </summary>
    public GroupByBuilder Having(Expr predicate)
    {
        _lazyGrouped.Having(predicate);
        return this; 
    }

    /// <summary>
    /// Count the number of values in each group.
    /// </summary>
    public DataFrame Count()
        => Agg(Pl.All().Count());

    /// <summary>
    /// Aggregate all columns into lists. 
    /// </summary>
    public DataFrame All()
        => Agg(Pl.All()); 
    
    /// <summary>
    /// Aggregate the first values in the group.
    /// </summary>
    /// <param name="ignoreNulls">Ignore null values (default False). If set to True, the first non-null value for each aggregation is returned, 
    /// otherwise None is returned if no non-null value exists.</param>
    /// <returns></returns>
    public DataFrame First(bool ignoreNulls=false)
        => Agg(Pl.All().First(ignoreNulls)); 
    /// <summary>
    /// Aggregate the last values in the group.
    /// </summary>
    /// <param name="ignoreNulls">Ignore null values (default False). If set to True, the last non-null value for each aggregation is returned, 
    /// otherwise None is returned if no non-null value exists.</param>
    /// <returns></returns>
    public DataFrame Last(bool ignoreNulls=false)
        => Agg(Pl.All().Last(ignoreNulls)); 
    public DataFrame Head(int n = 10) => _lazyGrouped.Head(n).Collect();
    public DataFrame Tail(int n = 10) => _lazyGrouped.Tail(n).Collect();
    /// <summary>
    /// Return the number of rows in each group.
    /// </summary>
    /// <param name="name">Assign a name to the resulting column; if unset, defaults to “len”.</param>
    /// <returns></returns>
    public DataFrame Len(string name="len")
        => Agg(Pl.Len().Alias(name));
    /// <summary>
    /// Reduce the groups to the maximal value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Max()
        => Agg(Pl.All().Max());
    /// <summary>
    /// Reduce the groups to the minimal value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Min()
        => Agg(Pl.All().Min());
    /// <summary>
    /// Reduce the groups to the median value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Median()
        => Agg(Pl.All().Median()); 
    /// <summary>
    /// Reduce the groups to the mean value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Mean()
        => Agg(Pl.All().Mean()); 
    /// <summary>
    /// Count the unique values per group.
    /// </summary>
    /// <returns></returns>   
    public DataFrame NUnique()
        => Agg(Pl.All().NUnique());
    /// <summary>
    /// Reduce the groups to the sum.
    /// </summary>
    /// <returns></returns>  
    public DataFrame Sum()
        => Agg(Pl.All().Sum());  
    /// <summary>
    /// Compute the quantile per group.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="interpolation">Interpolation method.</param>
    /// <returns></returns>          
    public DataFrame Quantile(double quantile,QuantileMethod interpolation = QuantileMethod.Linear)
        => Agg(Pl.All().Quantile(quantile,interpolation));   

    /// <summary>
    /// Aggregate with specified expressions.
    /// Under the hood, this routes through the Lazy engine to maximize performance and optimizations.
    /// </summary>
    /// <param name="aggs">Aggregation expressions</param>
    /// <returns>A new aggregated DataFrame</returns>
    public DataFrame Agg(params Expr[] aggs)
        => _lazyGrouped.Agg(aggs).Collect();

    private (DataFrame GroupsDf, string IndicesColName) BuildGroupsDataFrame()
    {
        string rowIdxCol = "__POLARS_GB_ROW_INDEX";
        string tempCol = "__POLARS_GB_GROUP_INDICES";

        var lazyWithIndex = _df.Lazy().WithRowIndex(rowIdxCol);
        var intoKeys = _lazyGrouped._keys.Select(k => (IntoExpr)k).ToArray();

        LazyGroupBy newLazyGrouped = _lazyGrouped._type switch
        {
            GroupByType.Dynamic => lazyWithIndex.GroupByDynamic(
                indexColumn: _lazyGrouped._indexColumn!,
                every: _lazyGrouped._every!,
                period: _lazyGrouped._period!,
                offset: _lazyGrouped._offset!,
                label: _lazyGrouped._label,
                includeBoundaries: _lazyGrouped._includeBoundaries,
                closedWindow: _lazyGrouped._closedWindow,
                startBy: _lazyGrouped._startBy,
                groupBy: intoKeys),

            GroupByType.Rolling => lazyWithIndex.Rolling(
                indexColumn: _lazyGrouped._indexColumn!,
                period: _lazyGrouped._period!,
                offset: _lazyGrouped._offset!,
                closedWindow: _lazyGrouped._closedWindow,
                groupBy: intoKeys),

            _ => lazyWithIndex.GroupBy(intoKeys, _lazyGrouped._maintainOrder)
        };

        var groupsDf = newLazyGrouped
            .Agg(Pl.Col(rowIdxCol).Implode().Alias(tempCol))
            .Collect();

        return (groupsDf, tempCol);
    }

    /// <summary>
    /// Allows iteration over the groups of the group by operation.(Strong Typed)
    /// </summary>
    public IEnumerable<(TKey Key, DataFrame Group)> GetGroups<TKey>() where TKey : new()
    {
        var (groupsDf, tempCol) = BuildGroupsDataFrame();
        
        using (groupsDf)
        using (var indicesCol = groupsDf[tempCol])
        using (var keysDf = groupsDf.Drop(tempCol))
        {
            var typedKeys = keysDf.Rows<TKey>().ToArray();

            for (int i = 0; i < typedKeys.Length; i++)
            {
                using var slicedList = indicesCol.Slice(i, 1);
                using var indices = slicedList.Explode(); 
                
                var groupDf = _df[indices]; 

                yield return (typedKeys[i], groupDf);
            }
        }
    }

    /// <summary>
    /// Allows iteration over the groups of the group by operation.
    /// </summary>
    public IEnumerator<(object[] Key, DataFrame Group)> GetEnumerator()
    {
        var (groupsDf, tempCol) = BuildGroupsDataFrame();

        using (groupsDf)
        using (var indicesCol = groupsDf[tempCol])
        using (var keysDf = groupsDf.Drop(tempCol))
        {
            for (int i = 0; i < keysDf.Height; i++)
            {
                object[] keyObjects = keysDf.Row(i)!;
                
                using var slicedList = indicesCol.Slice(i, 1);
                using var indices = slicedList.Explode(); 
                
                var groupDf = _df[indices];

                yield return (keyObjects, groupDf);
            }
        }
    }
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}