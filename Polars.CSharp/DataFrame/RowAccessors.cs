using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Polars.NET.Core;

namespace Polars.CSharp;

internal static class RowMapper<T> where T : new()
{

    private static readonly ConditionalWeakTable<PolarsSchema, Func<Series[], long, T>> Cache = [];

    public static Func<Series[], long, T> GetOrCreate(DataFrame df)
    {
        var schema = df.Schema;
        if (Cache.TryGetValue(schema, out var mapper))
        {
            return mapper;
        }

        mapper = BuildMapper(df);
        Cache.AddOrUpdate(schema, mapper);
        return mapper;
    }

    private static Func<Series[], long, T> BuildMapper(DataFrame df)
    {
        var targetType = typeof(T);
        var colsParam = Expression.Parameter(typeof(Series[]), "columns");
        var idxParam = Expression.Parameter(typeof(long), "rowIndex");

        var instanceVar = Expression.Variable(targetType, "instance");
        var blockExpressions = new List<Expression>
        {
            // var instance = new T();
            Expression.Assign(instanceVar, Expression.New(targetType))
        };

        var props = targetType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var dfCols = df.GetColumns();

        for (int i = 0; i < dfCols.Length; i++)
        {
            var col = dfCols[i];
            var colName = col.Name;

            var prop = Array.Find(props, p => p.CanWrite && string.Equals(p.Name, colName, StringComparison.OrdinalIgnoreCase));
            if (prop == null) continue;

            // columns[i]
            var colAccess = Expression.ArrayIndex(colsParam, Expression.Constant(i));

            // columns[i].GetValue<PropertyType>(rowIndex, uncheck: true)
            var getValueMethod = typeof(Series)
                .GetMethod(nameof(Series.GetValue), [typeof(long), typeof(bool)])!
                .MakeGenericMethod(prop.PropertyType);

            var readExpr = Expression.Call(colAccess, getValueMethod, idxParam, Expression.Constant(true, typeof(bool)));

            // instance.Prop = columns[i].GetValue<PropertyType>(rowIndex, true);
            var assignExpr = Expression.Assign(Expression.Property(instanceVar, prop), readExpr);
            blockExpressions.Add(assignExpr);
        }

        // return instance;
        blockExpressions.Add(instanceVar);

        var body = Expression.Block([instanceVar], blockExpressions);
        return Expression.Lambda<Func<Series[], long, T>>(body, colsParam, idxParam).Compile();
    }
}
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    // ==========================================
    // Object Mapping (To Records)
    // ==========================================

    /// <summary>
    /// Enumerates DataFrame rows as strongly-typed objects using pre-compiled Fast Path mappings.
    /// Eliminates Arrow C-Data export and unboxes primitives directly into properties.
    /// </summary>
    /// <typeparam name="T">The target DTO type with a parameterless constructor.</typeparam>
    /// <returns>An IEnumerable of hydrated objects.</returns>
    public IEnumerable<T> Rows<T>() where T : new()
    {
        long height = Height;
        if (height == 0) yield break;

        var mapper = RowMapper<T>.GetOrCreate(this);

        var columns = GetColumns();

        for (long i = 0; i < height; i++)
        {
            yield return mapper(columns, i);
        }
    }

    /// <summary>
    /// Get data for selected row.
    /// </summary>
    public object?[] Row(long index)
    {
        if (index < 0 || index >= Height)
            throw new IndexOutOfRangeException($"Row index {index} is out of bounds. Height: {Height}");

        long width = this.Width;
        var rowData = new object?[width];

        var columns = GetColumns();

        for (long i = 0; i < width; i++)
        {
            rowData[i] = columns[i].GetValue<object?>(index, uncheck: true);
        }

        return rowData;
    }
}
