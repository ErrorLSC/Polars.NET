using System.Linq.Expressions;
using System.Reflection;

namespace Polars.NET.Core.Helpers;

public static class StructPacker
{
    private static class PackerCache<T>
    {
        public static readonly Func<T[], SeriesHandle>[] ColumnPackers;

        static PackerCache()
        {
            var props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            ColumnPackers = new Func<T[], SeriesHandle>[props.Length];

            for (int i = 0; i < props.Length; i++)
            {
                var prop = props[i];
                var method = typeof(StructPacker).GetMethod(nameof(BuildColumnPacker), BindingFlags.NonPublic | BindingFlags.Static)!;
                var genericMethod = method.MakeGenericMethod(typeof(T), prop.PropertyType);
                
                ColumnPackers[i] = (Func<T[], SeriesHandle>)genericMethod.Invoke(null, [prop])!;
            }
        }
    }

    public static SeriesHandle Pack<T>(string name, T[] rows)
    {
        var packers = PackerCache<T>.ColumnPackers;
        var fieldHandles = new SeriesHandle[packers.Length];

        try
        {
            for (int i = 0; i < packers.Length; i++)
            {
                fieldHandles[i] = packers[i](rows);
            }

            // Construct Struct Series
            return PolarsWrapper.SeriesNewStruct(name, fieldHandles);
        }
        finally
        {
            // Dispose Handles to prevent unmanaged memory leaks in Rust
            foreach (var h in fieldHandles)
            {
                h?.Dispose();
            }
        }
    }

    private static Func<T[], SeriesHandle> BuildColumnPacker<T, TProp>(PropertyInfo prop)
    {
        Func<T, TProp> getter = CreateGetter<T, TProp>(prop);
        string propName = prop.Name;

        return rows =>
        {
            int count = rows.Length;
            TProp[] columnData = new TProp[count];
            
            for (int i = 0; i < count; i++)
            {
                columnData[i] = getter(rows[i]);
            }

            return SeriesFactory.Create(propName, columnData);
        };
    }

    // Helper to build Expression Tree getter
    private static Func<T, TProp> CreateGetter<T, TProp>(PropertyInfo propInfo)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var propertyAccess = Expression.Property(instanceParam, propInfo);
        var lambda = Expression.Lambda<Func<T, TProp>>(propertyAccess, instanceParam);
        
        // Compile into a high-performance delegate
        return lambda.Compile();
    }
}