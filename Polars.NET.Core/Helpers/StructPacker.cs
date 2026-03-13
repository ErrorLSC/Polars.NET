using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace Polars.NET.Core.Helpers;
public static class StructPacker
{
    // Cache the compiled delegetes for extracting columns to avoid repeated reflection overhead
    private static readonly ConcurrentDictionary<Type, Func<object[], SeriesHandle[]>> _packerCache 
        = new();

    public static SeriesHandle Pack<T>(string name, T[] rows)
    {
        // 1. In a real highly-optimized scenario, you'd cache strongly-typed delegates 
        // to avoid allocating object arrays, but here is a simplified Expression Tree approach.
        Type type = typeof(T);
        PropertyInfo[] props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var fieldHandles = new List<SeriesHandle>(props.Length);

        try
        {
            foreach (var prop in props)
            {
                // Invoke cached fast extractor
                SeriesHandle h = ExtractAndCreateSeries(rows, prop);
                fieldHandles.Add(h);
            }

            // Construct Struct Series
            return PolarsWrapper.SeriesNewStruct(name, fieldHandles.ToArray());
        }
        finally
        {
            // Dispose Handles to prevent unmanaged memory leaks in Rust
            foreach (var h in fieldHandles)
            {
                h.Dispose();
            }
        }
    }

    private static SeriesHandle ExtractAndCreateSeries<T>(T[] rows, PropertyInfo prop)
    {
        // Use generic reflection to invoke the strongly-typed extractor
        MethodInfo method = typeof(StructPacker).GetMethod(
            nameof(ExtractTypedColumn), 
            BindingFlags.NonPublic | BindingFlags.Static)!;
            
        MethodInfo genericMethod = method.MakeGenericMethod(typeof(T), prop.PropertyType);
        
        // Return the SeriesHandle generated from the strongly typed array
        return (SeriesHandle)genericMethod.Invoke(null, [rows, prop])!;
    }

    // Strongly-typed extractor to avoid boxing for value types
    private static SeriesHandle ExtractTypedColumn<T, TProp>(T[] rows, PropertyInfo prop)
    {
        int count = rows.Length;
        TProp[] columnData = new TProp[count];
        
        // Compile a fast property getter delegate: (T obj) => obj.Property
        Func<T, TProp> getter = CreateGetter<T, TProp>(prop);

        for (int i = 0; i < count; i++)
        {
            // Direct call, no boxing, no reflection overhead in the loop
            columnData[i] = getter(rows[i]);
        }

        // SeriesFactory now receives a strongly-typed array (e.g., int[], double[])
        return SeriesFactory.Create(prop.Name, columnData);
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