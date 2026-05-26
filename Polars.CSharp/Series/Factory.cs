#pragma warning disable CS1591
using System.Runtime.CompilerServices;
using Polars.NET.Core;
using Polars.NET.Core.Helpers;

namespace Polars.CSharp;

public partial class Series : IDisposable,IPolarsSeries
{
    // ==========================================
    // Constructors
    // ==========================================

    // 1. Signed Integers
    public Series(string name,ReadOnlySpan<sbyte> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<sbyte?> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<short> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<short?> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<int> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<int?> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<long> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<long?> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<Int128> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<Int128?> data) => Handle = SeriesFactory.CreateSpan(name, data);

    // 2. Unsigned Integers
    public Series(string name,ReadOnlySpan<byte> data) => Handle = SeriesFactory.CreateSpan(name, data);

    public Series(string name,ReadOnlySpan<byte?> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<ushort> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<ushort?> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<uint> data) => Handle = SeriesFactory.CreateSpan(name, data);
    public Series(string name,ReadOnlySpan<uint?> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<ulong> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<ulong?> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<UInt128> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<UInt128?> data) => Handle = SeriesFactory.CreateSpan(name, data);
    // 3. Floating Point
    public Series(string name,ReadOnlySpan<Half> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<Half?> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<float> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<float?> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<double> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<double?> data) => Handle = SeriesFactory.CreateSpan(name, data);    

    // 4. Bool, String, Decimal
    public Series(string name,ReadOnlySpan<bool> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<bool?> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<string?> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<decimal> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<decimal?> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    // 5. Temporal
    public Series(string name,ReadOnlySpan<DateTime> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<DateTime?> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<DateTimeOffset> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<DateTimeOffset?> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<DateOnly> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<DateOnly?> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<TimeOnly> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<TimeOnly?> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<TimeSpan> data) => Handle = SeriesFactory.CreateSpan(name, data);    
    public Series(string name,ReadOnlySpan<TimeSpan?> data) => Handle = SeriesFactory.CreateSpan(name, data);    

    // 6. Fixed Size Arrays (2D)
    public Series(string name, sbyte[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, byte[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, short[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, ushort[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, int[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, uint[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, long[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, ulong[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, Half[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, float[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, double[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, decimal[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, Int128[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, UInt128[,] data) => Handle = SeriesFactory.Create(name, data);
    /// <summary>
    /// This will create an empty series, if dtype is also null, datatype will be null.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="dtype"></param>
    public Series(string name="",DataType? dtype=null)
        => Handle = dtype is null ? PolarsWrapper.SeriesNewNull(name,0) : PolarsWrapper.SeriesNewEmpty(name,dtype.Handle);
         
    // ==========================================
    // High-Level Factories
    // ==========================================
    /// <summary>
    /// Create a Series from a list of objects, primitives, or nested lists.
    /// Uses SeriesFactory to automatically select the fastest path (SIMD for Arrays, Reflection for Lists).
    /// </summary>
    public static Series From<T>(string name, IEnumerable<T> data) 
    {
        var handle = SeriesFactory.CreateGenericType(name, data);

        return new Series(handle);
    }
    /// <summary>
    /// Create Series from array
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series From<T>(string name, T[] data)
    {
        var handle = SeriesFactory.CreateSpan(name, new ReadOnlySpan<T>(data));
        
        if (handle != null)
        {
            return new Series(handle);
        }

        return new Series(SeriesFactory.Create(name, data));
    }
    /// <summary>
    /// Create Series from 2D matrix
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series From<T>(string name, T[,] data)
        => new(SeriesFactory.Create(name,data));
    /// <summary>
    /// Create Series from ReadOnlySpan
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series FromSpan<T>(string name, ReadOnlySpan<T> data)
        => new(SeriesFactory.CreateSpan(name, data));
    /// <summary>
    /// Create Series From single column expression.
    /// </summary>
    public static Series FromExpr(Expr expr)
    {
        using var df = new DataFrame().Select(expr);
        return df[0];
    }
}