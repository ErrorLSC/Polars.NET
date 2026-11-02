namespace Polars.NET.Core.Arrow;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Apache.Arrow;
using Polars.NET.Core.Data;

/// <summary>
/// Vectorized Arrow batch adapter for managed UDF callbacks invoked from Polars native engine.
/// Amortizes FFI call overhead by processing chunk arrays in-memory within .NET.
/// </summary>
public static class ArrowUdfBatchAdapter
{
    private static readonly ConcurrentDictionary<string, bool> s_warnedUdfs = new(StringComparer.Ordinal);

    /// <summary>
    /// Emits a warning to console and Trace when a managed UDF callback is pushed down to Rust.
    /// Deduplicates by method name / expression signature so each UDF site is reported only once.
    /// </summary>
    public static void WarnUdfDetected(string methodName, string expressionText)
    {
        string key = $"{methodName}:{expressionText}";
        if (s_warnedUdfs.TryAdd(key, true))
        {
            var prevColor = Console.ForegroundColor;
            try
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine(
                    $"[Polars.NET Warning] .NET UDF callback detected: ('{methodName}': {expressionText})" +
                    "Pushdown into native LazyFrame remains active, but row-by-row managed callback may be slower than native Polars expressions." +
                    "Please make sure the UDF is thread safe or limit number of threads to 1 in PolarsConfig");
            }
            finally
            {
                Console.ForegroundColor = prevColor;
            }

            System.Diagnostics.Trace.TraceWarning(
                $"[Polars.NET Warning] .NET UDF callback detected: ('{methodName}': {expressionText}),Pushdown into native LazyFrame remains active but performance is affected.");
        }
    }

    /// <summary>
    /// Gets a native Polars DataTypeHandle for a .NET type.
    /// </summary>
    public static DataTypeHandle GetDataTypeHandle(Type type)
    {
        var arrowType = ArrowTypeResolver.GetArrowTypeFromNetType(type);
        return PolarsWrapper.MapArrowToDataTypeHandle(arrowType);
    }

    /// <summary>
    /// Adapts a single-parameter delegate (object? -> object?) into an Arrow-to-Arrow batch mapping function.
    /// </summary>
    public static Func<IArrowArray, IArrowArray> CreateSingle(
        Func<object?, object?> userFunc,
        Type inputType,
        Type outputType)
    {
        bool isValueType = inputType.IsValueType && Nullable.GetUnderlyingType(inputType) == null;

        return inputArray =>
        {
            int length = inputArray.Length;
            var accessor = ArrowReader.CreateAccessor(inputArray, inputType);
            var buffer = ArrowColumnBufferFactory.Create(outputType, length);

            for (int i = 0; i < length; i++)
            {
                if (isValueType && inputArray.IsNull(i))
                {
                    buffer.Add(null);
                }
                else
                {
                    object? inVal = accessor(i);
                    object? outVal = userFunc(inVal);
                    buffer.Add(outVal);
                }
            }

            return buffer.BuildArray();
        };
    }

    /// <summary>
    /// Adapts a multi-parameter delegate (object?[] -> object?) into a multi-column Arrow batch mapping function.
    /// </summary>
    public static Func<IReadOnlyList<IArrowArray>, IArrowArray> CreateMulti(
        Func<object?[], object?> userFunc,
        Type[] inputTypes,
        Type outputType)
    {
        int colCount = inputTypes.Length;
        var isValueType = new bool[colCount];
        for (int c = 0; c < colCount; c++)
        {
            isValueType[c] = inputTypes[c].IsValueType && Nullable.GetUnderlyingType(inputTypes[c]) == null;
        }

        return arrays =>
        {
            if (arrays.Count == 0)
                throw new ArgumentException("At least one input array is required for multi-column UDF.", nameof(arrays));

            int length = arrays[0].Length;
            var accessors = new Func<int, object?>[colCount];
            for (int c = 0; c < colCount; c++)
            {
                accessors[c] = ArrowReader.CreateAccessor(arrays[c], inputTypes[c]);
            }

            var buffer = ArrowColumnBufferFactory.Create(outputType, length);
            var rowArgs = new object?[colCount];

            for (int i = 0; i < length; i++)
            {
                bool hasNullValueType = false;
                for (int c = 0; c < colCount; c++)
                {
                    if (isValueType[c] && arrays[c].IsNull(i))
                    {
                        hasNullValueType = true;
                        break;
                    }
                    rowArgs[c] = accessors[c](i);
                }

                if (hasNullValueType)
                {
                    buffer.Add(null);
                }
                else
                {
                    object? outVal = userFunc(rowArgs);
                    buffer.Add(outVal);
                }
            }

            return buffer.BuildArray();
        };
    }
}
