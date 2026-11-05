namespace Polars.NET.Core.Arrow;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using Polars.NET.Core.Data;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Wrappers;

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

    private static readonly ConcurrentDictionary<Type, Func<IArrowArray, System.Array>> s_readColumnDelegates = new();

    private static Func<IArrowArray, System.Array> GetArrayReader(Type elementType)
    {
        return s_readColumnDelegates.GetOrAdd(elementType, static type =>
        {
            var method = typeof(ArrowReader)
                .GetMethod(nameof(ArrowReader.ReadColumn), BindingFlags.Public | BindingFlags.Static)!
                .MakeGenericMethod(type);
            var param = Expression.Parameter(typeof(IArrowArray), "array");
            var call = Expression.Call(method, param);
            var cast = Expression.Convert(call, typeof(System.Array));
            return Expression.Lambda<Func<IArrowArray, System.Array>>(cast, param).Compile();
        });
    }

    /// <summary>
    /// Converts an Arrow array to an instance of the target sequence type (e.g. T[], List&lt;T&gt;, IEnumerable&lt;T&gt;, etc.).
    /// </summary>
    public static object? ConvertArrowToArgument(IArrowArray array, Type targetType)
    {
        if (targetType == typeof(IArrowArray) || typeof(IArrowArray).IsAssignableFrom(targetType))
        {
            return array;
        }

        Type elementType = PolarsTypeHelper.TryGetEnumerableElementType(targetType) ?? typeof(object);
        System.Array materializedArray = GetArrayReader(elementType)(array);

        if (targetType.IsArray ||
            targetType == typeof(IEnumerable<>) ||
            targetType.IsAssignableFrom(materializedArray.GetType()))
        {
            return materializedArray;
        }

        if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(List<>))
        {
            var listType = typeof(List<>).MakeGenericType(elementType);
            return Activator.CreateInstance(listType, materializedArray);
        }

        if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(HashSet<>))
        {
            var setType = typeof(HashSet<>).MakeGenericType(elementType);
            return Activator.CreateInstance(setType, materializedArray);
        }

        if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(IGrouping<,>))
        {
            var keyType = targetType.GetGenericArguments()[0];
            var groupingType = typeof(ArrowGrouping<,>).MakeGenericType(keyType, elementType);
            object? defaultKey = PolarsTypeHelper.GetDefaultValue(keyType);
            return Activator.CreateInstance(groupingType, defaultKey, materializedArray);
        }

        return materializedArray;
    }

    private sealed class ArrowGrouping<TKey, TElement>(TKey key, IEnumerable<TElement> elements) : IGrouping<TKey, TElement>
    {
        public TKey Key => key;
        public IEnumerator<TElement> GetEnumerator() => elements.GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => elements.GetEnumerator();
    }

    /// <summary>
    /// Adapts a single-parameter delegate (object? -> object?) into an Arrow group aggregation function.
    /// Receives an Arrow array representing group elements and returns a 1-element Arrow array containing the scalar result.
    /// </summary>
    public static Func<IArrowArray, IArrowArray> CreateGroupAggregationSingle(
        Func<object?, object?> userFunc,
        Type inputArgType,
        Type outputType)
    {
        return inputArray =>
        {
            object? arg = ConvertArrowToArgument(inputArray, inputArgType);
            object? result = userFunc(arg);
            if (result is IArrowArray arrowArr)
            {
                return arrowArr;
            }
            var buffer = ArrowColumnBufferFactory.Create(outputType, 1);
            buffer.Add(result);
            return buffer.BuildArray();
        };
    }

    /// <summary>
    /// Adapts a multi-parameter delegate (object?[] -> object?) into a multi-column Arrow group aggregation function.
    /// Receives Arrow arrays representing group elements for each column and returns a 1-element Arrow array containing the scalar result.
    /// </summary>
    public static Func<IReadOnlyList<IArrowArray>, IArrowArray> CreateGroupAggregationMulti(
        Func<object?[], object?> userFunc,
        Type[] inputArgTypes,
        Type outputType)
    {
        int colCount = inputArgTypes.Length;
        return arrays =>
        {
            if (arrays.Count == 0)
                throw new ArgumentException("At least one input array is required for multi-column group aggregation UDF.", nameof(arrays));

            var rowArgs = new object?[colCount];
            for (int c = 0; c < colCount; c++)
            {
                rowArgs[c] = ConvertArrowToArgument(arrays[c], inputArgTypes[c]);
            }

            object? result = userFunc(rowArgs);
            if (result is IArrowArray arrowArr)
            {
                return arrowArr;
            }
            var buffer = ArrowColumnBufferFactory.Create(outputType, 1);
            buffer.Add(result);
            return buffer.BuildArray();
        };
    }
}
