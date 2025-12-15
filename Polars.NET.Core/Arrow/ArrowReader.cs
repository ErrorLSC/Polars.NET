using System.Collections;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Arrow
{
    public static class ArrowReader
    {
        // ReadRecordBatch 保持不变，它只负责最外层的循环
        public static IEnumerable<T> ReadRecordBatch<T>(RecordBatch batch)
        {
            
            var targetType = typeof(T);

            // [新增] 模式 A: 标量模式 (Scalar Mode)
            // 解决 Rows<int>, Rows<DateTime>, Rows<string> 等问题
            if (IsScalarType(targetType))
            {
                if (batch.ColumnCount == 0) yield break;

                var col = batch.Column(0);
                var accessor = CreateAccessor(col, targetType);
                int count = batch.Length;

                for (int i = 0; i < count; i++)
                {
                    var val = accessor(i);
                    yield return val == null ? default! : (T)val;
                }
                yield break;
            }
            // [原有] 模式 B: 对象映射模式 (Object Mapping Mode)
            // 适用于 POCO (class/struct)
            
            int rowCount = batch.Length;
            
            // 获取可写属性
            var properties = targetType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                       .Where(p => p.CanWrite).ToArray();
            
            var columnAccessors = new Func<int, object?>[properties.Length];

            for (int i = 0; i < properties.Length; i++)
            {
                var prop = properties[i];
                var col = batch.Column(prop.Name); 

                if (col == null) 
                {
                    // 可以选择抛错，或者静默跳过（返回 null）
                    columnAccessors[i] = _ => null; 
                    continue; 
                }

                columnAccessors[i] = CreateAccessor(col, prop.PropertyType);
            }

            for (int i = 0; i < rowCount; i++)
            {
                // 使用 Activator 创建实例，不再依赖 new() 约束
                // ! 压制可能的 null警告（假设 T 是 POCO）
                var item = Activator.CreateInstance<T>()!; 
                
                for (int p = 0; p < properties.Length; p++)
                {
                    var accessor = columnAccessors[p];
                    var val = accessor(i);
                    if (val != null) properties[p].SetValue(item, val);
                }
                yield return item;
            }
        }

        // =============================================================
        // 🧠 核心：支持递归的 Accessor 工厂
        // =============================================================
        private static Func<int, object?> CreateAccessor(IArrowArray array, Type targetType)
        {
            // ---------------------------------------------------------
            // 0. 类型解析 (Type Resolution)
            // ---------------------------------------------------------
            bool isFSharpOption = FSharpHelper.IsFSharpOption(targetType);
            
            // 获取 "真实" 的处理类型
            // 如果是 Option<int> -> int
            // 如果是 int?        -> int
            // 如果是 List<T>     -> List<T>
            var underlyingType = isFSharpOption 
                ? FSharpHelper.GetUnderlyingType(targetType) 
                : (Nullable.GetUnderlyingType(targetType) ?? targetType);

            // 定义基础读取器 (返回 C# 对象或 null)
            Func<int, object?> baseAccessor = null!;

            // ---------------------------------------------------------
            // 1. StructArray -> Class / Struct
            // ---------------------------------------------------------
            if (array is StructArray structArray)
            {
                var props = underlyingType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                          .Where(p => p.CanWrite).ToArray();
                
                var structType = (StructType)structArray.Data.DataType;
                var setters = new List<Action<object, int>>();

                foreach (var prop in props)
                {
                    // 手动查找 Arrow 列索引
                    int fieldIndex = -1;
                    for (int k = 0; k < structType.Fields.Count; k++)
                    {
                        if (structType.Fields[k].Name == prop.Name) { fieldIndex = k; break; }
                    }

                    if (fieldIndex == -1) continue;

                    var childArray = structArray.Fields[fieldIndex];
                    
                    // [递归] 这里的 prop.PropertyType 可能是 Option<T>
                    // 递归调用会自动处理它
                    var childGetter = CreateAccessor(childArray, prop.PropertyType);

                    setters.Add((obj, rowIdx) => 
                    {
                        var val = childGetter(rowIdx);
                        if (val != null) prop.SetValue(obj, val);
                    });
                }

                baseAccessor = idx => 
                {
                    if (structArray.IsNull(idx)) return null;
                    var instance = Activator.CreateInstance(underlyingType)!;
                    foreach (var setter in setters) setter(instance, idx);
                    return instance;
                };
            }
            // ---------------------------------------------------------
            // 2. ListArray / LargeListArray -> List<T>
            // ---------------------------------------------------------
            else if (array is ListArray || array is LargeListArray)
            {
                // 统一处理 List 和 LargeList 的共性逻辑
                IArrowArray valuesArray;
                Func<int, long> getOffset;
                Func<int, bool> isNull;

                if (array is ListArray listArr)
                {
                    valuesArray = listArr.Values;
                    getOffset = i => listArr.ValueOffsets[i];
                    isNull = listArr.IsNull;
                }
                else
                {
                    var largeArr = (LargeListArray)array;
                    valuesArray = largeArr.Values;
                    getOffset = i => largeArr.ValueOffsets[i];
                    isNull = largeArr.IsNull;
                }

                // 解析 List 元素类型
                Type elementType = typeof(object);
                if (underlyingType.IsGenericType) elementType = underlyingType.GetGenericArguments()[0];
                else if (underlyingType.IsArray) elementType = underlyingType.GetElementType()!;
                bool isFSharpList = underlyingType.IsGenericType && 
                                    (underlyingType.GetGenericTypeDefinition().FullName == "Microsoft.FSharp.Collections.FSharpList`1");

                // [递归] 为元素创建读取器
                var childGetter = CreateAccessor(valuesArray, elementType);

                baseAccessor = idx =>
                {
                    if (isNull(idx)) return null;

                    long start = getOffset(idx);
                    long end = getOffset(idx + 1);
                    int count = (int)(end - start);

                    var listType = typeof(List<>).MakeGenericType(elementType);
                    var list = (IList)Activator.CreateInstance(listType, count)!;

                    for (int k = 0; k < count; k++)
                    {
                        var val = childGetter((int)(start + k));
                        list.Add(val);
                    }

                    if (underlyingType.IsArray)
                    {
                        var arr = System.Array.CreateInstance(elementType, list.Count);
                        list.CopyTo(arr, 0);
                        return arr;
                    }
                    if (isFSharpList)
                    {
                        return FSharpHelper.ToFSharpList(list, elementType);
                    }
                    return list;
                };
            }
            // ---------------------------------------------------------
            // 3. 基础类型 (Primitives)
            // ---------------------------------------------------------
            else
            {
                if (underlyingType == typeof(string))
                    baseAccessor = array.GetStringValue;

                else if (underlyingType == typeof(int) || underlyingType == typeof(long))
                {
                    baseAccessor = idx => 
                    {
                        long? val = array.GetInt64Value(idx);
                        if (!val.HasValue) return null;
                        if (underlyingType == typeof(int)) return (int)val.Value;
                        return val.Value;
                    };
                }

                else if (underlyingType == typeof(double) || underlyingType == typeof(float))
                {
                    baseAccessor = idx => 
                    {
                        double? v = array.GetDoubleValue(idx);
                        if (!v.HasValue) return null;
                        if (underlyingType == typeof(float)) return (float)v.Value;
                        return v.Value;
                    };
                }

                else if (underlyingType == typeof(decimal))
                {
                    baseAccessor = idx =>
                    {
                        if (array is Decimal128Array decArr) return decArr.GetValue(idx);
                        if (array is DoubleArray dArr) return dArr.GetValue(idx) is double v ? (decimal)v : (decimal?)null;
                        return null;
                    };
                }

                else if (underlyingType == typeof(bool))
                {
                    baseAccessor = idx => (array as BooleanArray)?.GetValue(idx);
                }

                else if (underlyingType == typeof(DateTime))
                {
                    baseAccessor = idx => array.GetDateTime(idx);
                }
                else if (underlyingType == typeof(DateTimeOffset))
                {
                TimeZoneInfo? tzi = null;
                
                // 1. 尝试从 Arrow Schema 获取时区
                if (array is TimestampArray tsArr && tsArr.Data.DataType is TimestampType tsType)
                {
                    string? arrowTz = tsType.Timezone;
                    if (!string.IsNullOrEmpty(arrowTz))
                    {
                        try 
                        {
                            // 只查找一次！
                            tzi = TimeZoneInfo.FindSystemTimeZoneById(arrowTz);
                        }
                        catch 
                        {
                            // 找不到就降级为 UTC，或者记录日志
                        }
                    }
                }

                // 2. 返回针对该 TimeZone 优化过的读取器
                return baseAccessor = idx => 
                {
                    // 复用 GetDateTimeOffset，但我们把 tzi 传进去（需要重载一下扩展方法）
                    // 或者直接在这里写逻辑
                    return array.GetDateTimeOffsetOptimized(idx, tzi);
                };
                }
                else if (underlyingType == typeof(DateOnly))
                {
                    baseAccessor = idx => array.GetDateOnly(idx);
                }

                else if (underlyingType == typeof(TimeOnly))
                {
                    baseAccessor = idx => array.GetTimeOnly(idx);
                }
                else if (underlyingType == typeof(TimeSpan))
                {
                    baseAccessor = idx => 
                    {
                        TimeSpan? v = array.GetTimeSpan(idx); // 调用 ArrowExtensions
                        if (!v.HasValue) return null;
                        return v.Value;
                    };
                }
            }

            // ---------------------------------------------------------
            // 4. 收尾：F# Option 包装
            // ---------------------------------------------------------
            
            // 如果没有匹配到任何读取器，返回 null 读取器
            if (baseAccessor == null) return _ => null;

            // 如果目标是 F# Option，我们需要把 null 转为 None，把 value 转为 Some(value)
            if (isFSharpOption)
            {
                var wrapper = FSharpHelper.CreateOptionWrapper(targetType);
                return idx => wrapper(baseAccessor(idx));
            }

            return baseAccessor;
        }
        // --- 辅助方法：判断是否为标量类型 ---
        private static bool IsScalarType(Type t)
        {
            var underlying = Nullable.GetUnderlyingType(t) ?? t;

            return underlying.IsPrimitive 
                || underlying == typeof(string)
                || underlying == typeof(decimal)
                || underlying == typeof(DateTime)
                || underlying == typeof(DateOnly)
                || underlying == typeof(TimeOnly)
                || underlying == typeof(TimeSpan)
                || underlying == typeof(DateTimeOffset)
                // F# Option 如果包裹的是标量，也视为标量
                || FSharpHelper.IsFSharpOption(t); 
        }
        /// <summary>
        /// [New] Create a high-performance accessor for a single Arrow Array.
        /// Used by Series.AsSeq().
        /// </summary>
        public static Func<int, object?> GetSeriesAccessor<T>(IArrowArray array)
        {
            // 直接复用 CreateAccessor 的强大逻辑
            // 它支持 DateTime 转换, F# List 转换, 甚至 Struct 递归
            return CreateAccessor(array, typeof(T));
        }
        public static T[] ReadColumn<T>(IArrowArray array)
        {
            var accessor = CreateAccessor(array, typeof(T));
            int len = array.Length;
            var result = new T[len];
            
            for (int i = 0; i < len; i++)
            {
                var val = accessor(i);
                // 处理拆箱和 null
                result[i] = val == null ? default! : (T)val;
            }
            return result;
        }
        /// <summary>
        /// [新增] 读取单个 Array 的第 i 个元素
        /// </summary>
        public static T? ReadItem<T>(IArrowArray array, int index)
        {
            var accessor = CreateAccessor(array, typeof(T));
            var val = accessor(index);
            return val == null ? default : (T)val;
        }
    }
    
}