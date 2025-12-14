using System.Collections;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Arrow
{
    public static class ArrowReader
    {
        // ReadRecordBatch 保持不变，它只负责最外层的循环
        public static IEnumerable<T> ReadRecordBatch<T>(RecordBatch batch) where T : new()
        {
            // ... (复用之前的逻辑) ...
            // 只需要确保 CreateAccessor 被正确调用即可
            
            int rowCount = batch.Length;
            var type = typeof(T);
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                 .Where(p => p.CanWrite).ToArray();
            
            var columnAccessors = new Func<int, object?>[properties.Length];

            for (int i = 0; i < properties.Length; i++)
            {
                var prop = properties[i];
                var col = batch.Column(prop.Name); 

                if (col == null) { columnAccessors[i] = _ => null; continue; }

                // 这里开始进入递归逻辑
                columnAccessors[i] = CreateAccessor(col, prop.PropertyType);
            }

            for (int i = 0; i < rowCount; i++)
            {
                var item = new T();
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
            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

            // ---------------------------------------------------------
            // 1. StructArray -> Class / Struct (递归的核心)
            // ---------------------------------------------------------
            if (array is StructArray structArray)
            {
                // A. 准备子属性元数据
                var props = underlyingType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                          .Where(p => p.CanWrite).ToArray();
                
                var structType = (StructType)structArray.Data.DataType;
                
                // B. 预编译子字段的 Setter
                // Action<object, int>: 传入目标对象(obj)和行号(rowIdx)，将 Arrow 值填入 obj
                var setters = new List<Action<object, int>>();

                foreach (var prop in props)
                {
                    // 查找对应的 Arrow 列 (按名字匹配)
                    // 注意：structType.Fields 保存元数据，structArray.Fields 保存实际数组
                    int fieldIndex = structType.GetFieldIndex(prop.Name);
                    
                    if (fieldIndex == -1) continue; // C# 有属性但 Arrow 没列，跳过

                    var childArray = structArray.Fields[fieldIndex];
                    
                    // [递归] 为子字段创建读取器！
                    var childGetter = CreateAccessor(childArray, prop.PropertyType);

                    // 创建 Setter 闭包
                    setters.Add((obj, rowIdx) => 
                    {
                        var val = childGetter(rowIdx);
                        if (val != null) prop.SetValue(obj, val);
                    });
                }

                // C. 返回 Struct 读取器
                return idx => 
                {
                    if (structArray.IsNull(idx)) return null;

                    // 创建 POCO 实例
                    var instance = Activator.CreateInstance(underlyingType);
                    
                    // 填充属性
                    foreach (var setter in setters)
                    {
                        setter(instance!, idx);
                    }
                    return instance;
                };
            }

            // ---------------------------------------------------------
            // 2. ListArray -> List<T> / IEnumerable<T>
            // ---------------------------------------------------------
            if (array is ListArray listArray)
            {
                // 获取 List 泛型参数 TElement
                // 假设 targetType 是 List<string>，elementType 就是 string
                Type elementType = typeof(object);
                if (targetType.IsGenericType)
                {
                     elementType = targetType.GetGenericArguments()[0];
                }
                else if (targetType.IsArray)
                {
                    elementType = targetType.GetElementType()!;
                }

                // [递归] 为 List 的 Values 数组创建读取器
                // 注意：Values 数组是扁平的，索引不是 rowIdx，而是 offset 到 offset+len
                var childArray = listArray.Values;
                var childGetter = CreateAccessor(childArray, elementType);

                return idx =>
                {
                    if (listArray.IsNull(idx)) return null;

                    // 获取切片范围
                    int start = listArray.ValueOffsets[idx];
                    int end = listArray.ValueOffsets[idx+1];
                    int count = end - start;

                    // 创建 C# List
                    // 这里我们需要反射创建泛型 List<TElement>
                    var listType = typeof(List<>).MakeGenericType(elementType);
                    var list = (IList)Activator.CreateInstance(listType, count)!;

                    // 填充 List
                    for (int k = 0; k < count; k++)
                    {
                        // 转换：当前行 List 的第 k 个元素，对应 Values 数组的 (start + k)
                        var val = childGetter(start + k);
                        // List add 会处理 null
                        list.Add(val);
                    }

                    // 如果目标是数组，转数组
                    if (targetType.IsArray)
                    {
                        var arr = System.Array.CreateInstance(elementType, list.Count);
                        list.CopyTo(arr, 0);
                        return arr;
                    }

                    return list;
                };
            }
            if (array is LargeListArray largeListArray)
            {
                Type elementType = typeof(object);
                if (targetType.IsGenericType) elementType = targetType.GetGenericArguments()[0];
                else if (targetType.IsArray) elementType = targetType.GetElementType()!;

                // LargeList 的 Values 依然是 IArrowArray
                var childArray = largeListArray.Values;
                var childGetter = CreateAccessor(childArray, elementType);

                return idx =>
                {
                    if (largeListArray.IsNull(idx)) return null;

                    // [注意] LargeList 的 Offsets 是 long
                    long start = largeListArray.ValueOffsets[idx];
                    long end = largeListArray.ValueOffsets[idx+1];
                    long longCount = end - start;
                    int count = (int)longCount; // C# List 限制 int 长度

                    var listType = typeof(List<>).MakeGenericType(elementType);
                    var list = (IList)Activator.CreateInstance(listType, count)!;

                    for (int k = 0; k < count; k++)
                    {
                        // Values 数组下标是 (int)(start + k)
                        var val = childGetter((int)(start + k));
                        list.Add(val);
                    }

                    if (targetType.IsArray)
                    {
                        var arr = System.Array.CreateInstance(elementType, list.Count);
                        list.CopyTo(arr, 0);
                        return arr;
                    }
                    return list;
                };
            }

            // ---------------------------------------------------------
            // 3. 基础类型 (String, Primitives, Date...)
            // ---------------------------------------------------------
            
            if (underlyingType == typeof(string))
                return array.GetStringValue;

            if (underlyingType == typeof(int) || underlyingType == typeof(long))
            {
                return idx => 
                {
                    long? val = array.GetInt64Value(idx);
                    if (!val.HasValue) return null;
                    if (underlyingType == typeof(int)) return (int)val.Value;
                    return val.Value;
                };
            }

            if (underlyingType == typeof(double) || underlyingType == typeof(float))
            {
                return idx => 
                {
                    double? v = array.GetDoubleValue(idx);
                    if (!v.HasValue) return null;
                    if (underlyingType == typeof(float)) return (float)v.Value;
                    return v.Value;
                };
            }

            if (underlyingType == typeof(DateTime))
            {
                return idx => 
                {
                     DateTime? v = array.GetDateTime(idx);
                     if (!v.HasValue) return null;
                     return v.Value;
                };
            }
            
            if (underlyingType == typeof(bool))
            {
                return idx => 
                {
                     if (array is BooleanArray bArr) return bArr.GetValue(idx);
                     return null;
                };
            }

            if (underlyingType == typeof(decimal))
            {
                return idx =>
                {
                    if (array is Decimal128Array decArr)
                    {
                        return decArr.GetValue(idx); // Arrow 自动处理了 Scale，返回 C# decimal?
                    }
                    // 兼容：如果 Polars 传回的是 Double (还没转 Decimal)，尝试强转
                    if (array is DoubleArray dArr)
                    {
                        var v = dArr.GetValue(idx);
                        return v.HasValue ? (decimal)v.Value : null;
                    }
                    return null;
                };
            }
            if (underlyingType == typeof(DateOnly))
            {
                return idx => 
                {
                    DateOnly? v = array.GetDateOnly(idx);
                    if (!v.HasValue) return null;
                    return v.Value;
                };
            }

            // [新增] TimeOnly
            if (underlyingType == typeof(TimeOnly))
            {
                return idx => 
                {
                    TimeOnly? v = array.GetTimeOnly(idx);
                    if (!v.HasValue) return null;
                    return v.Value;
                };
            }

            // Fallback
            return _ => null;
        }
    }
}