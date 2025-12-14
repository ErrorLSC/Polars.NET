using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Arrow
{
public static class ArrowConverter
    {
        /// <summary>
        /// 通用入口：根据 T 的类型决定创建什么 Array
        /// </summary>
        public static IArrowArray Build<T>(IEnumerable<T> data)
        {
            var type = typeof(T);
            var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

            // 1. 基础类型 (Primitives & String)
            if (underlyingType == typeof(int)) return BuildInt32(data.Cast<int?>());
            if (underlyingType == typeof(long)) return BuildInt64(data.Cast<long?>());
            if (underlyingType == typeof(double)) return BuildDouble(data.Cast<double?>());
            if (underlyingType == typeof(bool)) return BuildBoolean(data.Cast<bool?>());
            if (underlyingType == typeof(string)) return BuildString(data.Cast<string?>());

            // 2. 递归支持 List<U>
            // 检查是否实现了 IEnumerable<U> 且不是 string
            if (type != typeof(string) && 
                type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
            {
                // 获取 List 里面的元素类型 U
                // 比如 T 是 List<int>，那么 U 就是 int
                var enumerableInterface = type.GetInterfaces()
                    .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
                var elementType = enumerableInterface.GetGenericArguments()[0];

                // 反射调用泛型方法 BuildListArray<U>
                // 因为我们在写代码时不知道 U 是什么
                var method = typeof(ArrowConverter)
                    .GetMethod(nameof(BuildListArray), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(elementType);

                return (IArrowArray)method.Invoke(null, [data])!;
            }

            // 3. 支持 Struct (对象)
            if (type.IsClass)
            {
                return StructBuilderHelper.BuildStructArray(data);
            }

            throw new NotSupportedException($"Type {type.Name} is not supported yet.");
        }

        /// <summary>
        /// 核心逻辑：构建 ListArray (支持递归)
        /// 逻辑：拍扁数据 -> 构建子数组 -> 组装
        /// </summary>
        public static ListArray BuildListArray<U>(IEnumerable<IEnumerable<U>?> data)
        {
            // A. 拍扁数据 (Flatten)
            // 比如 [[1, 2], null, [3]] -> [1, 2, 3]
            var flattenedData = new List<U>();
            var offsetsBuilder = new Int32Array.Builder();
            var validityBuilder = new BooleanArray.Builder();
            
            int currentOffset = 0;
            offsetsBuilder.Append(0); // Start offset

            int nullCount = 0;

            foreach (var subList in data)
            {
                if (subList == null)
                {
                    validityBuilder.Append(false);
                    offsetsBuilder.Append(currentOffset); // 长度为0
                    nullCount++;
                }
                else
                {
                    validityBuilder.Append(true);
                    
                    int count = 0;
                    foreach (var item in subList)
                    {
                        flattenedData.Add(item);
                        count++;
                    }
                    
                    currentOffset += count;
                    offsetsBuilder.Append(currentOffset);
                }
            }

            // B. [递归] 构建子数组 (Values Array)
            // 如果 U 是 int，这里造出来的就是 IntArray
            // 如果 U 是 List<string>，这里造出来的就是 ListArray
            IArrowArray valuesArray = Build(flattenedData);

            // C. 组装
            var offsetsArray = offsetsBuilder.Build();
            var validityArray = validityBuilder.Build();
            
            // 构造 ListType
            var listType = new ListType(valuesArray.Data.DataType);

            return new ListArray(
                listType,
                data.Count(),
                offsetsArray.ValueBuffer,
                valuesArray,
                validityArray.ValueBuffer,
                nullCount
            );
        }

        // --- 基础类型 Builders (简单搬运) ---
        
        private static IArrowArray BuildInt32(IEnumerable<int?> data)
        {
            var b = new Int32Array.Builder();
            foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
            return b.Build();
        }
        
        private static IArrowArray BuildInt64(IEnumerable<long?> data)
        {
            var b = new Int64Array.Builder();
            foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
            return b.Build();
        }

        private static IArrowArray BuildDouble(IEnumerable<double?> data)
        {
            var b = new DoubleArray.Builder();
            foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
            return b.Build();
        }

        private static IArrowArray BuildBoolean(IEnumerable<bool?> data)
        {
            var b = new BooleanArray.Builder();
            foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
            return b.Build();
        }

        private static IArrowArray BuildString(IEnumerable<string?> data)
        {
            var b = new StringArray.Builder();
            foreach (var v in data) b.Append(v);
            return b.Build();
        }
    }
     // 你可以照葫芦画瓢，增加 BuildStringListArray, BuildDoubleListArray 等

    public static class StructBuilderHelper
    {
    public static StructArray BuildStructArray<T>(IEnumerable<T> data)
        {
            var type = typeof(T);
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetIndexParameters().Length == 0) 
                .ToArray();
            int length = data.Count();

            var fields = new List<Apache.Arrow.Field>();
            var builders = new List<IArrowArrayBuilder>();
            
            // [性能核心] 预编译的 Getter 列表
            // Func<T, object?>: 输入对象实例，输出属性值
            var getters = new List<Func<T, object?>>();

            // 1. 准备阶段：编译 Getter，创建 Builder
            foreach (var prop in properties)
            {
                // A. 编译高性能 Getter
                getters.Add(CompileGetter<T>(prop));

                // B. 创建 Arrow Builder
                var (fieldType, builder) = CreateBuilderForType(prop.PropertyType);
                
                // 处理可空性
                bool isNullable = Nullable.GetUnderlyingType(prop.PropertyType) != null || !prop.PropertyType.IsValueType;
                fields.Add(new Apache.Arrow.Field(prop.Name, fieldType, isNullable));
                builders.Add(builder);
            }

            var structValidityBuilder = new BooleanArray.Builder();
            int nullCount = 0;

            // 2. 填充数据阶段 (这里是百万行循环，必须极速)
            foreach (var item in data)
            {
                if (item == null)
                {
                    structValidityBuilder.Append(false);
                    nullCount++;
                    foreach (var builder in builders) AppendNullToBuilder(builder);
                }
                else
                {
                    structValidityBuilder.Append(true);
                    
                    // 遍历所有属性
                    for (int i = 0; i < properties.Length; i++)
                    {
                        // [极速调用] 使用预编译的委托，而不是 prop.GetValue(item)
                        var val = getters[i](item);
                        
                        AppendValueToBuilder(builders[i], val);
                    }
                }
            }

            // 3. Build 子数组
            var childrenArrays = builders.Select(b => BuildArray(b)).ToList();
            var structType = new StructType(fields);
            var structValidity = structValidityBuilder.Build();

            return new StructArray(
                structType,
                length,
                childrenArrays,
                structValidity.ValueBuffer,
                nullCount
            );
        }

        // =================================================================
        // ⚡ 黑魔法：使用 Expression Tree 动态编译 Getter
        // 效果：将反射调用 ((T)obj).Prop 编译成直接的 IL 指令
        // =================================================================
        private static Func<T, object?> CompileGetter<T>(PropertyInfo propertyInfo)
        {
            // 定义参数: (T item)
            var instanceParam = Expression.Parameter(typeof(T), "item");

            // 定义访问: item.Property
            var propertyAccess = Expression.Property(instanceParam, propertyInfo);

            // 定义转换: (object)item.Property
            // 因为我们需要统一返回 object，所以必须 Box 值类型
            var convertToObject = Expression.Convert(propertyAccess, typeof(object));

            // 编译成 Lambda: (item) => (object)item.Property
            return Expression.Lambda<Func<T, object?>>(convertToObject, instanceParam).Compile();
        }

        private static (IArrowType, IArrowArrayBuilder) CreateBuilderForType(Type type)
        {
            var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

            if (underlyingType == typeof(int)) 
                return (new Int32Type(), new Int32Array.Builder());
            if (underlyingType == typeof(long)) 
                return (new Int64Type(), new Int64Array.Builder());
            if (underlyingType == typeof(double)) 
                return (new DoubleType(), new DoubleArray.Builder());
            if (underlyingType == typeof(string)) 
                return (new StringType(), new StringArray.Builder());
            if (underlyingType == typeof(bool)) 
                return (new BooleanType(), new BooleanArray.Builder());

            // 递归支持将在这里扩展：
            // if (typeof(System.Collections.IEnumerable).IsAssignableFrom(type) && type != typeof(string))
            // { ... }

            throw new NotSupportedException($"Type {type.Name} is not yet supported in Struct auto-mapping.");
        }
        // --- 辅助方法：往 Builder 里塞值 ---
        // 这一块用 dynamic 或者 switch 会比较长，为了演示逻辑简化写
        private static void AppendValueToBuilder(IArrowArrayBuilder builder, object? value)
        {
            if (value == null)
            {
                AppendNullToBuilder(builder);
                return;
            }

            // 使用模式匹配分发
            switch (builder)
            {
                case Int32Array.Builder b: b.Append((int)value); break;
                case Int64Array.Builder b: b.Append(Convert.ToInt64(value)); break; // 宽容转换
                case DoubleArray.Builder b: b.Append((double)value); break;
                case StringArray.Builder b: b.Append((string)value); break;
                case BooleanArray.Builder b: b.Append((bool)value); break;
                default: throw new NotImplementedException();
            }
        }

        private static void AppendNullToBuilder(IArrowArrayBuilder builder)
        {
             switch (builder)
            {
                case Int32Array.Builder b: b.AppendNull(); break;
                case Int64Array.Builder b: b.AppendNull(); break;
                case DoubleArray.Builder b: b.AppendNull(); break;
                case StringArray.Builder b: b.AppendNull(); break;
                case BooleanArray.Builder b: b.AppendNull(); break;
                default: throw new NotImplementedException();
            }
        }
        private static IArrowArray BuildArray(IArrowArrayBuilder builder)
        {
            return builder switch
            {
                Int32Array.Builder b => b.Build(),
                Int64Array.Builder b => b.Build(),
                DoubleArray.Builder b => b.Build(),
                StringArray.Builder b => b.Build(),
                BooleanArray.Builder b => b.Build(),
                
                // 如果是 Struct 递归的情况，这里也需要支持
                // StructArray.Builder b => b.Build(),
                ListArray.Builder b => b.Build(),

                _ => throw new NotImplementedException($"Builder type {builder.GetType().Name} build not supported.")
            };
        }
    }
}