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

    internal static class StructBuilderHelper
    {
        // =================================================================
        // 核心逻辑：列式构建 (Columnar Construction)
        // =================================================================
        public static StructArray BuildStructArray<T>(IEnumerable<T> data)
        {
            // 1. 预处理数据 (避免多次枚举)
            // 如果 data 很大，这里会有一份引用拷贝，但为了列式处理是必须的
            var dataList = data as IList<T> ?? data.ToList();
            int length = dataList.Count;
            var type = typeof(T);

            // 2. 获取属性 (过滤索引器)
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetIndexParameters().Length == 0)
                .ToArray();

            var fields = new List<Field>();
            var childrenArrays = new List<IArrowArray>();

            // 3. 遍历每个属性，构建子数组
            foreach (var prop in properties)
            {
                // A. 编译高性能 Getter
                var getter = CompileGetter<T>(prop);

                // B. 构建子数组 (递归调用的魔法在这里发生)
                // 我们通过反射调用泛型 helper 方法 ProjectAndBuild<T, PropType>
                // 这样能保留属性的强类型信息，传给 ArrowConverter.Build<PropType>
                var childArray = ProjectAndBuild(dataList, prop.PropertyType, getter);

                // C. 定义 Field
                // 自动推断可空性：如果是引用类型或 Nullable<T>，则为 nullable
                bool isNullable = !prop.PropertyType.IsValueType || Nullable.GetUnderlyingType(prop.PropertyType) != null;
                var field = new Field(prop.Name, childArray.Data.DataType, isNullable);

                fields.Add(field);
                childrenArrays.Add(childArray);
            }

            // 4. 构建 Struct 自身的 Validity Bitmap
            // 只需要检查 item != null
            var validityBuilder = new BooleanArray.Builder();
            int nullCount = 0;
            foreach (var item in dataList)
            {
                if (item == null)
                {
                    validityBuilder.Append(false);
                    nullCount++;
                }
                else
                {
                    validityBuilder.Append(true);
                }
            }
            var validityBuffer = validityBuilder.Build().ValueBuffer;

            // 5. 组装
            var structType = new StructType(fields);
            
            return new StructArray(
                structType,
                length,
                childrenArrays,
                validityBuffer,
                nullCount
            );
        }

        // =================================================================
        // 辅助：反射桥接
        // =================================================================
        
        /// <summary>
        /// 这是一个泛型桥梁方法。
        /// 它将 IList<TParent> 转换为 IEnumerable<TProp>，然后调用 ArrowConverter.Build
        /// </summary>
        private static IArrowArray ProjectAndBuild<TParent>(IList<TParent> data, Type propType, Func<TParent, object?> getter)
        {
            // 利用反射调用泛型的 BuildColumn<TParent, TProp>
            var method = typeof(StructBuilderHelper)
                .GetMethod(nameof(BuildColumn), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(typeof(TParent), propType);

            return (IArrowArray)method.Invoke(null, new object[] { data, getter })!;
        }

        /// <summary>
        /// 实际执行数据投影和构建的方法
        /// </summary>
        private static IArrowArray BuildColumn<TParent, TProp>(IList<TParent> data, Func<TParent, object?> getter)
        {
            // 1. 投影数据 (Projection)
            // 把 List<Parent> 变成 IEnumerable<Prop>
            var columnData = data.Select(item => 
            {
                // 如果父对象是 null，子属性给默认值 (null 或 0)
                // StructArray 的 ValidityBitmap 会负责标记这一行为空，所以这里填充默认值是安全的
                if (item == null) return default;

                var val = getter(item);
                
                // 处理值类型拆箱
                if (val == null) return default;
                return (TProp)val;
            });

            // 2. [递归] 调用通用转换器
            // 这里会自动路由：
            // - 如果 TProp 是 int -> BuildInt32
            // - 如果 TProp 是 NestedItem -> BuildStructArray (递归回来)
            // - 如果 TProp 是 List<double> -> BuildListArray
            return ArrowConverter.Build(columnData);
        }

        // =================================================================
        // 表达式树优化 Getter (和之前一样)
        // =================================================================
        private static Func<T, object?> CompileGetter<T>(PropertyInfo propertyInfo)
        {
            var instanceParam = Expression.Parameter(typeof(T), "item");
            var propertyAccess = Expression.Property(instanceParam, propertyInfo);
            var convertToObject = Expression.Convert(propertyAccess, typeof(object));
            return Expression.Lambda<Func<T, object?>>(convertToObject, instanceParam).Compile();
        }
    }
}