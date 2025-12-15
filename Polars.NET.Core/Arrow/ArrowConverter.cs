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
            // [新增] 0. 顶层 F# Option 解包
            // 将 seq<Option<U>> 视为 seq<U?> 处理，生成扁平的 Arrow Array
            if (FSharpHelper.IsFSharpOption(type))
            {
                var innerType = FSharpHelper.GetUnderlyingType(type);
                
                // 获取针对 Option<U> 的解包器
                var unwrapper = FSharpHelper.CreateOptionUnwrapper(type);

                // 动态调用 HandleFSharpOption<InnerType>
                var method = typeof(ArrowConverter)
                    .GetMethod(nameof(HandleFSharpOption), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(innerType);

                // 注意：data 是 IEnumerable<Option<U>>，我们当作 IEnumerable<object> 传进去处理
                return (IArrowArray)method.Invoke(null, new object[] { data, unwrapper })!;
            }
            var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

            // 1. 基础类型 (Primitives & String)
            if (underlyingType == typeof(float)) return BuildFloat(data.Cast<float?>());
            if (underlyingType == typeof(int)) return BuildInt32(data.Cast<int?>());
            if (underlyingType == typeof(long)) return BuildInt64(data.Cast<long?>());
            if (underlyingType == typeof(double)) return BuildDouble(data.Cast<double?>());
            if (underlyingType == typeof(bool)) return BuildBoolean(data.Cast<bool?>());
            if (underlyingType == typeof(string)) return BuildString(data.Cast<string?>());
            if (underlyingType == typeof(DateOnly)) return BuildDate32(data.Cast<DateOnly?>());
            if (underlyingType == typeof(TimeOnly)) return BuildTime64(data.Cast<TimeOnly?>());
            if (underlyingType == typeof(DateTime)) return BuildTimestamp(data.Cast<DateTime?>());
            if (underlyingType == typeof(DateTimeOffset)) return BuildDateTimeOffset(data.Cast<DateTimeOffset?>());
            if (underlyingType == typeof(TimeSpan)) return BuildDuration(data.Cast<TimeSpan?>());
            if (underlyingType == typeof(decimal)) return BuildDecimal(data.Cast<decimal?>());
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
        /// <summary>
        /// 中转方法：判断 U 是值类型还是引用类型，分发到不同的构建器
        /// </summary>
        private static IArrowArray HandleFSharpOption<U>(IEnumerable<object> data, Func<object, object?> unwrapper)
        {
            if (typeof(U).IsValueType)
            {
                // 值类型 (int, double, DateTime...) -> 需要转为 Nullable<U>
                // 必须通过反射调用带 where T : struct 约束的方法，才能让编译器允许 (T?)null 写法
                var method = typeof(ArrowConverter)
                    .GetMethod(nameof(BuildStructOption), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(typeof(U));
                return (IArrowArray)method.Invoke(null, [data, unwrapper])!;
            }
            else
            {
                // 引用类型 (string, List...) -> 直接转为 U (因为 U 本身可空)
                return BuildClassOption<U>(data, unwrapper);
            }
        }

        /// <summary>
        /// 专门处理值类型的 Option (如 Option<int>, Option<DateTime>)
        /// 约束: where T : struct
        /// </summary>
        private static IArrowArray BuildStructOption<T>(IEnumerable<object> data, Func<object, object?> unwrapper) 
            where T : struct
        {
            // 将 IEnumerable<FSharpOption<T>> 转换为 IEnumerable<T?>
            var nullableData = data.Select(item => 
            {
                var val = unwrapper(item);
                // 这里编译器终于开心了，因为 T 是 struct，T? 是合法的 Nullable<T>
                return val == null ? (T?)null : (T)val;
            });
            
            // 递归调用主入口 Build<T?>
            return Build(nullableData);
        }

        /// <summary>
        /// 专门处理引用类型的 Option (如 Option<string>, Option<List<int>>)
        /// </summary>
        private static IArrowArray BuildClassOption<T>(IEnumerable<object> data, Func<object, object?> unwrapper)
        {
            // 将 IEnumerable<FSharpOption<T>> 转换为 IEnumerable<T>
            var classData = data.Select(item => 
            {
                var val = unwrapper(item);
                return (T)val!; // val 为 null 时直接转为 T (对于引用类型 T，null 是合法的)
            });

            // 递归调用主入口 Build<T>
            return Build(classData);
        }
        private static FloatArray BuildFloat(IEnumerable<float?> data)
        {
            var b = new FloatArray.Builder();
            foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
            return b.Build();
        }

        // [新增] Decimal Builder
        private static Decimal128Array BuildDecimal(IEnumerable<decimal?> data)
        {
            // Polars 默认推断通常是 Decimal(38, 9) 或类似，这里我们用常见的 (28, 6) 或者根据数据推断
            // 为了通用性，先给个足够大的精度。Arrow C# 需要显式指定 Type
            var type = new Decimal128Type(38, 18); // 38位精度，18位小数 (标准高精度)
            var b = new Decimal128Array.Builder(type);
            foreach (var v in data)
            {
                if (v.HasValue) b.Append(v.Value);
                else b.AppendNull();
            }
            return b.Build();
        }
        private static Int32Array BuildInt32(IEnumerable<int?> data)
        {
            var b = new Int32Array.Builder();
            foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
            return b.Build();
        }
        
        private static Int64Array BuildInt64(IEnumerable<long?> data)
        {
            var b = new Int64Array.Builder();
            foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
            return b.Build();
        }

        private static DoubleArray BuildDouble(IEnumerable<double?> data)
        {
            var b = new DoubleArray.Builder();
            foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
            return b.Build();
        }

        private static BooleanArray BuildBoolean(IEnumerable<bool?> data)
        {
            var b = new BooleanArray.Builder();
            foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
            return b.Build();
        }

        private static StringArray BuildString(IEnumerable<string?> data)
        {
            var b = new StringArray.Builder();
            foreach (var v in data) b.Append(v);
            return b.Build();
        }
        // [新增] DateOnly -> Date32 (Days since epoch)
        private static Date32Array BuildDate32(IEnumerable<DateOnly?> data)
        {
            var b = new Date32Array.Builder();
            int epoch = new DateOnly(1970, 1, 1).DayNumber;
            foreach (var v in data)
            {
                if (v.HasValue) b.Append(v.Value.ToDateTime(TimeOnly.MinValue));
                else b.AppendNull();
            }
            return b.Build();
        }

        // [新增] TimeOnly -> Time64 (Microseconds)
        private static Time64Array BuildTime64(IEnumerable<TimeOnly?> data)
        {
            var b = new Time64Array.Builder(TimeUnit.Microsecond); // 注意设置单位
            foreach (var v in data)
            {
                if (v.HasValue) b.Append(v.Value.Ticks / 10L); // 1 tick = 100ns, 10 ticks = 1us
                else b.AppendNull();
            }
            return b.Build();
        }
        // [新增] DateTime -> Timestamp (Microsecond)
        private static TimestampArray BuildTimestamp(IEnumerable<DateTime?> data)
        {
            // Polars 默认倾向于 Microsecond 或 Nanosecond
            // C# Ticks 是 100ns。为了兼顾范围和精度，我们选 Microsecond (us)
            // 1 us = 10 Ticks
            var b = new TimestampArray.Builder(TimeUnit.Microsecond);

            foreach (var v in data)
            {
                if (v.HasValue)
                {
                    DateTime val = v.Value;
                    
                    // 1. 如果是 Local，转 UTC
                    if (val.Kind == DateTimeKind.Local)
                    {
                        val = val.ToUniversalTime();
                    }
                    // 2. 如果是 Unspecified，我们默认它就是“墙上时间”（即 Naive），
                    //    或者你可以选择把它当 UTC。Polars 默认通常喜欢 Naive。
                    //    这里我们保持原值，但在转 Offset 时设为 Zero (UTC)
                    
                    var dto = new DateTimeOffset(val, TimeSpan.Zero);
                    b.Append(dto);
                }
                else 
                {
                    b.AppendNull();
                }
            }
            return b.Build();
        }
        private static TimestampArray BuildDateTimeOffset(IEnumerable<DateTimeOffset?> data)
        {
            var b = new TimestampArray.Builder(TimeUnit.Microsecond);
            
            foreach (var v in data)
            {
                if (v.HasValue)
                {
                    // Arrow Builder 会自动把 DateTimeOffset 归一化为 UTC
                    b.Append(v.Value); 
                }
                else
                {
                    b.AppendNull();
                }
            }
            return b.Build();
        }
        private static DurationArray BuildDuration(IEnumerable<TimeSpan?> data)
        {
            // Polars 默认 Duration 是 Microsecond 或 Nanosecond
            // C# TimeSpan.Ticks 是 100ns
            // 我们选择 Microsecond (us) 以保持与 Timestamp 的一致性
            var b = new DurationArray.Builder(DurationType.Microsecond);

            foreach (var v in data)
            {
                if (v.HasValue)
                {
                    // Ticks (100ns) -> Microseconds (1000ns)
                    // 除以 10
                    b.Append(v.Value.Ticks / 10L);
                }
                else
                {
                    b.AppendNull();
                }
            }
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
            bool isFSharpOption = FSharpHelper.IsFSharpOption(propType);
            
            // [关键修复] 如果是 Nullable<T>，我们把 T 传给 BuildColumn，避免 Nullable 嵌套问题
            // 如果 propType 是 DateTime?，targetType 变成 DateTime
            // 如果 propType 是 DateTime，targetType 还是 DateTime
            Type cleanType = propType;

            if (isFSharpOption)
            {
                cleanType = FSharpHelper.GetUnderlyingType(propType);
            }
            else
            {
                // 如果是 Nullable<int>，剥离出 int
                cleanType = Nullable.GetUnderlyingType(propType) ?? propType;
            }

            // 2. 构建目标类型 (TargetType)
            // 如果是值类型 (int, double)，强制包一层 Nullable (int?, double?)
            // 如果是引用类型 (string, List)，保持原样
            Type targetType = cleanType;
            if (cleanType.IsValueType)
            {
                targetType = typeof(Nullable<>).MakeGenericType(cleanType);
            }

            var method = typeof(StructBuilderHelper)
                .GetMethod(nameof(BuildColumn), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(typeof(TParent), targetType);

            return (IArrowArray)method.Invoke(null, [data, getter, isFSharpOption])!;
        }

        /// <summary>
        /// 实际执行数据投影和构建的方法
        /// </summary>
        private static IArrowArray BuildColumn<TParent, TProp>(
            IList<TParent> data, 
            Func<TParent, object?> getter, 
            bool isOption)
        {
            Func<object, object?>? unwrapper = null;
            if (isOption)
            {
                // 获取 TProp 的底层类型 (e.g. int?, double?)
                var innerType = Nullable.GetUnderlyingType(typeof(TProp)) ?? typeof(TProp);
                
                // [修复] 动态构造 FSharpOption<Inner> 类型
                // 以前写 typeof(Microsoft.FSharp...) 会报错，现在调用 Helper
                var optionType = FSharpHelper.MakeFSharpOptionType(innerType);
                
                unwrapper = FSharpHelper.CreateOptionUnwrapper(optionType);
            }
            // 1. 投影数据 (Projection)
            // 把 List<Parent> 变成 IEnumerable<Prop>
            var columnData = data.Select(item => 
            {
                // 如果父对象是 null，子属性给默认值 (null 或 0)
                // StructArray 的 ValidityBitmap 会负责标记这一行为空，所以这里填充默认值是安全的
                if (item == null) return default;

                var rawVal = getter(item);
                
                // 处理值类型拆箱
                if (rawVal == null) return default;
                // [关键] 如果是 Option，先解包
                if (isOption)
                {
                    var unwrapped = unwrapper!(rawVal);
                    // [修复] 使用 default(TProp?)
                    if (unwrapped == null) return default;
                    return (TProp)unwrapped;
                }
                return (TProp)rawVal;
            });

            // 2. [递归] 调用通用转换器
            // 这里会自动路由：
            // - 如果 TProp 是 int -> BuildInt32
            // - 如果 TProp 是 NestedItem -> BuildStructArray (递归回来)
            // - 如果 TProp 是 List<double> -> BuildListArray
            return ArrowConverter.Build(columnData);
        }

        // =================================================================
        // 表达式树优化 Getter
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