using System.Linq.Expressions;
using System.Reflection;

namespace Polars.NET.Core.Arrow
{
    internal static class FSharpHelper
    {
        private const string OptionTypeName = "Microsoft.FSharp.Core.FSharpOption`1";

        public static bool IsFSharpOption(Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition().FullName == OptionTypeName;
        }

        public static Type GetUnderlyingType(Type type)
        {
            return IsFSharpOption(type) ? type.GetGenericArguments()[0] : type;
        }

        /// <summary>
        /// [Fixed] Create Unwrapper: FSharpOption<T> -> T (or null)
        /// Logic: obj == null ? null : obj.Value
        /// </summary>
        public static Func<object, object?> CreateOptionUnwrapper(Type optionType)
        {
            // Param: (object obj)
            var inputParam = Expression.Parameter(typeof(object), "obj");
            
            // 1. Cast: (FSharpOption<T>)obj
            var castedInput = Expression.Convert(inputParam, optionType);

            // 2. Access: .Value
            var valueProp = optionType.GetProperty("Value");
            if (valueProp == null) 
                throw new InvalidOperationException($"Type {optionType.Name} missing 'Value' property.");

            var valueAccess = Expression.Property(castedInput, valueProp);
            
            // 3. Result: (object)Value
            var result = Expression.Convert(valueAccess, typeof(object));

            // 4. Check: obj == null
            // In C# interop, F# None is represented as null for reference types.
            // Even though FSharpOption<T> is a class, None is null.
            var nullCheck = Expression.ReferenceEqual(inputParam, Expression.Constant(null));
            
            // Body: if (obj == null) return null; else return obj.Value;
            var body = Expression.Condition(
                nullCheck,
                Expression.Constant(null, typeof(object)), 
                result
            );

            return Expression.Lambda<Func<object, object?>>(body, inputParam).Compile();
        }

        /// <summary>
        /// Create Wrapper: T -> FSharpOption<T>
        /// Logic: val == null ? None : Some(val)
        /// </summary>
        public static Func<object?, object> CreateOptionWrapper(Type optionType)
        {
            var innerType = GetUnderlyingType(optionType);
            
            var inputParam = Expression.Parameter(typeof(object), "val");

            var someMethod = optionType.GetMethod("Some", BindingFlags.Public | BindingFlags.Static)!;
            var noneProp = optionType.GetProperty("None", BindingFlags.Public | BindingFlags.Static)!;
            
            var castedVal = Expression.Convert(inputParam, innerType);
            var callSome = Expression.Call(someMethod, castedVal);
            var noneValue = Expression.Property(null, noneProp);

            var isNull = Expression.Equal(inputParam, Expression.Constant(null));
            var body = Expression.Condition(isNull, noneValue, callSome);

            return Expression.Lambda<Func<object?, object>>(body, inputParam).Compile();
        }
        /// <summary>
        /// Converts C# IList to F# List (FSharpList<T>)
        /// </summary>
    public static object ToFSharpList(System.Collections.IList list, Type elementType)
    {
        // Reflection: Microsoft.FSharp.Collections.ListModule.OfSeq<T>(IEnumerable<T>)
        var listModule = Type.GetType("Microsoft.FSharp.Collections.ListModule, FSharp.Core");
        if (listModule == null) throw new InvalidOperationException("FSharp.Core assembly not found.");

        var ofSeqMethod = listModule.GetMethod("OfSeq")!.MakeGenericMethod(elementType);
        
        // We must cast IList to IEnumerable<T> for OfSeq to work
        var castMethod = typeof(System.Linq.Enumerable).GetMethod("Cast")!.MakeGenericMethod(elementType);
        var typedEnumerable = castMethod.Invoke(null, [list]);

        return ofSeqMethod.Invoke(null, [typedEnumerable])!;
    }
    }
}