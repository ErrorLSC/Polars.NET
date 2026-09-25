namespace Polars.NET.Linq.Provider

open System
open System.Collections.Concurrent
open System.Linq
open System.Linq.Expressions
open System.Reflection

/// Global reflection cache for LINQ methods to eliminate runtime method lookup overhead
type internal LinqReflectionCache private () =
    static let queryableMethods = typeof<Queryable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
    static let enumerableMethods = typeof<Enumerable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)

    // Pre-resolved Queryable definitions
    static let whereMethodDef =
        queryableMethods
        |> Array.find (fun m -> 
            m.Name = "Where" && 
            m.GetParameters().Length = 2 && 
            m.GetParameters().[1].ParameterType.GetGenericTypeDefinition() = typedefof<Expression<Func<_, _>>>)

    static let countMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "Count" && m.GetParameters().Length = 1)
    static let longCountMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "LongCount" && m.GetParameters().Length = 1)
    static let anyMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "Any" && m.GetParameters().Length = 1)
    static let singleMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "Single" && m.GetParameters().Length = 1)
    static let singleOrDefaultMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "SingleOrDefault" && m.GetParameters().Length = 1)
    static let firstMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "First" && m.GetParameters().Length = 1)
    static let firstOrDefaultMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "FirstOrDefault" && m.GetParameters().Length = 1)
    static let lastMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "Last" && m.GetParameters().Length = 1)
    static let lastOrDefaultMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "LastOrDefault" && m.GetParameters().Length = 1)

    // Cache for constructed generic methods
    static let queryableGenericCache = ConcurrentDictionary<string * Type, MethodInfo>()
    static let enumerableGenericCache = ConcurrentDictionary<string * int * string, MethodInfo>()

    static member QueryableWhereDef = whereMethodDef
    
    static member GetQueryableWhere(elemType: Type) =
        queryableGenericCache.GetOrAdd(("Where", elemType), fun _ -> whereMethodDef.MakeGenericMethod(elemType))

    /// Resolves and caches 1-argument Queryable scalar methods (Count, Any, First, etc.)
    static member GetQueryableScalar(methodName: string, elemType: Type) : MethodInfo =
        queryableGenericCache.GetOrAdd((methodName, elemType), fun _ ->
            let mDef =
                match methodName with
                | "Count" -> countMethodDef
                | "LongCount" -> longCountMethodDef
                | "Any" -> anyMethodDef
                | "Single" -> singleMethodDef
                | "SingleOrDefault" -> singleOrDefaultMethodDef
                | "First" -> firstMethodDef
                | "FirstOrDefault" -> firstOrDefaultMethodDef
                | "Last" -> lastMethodDef
                | "LastOrDefault" -> lastOrDefaultMethodDef
                | other -> 
                    queryableMethods 
                    |> Array.find (fun m -> m.Name = other && m.GetParameters().Length = 1)
            mDef.MakeGenericMethod(elemType)
        )

    /// Resolves and caches Enumerable methods for in-memory fallbacks
    static member GetEnumerableMethod(name: string, paramCount: int, typeArgs: Type array) : MethodInfo =
        let key = sprintf "%s_%d_%s" name paramCount (String.Join(",", typeArgs |> Array.map (fun t -> t.FullName)))
        enumerableGenericCache.GetOrAdd((name, paramCount, key), fun _ ->
            let methodDef =
                enumerableMethods
                |> Array.find (fun m ->
                    m.Name = name &&
                    m.GetParameters().Length = paramCount &&
                    m.GetGenericArguments().Length = typeArgs.Length)
            methodDef.MakeGenericMethod(typeArgs)
        )