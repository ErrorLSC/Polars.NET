namespace Polars.FSharp

open Polars.NET.Core

type [<Struct>] NameOps(handle: ExprHandle) =
    /// <summary>
    /// Prefix the column name with a specified string.
    /// </summary>
    member _.Prefix(prefix: string) = new Expr(PolarsWrapper.Prefix(handle,prefix))
    /// <summary>
    /// Suffix the column name with a specified string.
    /// </summary>
    member _.Suffix(suffix: string) = new Expr(PolarsWrapper.Suffix(handle,suffix))
    /// <summary>
    /// Add a prefix to all field names of a struct.
    /// </summary>
    /// <param name="prefix">Prefix to add to the field name.</param>
    member _.PrefixFields(prefix) = new Expr(PolarsWrapper.FieldPrefix(handle,prefix))
    /// <summary>
    /// Add a suffix to all field names of a struct.
    /// </summary>
    /// <param name="suffix">Suffix to add to the field name.</param>
    member _.SuffixFields(suffix) = new Expr(PolarsWrapper.FieldSuffix(handle,suffix))
    /// <summary>
    /// Make the root column name uppercase.
    /// </summary>
    member _.ToUppercase() = new Expr(PolarsWrapper.NameToUpperCase(handle))
    /// <summary>
    /// Make the root column name lowercase.
    /// </summary>
    member _.ToLowercase() = new Expr(PolarsWrapper.NameToLowerCase(handle))  
    /// <summary>
    /// Replace matching regex/literal substring in the name with a new value.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <param name="value">String that will replace the matched substring.</param>
    /// <param name="literal">Treat pattern as a literal string, not a regex.</param>
    member _.Replace(pattern,value,?literal) = 
        let lit = defaultArg literal false
        new Expr(PolarsWrapper.NameReplace(handle,pattern,value,lit))  
    /// <summary>
    /// Rename the output of an expression by mapping a function over the root name.
    /// </summary>
    /// <param name="func">Function that maps a root name to a new name.</param>
    member _.Map(func:string -> string) = new Expr(PolarsWrapper.MapName(handle,func))
    /// <summary>
    /// Rename fields of a struct by mapping a function over the field name(s).
    /// </summary>
    /// <param name="func">Function that maps a field name to a new name.</param>
    member _.MapFields(func:string -> string) = new Expr(PolarsWrapper.StructMapFields(handle,func))

