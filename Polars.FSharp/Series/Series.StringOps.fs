namespace Polars.FSharp

type SeriesStrNameSpace(parent: Series) =
    
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary> Convert to uppercase. </summary>
    member _.ToUpper() = apply (fun e -> e.Str.ToUpper())

    /// <summary> Convert to lowercase. </summary>
    member _.ToLower() = apply (fun e -> e.Str.ToLower())

    /// <summary> Get length in bytes. </summary>
    member _.Len() = apply (fun e -> e.Str.Len())

    /// <summary> Slice the string. </summary>
    member _.Slice(offset: int64, length: uint64) = 
        apply (fun e -> e.Str.Slice(offset, length))

    /// <summary> Replace all occurrences of a pattern. </summary>
    member _.ReplaceAll(pattern: string, value: string, ?literal: bool) =
        apply (fun e -> e.Str.ReplaceAll(pattern, value, ?literal=literal))

    /// <summary> Extract the target capture group from regex pattern. </summary>
    member _.Extract(pattern: string, groupIndex: int) =
        apply (fun e -> e.Str.Extract(pattern, groupIndex))

    /// <summary> Check if string contains pattern. </summary>
    member _.Contains(pat: string) = 
        apply (fun e -> e.Str.Contains pat)

    /// <summary> Split string by separator. Returns a List column. </summary>
    member _.Split(separator: string) = 
        apply (fun e -> e.Str.Split separator)

    // --- Trimming ---

    /// <summary> Remove leading and trailing characters. </summary>
    member _.Strip(?matches: string) = 
        apply (fun e -> e.Str.Strip(?matches=matches))

    /// <summary> Remove leading characters. </summary>
    member _.LStrip(?matches: string) = 
        apply (fun e -> e.Str.LStrip(?matches=matches))

    /// <summary> Remove trailing characters. </summary>
    member _.RStrip(?matches: string) = 
        apply (fun e -> e.Str.RStrip(?matches=matches))

    member _.StripPrefix(prefix: string) = 
        apply (fun e -> e.Str.StripPrefix prefix)

    member _.StripSuffix(suffix: string) = 
        apply (fun e -> e.Str.StripSuffix suffix)

    // --- Checks ---

    member _.StartsWith(prefix: string) = 
        apply (fun e -> e.Str.StartsWith prefix)

    member _.EndsWith(suffix: string) = 
        apply (fun e -> e.Str.EndsWith suffix)

    // --- Parsing ---

    /// <summary> Parse string to Date. </summary>
    member _.ToDate(format: string) = 
        apply (fun e -> e.Str.ToDate format)

    /// <summary> Parse string to Datetime. </summary>
    member _.ToDatetime(format: string) = 
        apply (fun e -> e.Str.ToDatetime format)