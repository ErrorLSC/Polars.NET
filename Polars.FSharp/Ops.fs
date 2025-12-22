namespace Polars.FSharp

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Series =

    // --- Aggregations (Pipe-friendly) ---

    let sum (s: Series) = s.Sum()
    let mean (s: Series) = s.Mean()
    let min (s: Series) = s.Min()
    let max (s: Series) = s.Max()
    let count (s: Series) = s.Length // 或者 s.Length
    let nullCount (s: Series) = s.NullCount

    // --- Arithmetic (Pipe-friendly) ---
    
    let add (other: Series) (s: Series) = s + other
    let sub (other: Series) (s: Series) = s - other
    let mul (other: Series) (s: Series) = s * other
    let div (other: Series) (s: Series) = s / other

    // --- Comparison ---
    
    let eq (other: Series) (s: Series) = s .= other
    let nteq (other: Series) (s: Series) = s != other
    let gt (other: Series) (s: Series) = s .> other
    let lt (other: Series) (s: Series) = s .< other
    let gtEq (other: Series) (s: Series) = s .>= other
    let ltEq (other: Series) (s: Series) = s .<= other

    // 支持标量广播
    let addLit (value: 'T) (s: Series) = s + Series.ofSeq("lit", [value])
    let subLit (value: 'T) (s: Series) = s - Series.ofSeq("lit", [value])
    let subLitFrom (value: 'T) (s: Series) = Series.ofSeq("lit", [value]) - s
    let mulLit (value: 'T) (s: Series) = s * Series.ofSeq("lit", [value])
    let divLit (value: 'T) (s: Series) = s / Series.ofSeq("lit", [value])
    let divLitFrom (value: 'T) (s: Series) = Series.ofSeq("lit", [value]) / s
    // Comparison (用于 Filter / Mask)
    let eqLit (value: 'T) (s: Series) = s .= Series.ofSeq("lit", [value])
    let neqLit (value: 'T) (s: Series) = s != Series.ofSeq("lit", [value])
    let gtLit (value: 'T) (s: Series) = s .> Series.ofSeq("lit", [value])
    let ltLit (value: 'T) (s: Series) = s .< Series.ofSeq("lit", [value])
    let gtEqLit  (value: 'T) (s: Series) = s .>= Series.ofSeq("lit", [value])
    let ltEqLit (value: 'T) (s: Series) = s .<= Series.ofSeq("lit", [value])
    
    // --- Transformations ---
    let cast (dtype: DataType) (s: Series) = s.Cast dtype