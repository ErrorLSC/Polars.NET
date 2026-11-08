namespace Polars.FSharp

type SeriesBinaryNameSpace(parent: Series) =

    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary>
    /// Get the size of the binary data.
    /// </summary>
    member _.Size(?unit: SizeUnit) =
        apply (fun e -> e.Bin.Size(?unit = unit))

    /// <summary>
    /// Check if the binary data contains the given value.
    /// </summary>
    member _.Contains(literal: Expr) = apply (fun e -> e.Bin.Contains literal)

    /// <summary>
    /// Check if the binary data starts with the given prefix.
    /// </summary>
    member _.StartsWith(prefix: Expr) =
        apply (fun e -> e.Bin.StartsWith prefix)

    /// <summary>
    /// Check if the binary data ends with the given suffix.
    /// </summary>
    member _.EndsWith(suffix: Expr) = apply (fun e -> e.Bin.EndsWith suffix)
    /// <summary>
    /// Get the first n bytes of the binary data.
    /// </summary>
    member _.Head(?n) = apply (fun e -> e.Bin.Head(?n = n))
    /// <summary>
    /// Get the last n bytes of the binary data.
    /// </summary>
    member _.Tail(?n) = apply (fun e -> e.Bin.Tail(?n = n))
    /// <summary>
    /// Encode the binary data using the given encoding.
    /// </summary>
    member _.Encode(encoding) = apply (fun e -> e.Bin.Encode(encoding))

    /// <summary>
    /// Decode the binary data using the given encoding.
    /// </summary>
    member _.Decode(encoding, ?strict) =
        apply (fun e -> e.Bin.Decode(encoding, ?strict = strict))

    /// <summary>
    /// Reinterpret the binary data as the given data type.
    /// </summary>
    member _.Reinterpret(dtype: DataTypeExpr, ?endianness) =
        apply (fun e -> e.Bin.Reinterpret(dtype, ?endianness = endianness))

    /// <summary>
    /// Reinterpret the binary data as the given data type.
    /// </summary>
    member _.Reinterpret(dtype: DataType, ?endianness) =
        apply (fun e -> e.Bin.Reinterpret(dtype, ?endianness = endianness))

    /// <summary>
    /// Get a slice of the binary data.
    /// </summary>
    member _.Slice(offset, ?length) =
        apply (fun e -> e.Bin.Slice(offset, ?length = length))
