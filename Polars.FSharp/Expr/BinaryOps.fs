namespace Polars.FSharp

open Polars.NET.Core

type [<Struct>] BinaryOps(handle: ExprHandle) = 
    static member private BytesInKb = 1024.0
    static member private BytesInMb = 1048576.0
    static member private BytesInGb = 1073741824.0
    static member private BytesInTb = 1099511627776.0
    /// <summary>
    /// Computes the size of the binary data in the specified unit.
    /// The default unit is Bytes. Scaling to other units will result in a Float64 expression.
    /// </summary>
    /// <param name="unit">The unit to scale the binary size to.</param>
    /// <returns>A new Polars Expression representing the scaled size.</returns>
    member this.Size(?unit: SizeUnit) =
        let unit = defaultArg unit SizeUnit.Bytes
        let byteSizeExpr = new Expr(PolarsWrapper.BinSizeBytes handle)
        match unit with
        | SizeUnit.Bytes     -> byteSizeExpr
        | SizeUnit.Kilobytes -> byteSizeExpr / new Expr(PolarsWrapper.Lit(BinaryOps.BytesInKb))
        | SizeUnit.Megabytes -> byteSizeExpr / new Expr(PolarsWrapper.Lit(BinaryOps.BytesInMb))
        | SizeUnit.Gigabytes -> byteSizeExpr / new Expr(PolarsWrapper.Lit(BinaryOps.BytesInGb))
        | SizeUnit.Terabytes -> byteSizeExpr / new Expr(PolarsWrapper.Lit(BinaryOps.BytesInTb))
    /// <summary>
    /// Check if binaries in Series contain a binary substring.
    /// </summary>
    /// <param name="literal">The binary substring to look for</param>
    /// <returns>Expression/Series of data type Boolean.</returns>
    member _.Contains(literal:Expr) = new Expr(PolarsWrapper.BinContains(handle,literal.CloneHandle()))
    /// <summary>
    /// Check if values start with a binary substring.
    /// </summary>
    /// <param name="prefix">Prefix substring.</param>
    /// <returns>Expression/Series of data type Boolean.</returns>
    member _.StartsWith(prefix:Expr) = new Expr(PolarsWrapper.BinStartsWith(handle,prefix.CloneHandle()))
    /// <summary>
    /// Check if values start with a binary substring.
    /// </summary>
    /// <param name="suffix">Prefix substring.</param>
    /// <returns>Expression/Series of data type Boolean.</returns>
    member _.EndsWith(suffix:Expr) = new Expr(PolarsWrapper.BinEndsWith(handle,suffix.CloneHandle()))
    /// <summary>
    /// Take the first n bytes of the binary values.
    /// </summary>
    /// <param name="n">Length of the slice (integer or expression). Negative indexing is supported</param>
    /// <returns>Expression/Series of data type Binary</returns>
    member _.Head(?n:int) =
        let nE = defaultArg n 5
        new Expr(PolarsWrapper.BinHead(handle,PolarsWrapper.Lit nE))
    /// <summary>
    /// Take the last n bytes of the binary values.
    /// </summary>
    /// <param name="n">Length of the slice (integer or expression). Negative indexing is supported</param>
    /// <returns>Expression/Series of data type Binary</returns>
    member _.Tail(?n:int) =
        let nE = defaultArg n 5
        new Expr(PolarsWrapper.BinTail(handle,PolarsWrapper.Lit nE))
    /// <summary>
    /// Encode a value using the provided encoding.
    /// </summary>
    /// <param name="encoding">The encoding to use.</param>
    /// <returns>Expression/Series of data type Binary.</returns>
    member _.Encode(encoding:TransferEncoding) =
        match encoding with
        | TransferEncoding.Base64 -> new Expr(PolarsWrapper.BinBase64Encode(handle))
        | TransferEncoding.Hex -> new Expr(PolarsWrapper.BinHexEncode(handle))
    /// <summary>
    /// Decode values using the provided encoding.
    /// </summary>
    /// <param name="encoding">The encoding to use.</param>
    /// <param name="strict">Raise an error if the underlying value cannot be decoded, otherwise mask out with a null value.</param>
    /// <returns>Expression/Series of data type Binary.</returns>
    member _.Decode(encoding:TransferEncoding,?strict) =
        let st = defaultArg strict true 
        match encoding with
        | TransferEncoding.Base64 -> new Expr(PolarsWrapper.BinBase64Decode(handle,st))
        | TransferEncoding.Hex -> new Expr(PolarsWrapper.BinHexDecode(handle,st))
    /// <summary>
    /// Interpret bytes as another type.Supported types are numerical or temporal dtypes, or an Array of these dtypes.
    /// </summary>
    /// <param name="dtype">Which type to interpret binary column into.</param>
    /// <param name="endianness">Which endianness to use when interpreting bytes, by default “little”.</param>
    /// <returns>Expression/Series of data type dtype. 
    /// Note that rows of the binary array where the length does not match the size in bytes of the output array 
    /// (number of items * byte size of item) will become NULL.</returns>
    member _.Reinterpret(dtype:DataTypeExpr,?endianness:Endianness) =
        let endi = defaultArg endianness Endianness.Little
        let isLittle = endi = Endianness.Little
        new Expr(PolarsWrapper.BinReinterpret(handle,dtype.CloneHandle(),isLittle))
    member this.Reinterpret(dtype:DataType,?endianness:Endianness) = 
        this.Reinterpret(dtype.ToDataTypeExpr(),?endianness=endianness)
    /// <summary>
    /// Slice the binary values.
    /// </summary>
    /// <param name="offset">Start index. Negative indexing is supported.</param>
    /// <param name="length">Length of the slice. If set to None (default), the slice is taken to the end of the value.</param>
    /// <returns>Expression/Series of data type Binary.</returns>
    member this.Slice(offset:int,?length:int) =
        let offint = PolarsWrapper.Lit offset
        let lengthint = 
            match length with
            | Some length -> PolarsWrapper.Lit length
            | None -> PolarsWrapper.LitNull()
        new Expr(PolarsWrapper.BinSlice(handle,offint,lengthint))
    

