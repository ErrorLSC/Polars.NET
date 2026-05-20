namespace Polars.FSharp

type SeriesBinaryNameSpace(parent: Series) =
    
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr
    member _.Size(?unit:SizeUnit) = apply (fun e -> e.Bin.Size(?unit=unit))
    member _.Contains(literal:Expr) = apply (fun e -> e.Bin.Contains(literal))
    member _.StartsWith(prefix:Expr) = apply (fun e -> e.Bin.StartsWith(prefix))
    member _.EndsWith(suffix:Expr) = apply (fun e -> e.Bin.EndsWith(suffix))
    member _.Head(?n) = apply (fun e->e.Bin.Head(?n=n))
    member _.Tail(?n) = apply (fun e->e.Bin.Tail(?n=n))
    member _.Encode(encoding) = apply (fun e->e.Bin.Encode(encoding))
    member _.Decode(encoding,?strict) = apply (fun e->e.Bin.Decode(encoding,?strict=strict))
    member _.Reinterpret(dtype:DataTypeExpr,?endianness) = apply (fun e->e.Bin.Reinterpret(dtype,?endianness=endianness))
    member _.Reinterpret(dtype:DataType,?endianness) = apply (fun e->e.Bin.Reinterpret(dtype,?endianness=endianness))
    member _.Slice(offset,?length) = apply (fun e->e.Bin.Slice(offset,?length=length))