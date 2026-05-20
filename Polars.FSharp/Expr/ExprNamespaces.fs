namespace Polars.FSharp

[<AutoOpen>]
module ExprNamespaces =
    type Expr with
        /// <summary> Access temporal (date/time) operations. </summary>
        member this.Dt = new DtOps(this.CloneHandle())
        /// <summary> Access string manipulation operations. </summary>
        member this.Str = new StringOps(this.CloneHandle())
        /// <summary> Access list operations. </summary>
        member this.List = new ListOps(this.CloneHandle())
        /// <summary> Access array operations. </summary>
        member this.Array = new ArrayOps(this.CloneHandle())

        /// <summary> Access naming operations (prefix/suffix). </summary>
        member this.Name = new NameOps(this.CloneHandle())

        /// <summary> Access struct operations. </summary>
        member this.Struct = new StructOps(this.CloneHandle())

        /// <summary> Access meta operations. </summary>
        member this.Meta = new MetaOps(this.Handle)
        /// <summary> Access binary operations. </summary>
        member this.Bin = new BinaryOps(this.CloneHandle())