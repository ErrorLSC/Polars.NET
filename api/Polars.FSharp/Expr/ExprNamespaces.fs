namespace Polars.FSharp

[<AutoOpen>]
module ExprNamespaces =
    type Expr with
        /// <summary> Access temporal (date/time) operations. </summary>
        member this.Dt = DtOps(this.CloneHandle())
        /// <summary> Access string manipulation operations. </summary>
        member this.Str = StringOps(this.CloneHandle())
        /// <summary> Access list operations. </summary>
        member this.List = ListOps(this.CloneHandle())
        /// <summary> Access array operations. </summary>
        member this.Array = ArrayOps(this.CloneHandle())

        /// <summary> Access naming operations (prefix/suffix). </summary>
        member this.Name = NameOps(this.CloneHandle())

        /// <summary> Access struct operations. </summary>
        member this.Struct = StructOps(this.CloneHandle())

        /// <summary> Access meta operations. </summary>
        member this.Meta = MetaOps(this.CloneHandle())
        /// <summary> Access binary operations. </summary>
        member this.Bin = BinaryOps(this.CloneHandle())
        /// <summary> Access categorical operations. </summary>
        member this.Cat = CategoricalOps(this.CloneHandle())
        /// <summary> Access extension datatype operations. </summary>
        member this.Ext = ExtensionOps(this.CloneHandle())
