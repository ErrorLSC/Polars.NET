namespace Polars.FSharp

[<AutoOpen>]
module SeriesNamespaces =
    type Series with
        /// <summary> Access temporal (Date/Time) operations. </summary>
        member this.Dt = SeriesDtNameSpace this
        /// <summary> Access string manipulation operations. </summary>
        member this.Str = SeriesStrNameSpace this
        /// <summary> Access list operations. </summary>
        member this.List = SeriesListNameSpace this
        /// <summary> Access array (fixed-size list) operations. </summary>
        member this.Array = SeriesArrayNameSpace this
        /// <summary> Access struct operations. </summary>
        member this.Struct = SeriesStructNameSpace this
        /// <summary> Access binary operations. </summary>
        member this.Bin = SeriesBinaryNameSpace this
        /// <summary> Access categorical operations. </summary>
        member this.Cat = SeriesCategoricalNameSpace this