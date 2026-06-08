namespace Polars.FSharp

open System
open Apache.Arrow
open Polars.NET.Core.Arrow 
open Polars.NET.Core.Data 

module Udf =
    // ==========================================
    // Normal Map (T -> U) 
    // ==========================================
    let map (f: 'T -> 'U) : Func<IArrowArray, IArrowArray> =
        Func<IArrowArray, IArrowArray>(fun inputArray ->
            let len = inputArray.Length
            let buffer = ColumnBufferFactory.Create(typeof<'U>, len)
            let rawGetter = ArrowReader.CreateAccessor(inputArray, typeof<'T>)
            
            let tIn = typeof<'T>

            let isNullableValueType = tIn.IsValueType && not (isNull (Nullable.GetUnderlyingType tIn))
            let isPureValueType = tIn.IsValueType && isNull (Nullable.GetUnderlyingType tIn)

            if isPureValueType then
                seq { 0 .. len - 1 }
                |> Seq.iter (fun i ->
                    if inputArray.IsNull i then buffer.Add null
                    else buffer.Add (f (rawGetter.Invoke i |> unbox<'T>))
                )
            elif isNullableValueType then
                seq { 0 .. len - 1 }
                |> Seq.iter (fun i ->
                    if inputArray.IsNull i then 
                        let nullInstance = Unchecked.defaultof<'T>
                        buffer.Add (f nullInstance)
                    else 
                        buffer.Add (f (rawGetter.Invoke i |> unbox<'T>))
                )
            else
                seq { 0 .. len - 1 }
                |> Seq.iter (fun i ->
                    let inputVal = if inputArray.IsNull i then unbox<'T> null else rawGetter.Invoke i |> unbox<'T>
                    buffer.Add (f inputVal)
                )

            buffer.BuildArray()
        )

    // ==========================================
    // 2. Option Map (T option -> U option)
    // ==========================================
    let mapOption (f: 'T option -> 'U option) : Func<IArrowArray, IArrowArray> =
        Func<IArrowArray, IArrowArray>(fun inputArray ->
            let len = inputArray.Length
            let rawGetter = ArrowReader.CreateAccessor(inputArray, typeof<'T>)
            let buffer = ColumnBufferFactory.Create(typeof<'U>, len)

            seq { 0 .. len - 1 }
            |> Seq.iter (fun i ->
                let rawVal = rawGetter.Invoke i
                
                let inputOpt = if isNull rawVal then None else Some (unbox<'T> rawVal)
                
                match f inputOpt with
                | Some v -> buffer.Add v
                | None   -> buffer.Add null
            )

            buffer.BuildArray()
        )

    // ==========================================
    // ValueOption Map (T voption -> U voption) 
    // ==========================================
    let mapValueOption (f: 'T voption -> 'U voption) : Func<IArrowArray, IArrowArray> =
        Func<IArrowArray, IArrowArray>(fun inputArray ->
            let len = inputArray.Length
            let rawGetter = ArrowReader.CreateAccessor(inputArray, typeof<'T>)
            let buffer = ColumnBufferFactory.Create(typeof<'U>, len)

            seq { 0 .. len - 1 }
            |> Seq.iter (fun i ->
                let rawVal = rawGetter.Invoke i
                
                let inputVOpt = if isNull rawVal then ValueNone else ValueSome (unbox<'T> rawVal)
                
                match f inputVOpt with
                | ValueSome v -> buffer.Add v
                | ValueNone   -> buffer.Add null
            )

            buffer.BuildArray()
        )