namespace Polars.FSharp

open System
open Apache.Arrow
open Polars.NET.Core.Arrow 
open Polars.NET.Core.Data 

module internal Udf =
    // ==========================================
    // Normal Map (T -> U) 
    // ==========================================
    let internal map (f: 'T -> 'U) : IArrowArray -> IArrowArray =
        fun (inputArray: IArrowArray) ->
            let len = inputArray.Length
            let buffer = ColumnBufferFactory.Create(typeof<'U>, len)
            let rawGetter = ArrowReader.CreateAccessor(inputArray, typeof<'T>)
            
            let tIn = typeof<'T>
            let isNullableValueType = tIn.IsValueType && not (isNull (Nullable.GetUnderlyingType tIn))
            let isPureValueType = tIn.IsValueType && isNull (Nullable.GetUnderlyingType tIn)

            if isPureValueType then
                for i = 0 to len - 1 do
                    if inputArray.IsNull i then
                        buffer.Add null
                    else
                        let v = rawGetter.Invoke i |> unbox<'T>
                        buffer.Add (box (f v))
            elif isNullableValueType then
                for i = 0 to len - 1 do
                    if inputArray.IsNull i then 
                        let nullInstance = Unchecked.defaultof<'T>
                        buffer.Add (box (f nullInstance))
                    else 
                        let v = rawGetter.Invoke i |> unbox<'T>
                        buffer.Add (box (f v))
            else
                for i = 0 to len - 1 do
                    let inputVal =
                        if inputArray.IsNull i then
                            Unchecked.defaultof<'T>
                        else
                            rawGetter.Invoke i |> unbox<'T>
                    buffer.Add (box (f inputVal))

            buffer.BuildArray()

    // ==========================================
    // 2. Option Map (T option -> U option)
    // ==========================================
    let internal mapOption (f: 'T option -> 'U option) : IArrowArray -> IArrowArray =
        fun (inputArray: IArrowArray) ->
            let len = inputArray.Length
            let rawGetter = ArrowReader.CreateAccessor(inputArray, typeof<'T>)
            let buffer = ColumnBufferFactory.Create(typeof<'U>, len)

            for i = 0 to len - 1 do
                let inputOpt =
                    if inputArray.IsNull i then
                        None
                    else
                        let rawVal = rawGetter.Invoke i
                        if isNull rawVal then None else Some (unbox<'T> rawVal)

                match f inputOpt with
                | Some v -> buffer.Add (box v)
                | None   -> buffer.Add null

            buffer.BuildArray()

    // ==========================================
    // ValueOption Map (T voption -> U voption) 
    // ==========================================
    let internal mapValueOption (f: 'T voption -> 'U voption) : IArrowArray -> IArrowArray =
        fun (inputArray: IArrowArray) ->
            let len = inputArray.Length
            let rawGetter = ArrowReader.CreateAccessor(inputArray, typeof<'T>)
            let buffer = ColumnBufferFactory.Create(typeof<'U>, len)

            for i = 0 to len - 1 do
                let inputVOpt =
                    if inputArray.IsNull i then
                        ValueNone
                    else
                        let rawVal = rawGetter.Invoke i
                        if isNull rawVal then ValueNone else ValueSome (unbox<'T> rawVal)

                match f inputVOpt with
                | ValueSome v -> buffer.Add (box v)
                | ValueNone   -> buffer.Add null

            buffer.BuildArray()