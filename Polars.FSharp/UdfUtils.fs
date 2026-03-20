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

            let rawGetter = ArrowReader.CreateAccessor(inputArray, typeof<'T>)

            let buffer = ColumnBufferFactory.Create(typeof<'U>, len)

            let tIn = typeof<'T>
            let isValueType = tIn.IsValueType && isNull (Nullable.GetUnderlyingType tIn)

            for i in 0 .. len - 1 do
                if inputArray.IsNull i && isValueType then
                    buffer.Add null
                else
                    let rawVal = rawGetter.Invoke i
                    let inputVal = unbox<'T> rawVal 
                    
                    let outputVal = f inputVal
                    
                    buffer.Add outputVal

            buffer.BuildArray()
        )

    // ==========================================
    // 2. Option Map (T option -> U option)
    // ==========================================
    let mapOption (f: 'T option -> 'U option) : Func<IArrowArray, IArrowArray> =
        Func<IArrowArray, IArrowArray>(fun inputArray ->
            let len = inputArray.Length
            
            // 1. Reader
            let rawGetter = ArrowReader.CreateAccessor(inputArray, typeof<'T>)
            
            // 2. Writer 
            let buffer = ColumnBufferFactory.Create(typeof<'U>, len)

            for i in 0 .. len - 1 do
                let rawVal = rawGetter.Invoke i
                
                let inputOpt = 
                    if isNull rawVal then None 
                    else Some (unbox<'T> rawVal)
                
                let outputOpt = f inputOpt
                
                match outputOpt with
                | Some v -> buffer.Add v
                | None -> buffer.Add null

            buffer.BuildArray()
        )

    // ==========================================
    // 3. ValueOption Map (T voption -> U voption)
    // ==========================================
    let mapValueOption (f: 'T voption -> 'U voption) : Func<IArrowArray, IArrowArray> =
        Func<IArrowArray, IArrowArray>(fun inputArray ->
            let len = inputArray.Length
            
            // 1. Reader
            let rawGetter = ArrowReader.CreateAccessor(inputArray, typeof<'T>)
            
            // 2. Writer 
            let buffer = ColumnBufferFactory.Create(typeof<'U>, len)

            for i in 0 .. len - 1 do
                let rawVal = rawGetter.Invoke i
                
                let inputVOpt = 
                    if isNull rawVal then ValueNone 
                    else ValueSome (unbox<'T> rawVal)
                
                let outputVOpt = f inputVOpt
                
                match outputVOpt with
                | ValueSome v -> buffer.Add v
                | ValueNone -> buffer.Add null

            buffer.BuildArray()
        )