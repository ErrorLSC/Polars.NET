namespace Polars.FSharp.Tests

open System.Collections.Generic
open Xunit
open Polars.FSharp
open Polars.FSharp.Query

type MapRow = { Id: int; Tags: Map<string, int64> }
type OptionalMapRow = { Id: int; Tags: Map<string, int64> option }
type ScoreRow = { Id: int; Marks: Map<string, int> }

/// The Polars 2.2 Map type from F#: DataType.Map, F# Map values in and out, map literals, selectors.
type MapTypeTests() =

    [<Fact>]
    member _.``Map type has its kind, parts and display`` () =
        use map = DataType.Map(DataType.String, DataType.Int64)

        match map.Kind with
        | DataTypeKind.Map(key, value) ->
            Assert.Equal(DataType.String, key)
            Assert.Equal(DataType.Int64, value)
        | other -> failwithf "expected a Map kind, got %A" other

        Assert.Equal("map[str, i64]", map.ToString())
        Assert.Equal(Some DataType.String, map.MapKeyType)
        Assert.Equal(Some DataType.Int64, map.MapValueType)
        Assert.True map.IsNested
        Assert.Equal(29, map.Code)
        Assert.Equal(None, DataType.Int64.MapKeyType)
        Assert.Equal("map[str, map[i32, f64]]", (pl.map DataType.String (pl.map DataType.Int32 DataType.Float64)).ToString())

    [<Fact>]
    member _.``F# Map values make a Map series and read back`` () =
        use s = Series.create ("m", [ Map [ "a", 1L; "b", 2L ]; Map.empty ])

        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), s.DataType)
        Assert.Equal<Map<string, int64>>(Map [ "a", 1L; "b", 2L ], s.GetValue<Map<string, int64>> 0)
        Assert.Equal<Map<string, int64>>(Map.empty, s.GetValue<Map<string, int64>> 1)
        Assert.Equal(2, s.GetValue<Dictionary<string, int64>>(0).Count)
        Assert.Equal<Map<string, int64>[]>([| Map [ "a", 1L; "b", 2L ]; Map.empty |], s.ToArray<Map<string, int64>>())

    [<Fact>]
    member _.``Records with Map members round-trip`` () =
        let rows: MapRow list = [ { Id = 1; Tags = Map [ "x", 10L ] }; { Id = 2; Tags = Map.empty } ]
        use df = DataFrame.ofRecords rows

        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), df.["Tags"].DataType)
        Assert.Equal<MapRow list>(rows, df.ToRecordList<MapRow>())

        let optional: OptionalMapRow list = [ { Id = 1; Tags = Some(Map [ "y", 3L ]) }; { Id = 2; Tags = None } ]
        use dfOpt = DataFrame.ofRecords optional
        Assert.Equal(1L, dfOpt.["Tags"].NullCount)
        Assert.Equal<OptionalMapRow list>(optional, dfOpt.ToRecordList<OptionalMapRow>())

    [<Fact>]
    member _.``A map literal is one map value`` () =
        use df = DataFrame.create [ Series.create ("id", [| 1; 2 |]) ]
        use res = df |> DataFrame.select [ pl.col "id"; (pl.litMap (Map [ "k", 5L ])).Alias "m" ]

        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), res.["m"].DataType)
        Assert.Equal<Map<string, int64>[]>([| Map [ "k", 5L ]; Map [ "k", 5L ] |], res.["m"].ToArray<Map<string, int64>>())

    [<Fact>]
    member _.``The map selector picks Map columns`` () =
        use df =
            DataFrame.create
                [ Series.create ("id", [| 1 |])
                  Series.create ("m", [ Map [ 1, "one" ] ])
                  Series.create ("l", [ [| 1 |] ]) ]

        use picked = df |> DataFrame.select [ pl.cs.map().ToExpr() ]
        Assert.Equal<string seq>([ "m" ], picked.Columns)
        Assert.Equal(DataType.Map(DataType.Int32, DataType.String), picked.["m"].DataType)

    [<Fact>]
    member _.``MapOps look up, count and list entries`` () =
        use df =
            DataFrame.create
                [ Series.create ("m", [ Map [ "a", 1L; "b", 2L ]; Map.empty; Map [ "c", 3L ] ])
                  Series.create ("k", [| "b"; "a"; "x" |]) ]

        use res =
            df
            |> DataFrame.select
                [ (pl.col "m").MapOps.Get("a").Alias "a"
                  (pl.col "m").MapOps.Get(pl.col "k").Alias "byColumn"
                  (pl.col "m").MapOps.ContainsKey("c").Alias "hasC"
                  (pl.col "m").MapOps.Len().Alias "len"
                  (pl.col "m").MapOps.Keys().Alias "keys" ]

        Assert.Equal<int64 option[]>([| Some 1L; None; None |], res.["a"].ToArrayOption<int64>())
        Assert.Equal<int64 option[]>([| Some 2L; None; None |], res.["byColumn"].ToArrayOption<int64>())
        Assert.Equal<bool[]>([| false; false; true |], res.["hasC"].ToArray<bool>())
        Assert.Equal<uint32[]>([| 2u; 0u; 1u |], res.["len"].ToArray<uint32>())
        Assert.Equal<string[]>([| "a"; "b" |], res.["keys"].GetValue<string[]> 0)

        // The series namespace, and List.ToMap back from the entries.
        use s = df.["m"]
        Assert.Equal<int64 option[]>([| None; None; Some 3L |], s.MapOps.Get("c").ToArrayOption<int64>())
        use back = s.MapOps.Entries().List.ToMap()
        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), back.DataType)
        Assert.Equal<Map<string, int64>[]>(s.ToArray<Map<string, int64>>(), back.ToArray<Map<string, int64>>())

    [<Fact>]
    member _.``MapOps take integer keys`` () =
        use s = Series.create ("m", [ Map [ 1, "one"; 2, "two" ] ])
        Assert.Equal("two", s.MapOps.Get(2).GetValue<string> 0)
        Assert.True(s.MapOps.ContainsKey(1).GetValue<bool> 0)

    [<Fact>]
    member _.``Nested naive datetimes keep no time zone`` () =
        // The native side reads "no time zone" as an empty string, which is not a zone named "".
        use s = Series.create ("l", [ [| System.DateTime(2024, 1, 2) |] ])
        Assert.Equal(DataType.List(DataType.Datetime TimeUnit.Microseconds), s.DataType)
        Assert.Equal("list[datetime[μs]]", s.DataType.ToString())
        use m = Series.create ("m", [ Map [ "a", System.DateTime(2024, 1, 2) ] ])
        Assert.Equal(DataType.Map(DataType.String, DataType.Datetime TimeUnit.Microseconds), m.DataType)

    [<Fact>]
    member _.``ofMaps takes nested maps of objects as records and typed maps as maps`` () =
        let rows: Map<string, obj> list =
            [ Map [ "id", box 1L; "r", box (Map [ "a", box 1L ]); "t", box (Map [ "k", 2L ]) ]
              Map [ "id", box 2L; "r", box (Map [ "b", box "x" ]); "t", box (Map.empty<string, int64>) ] ]
        use df = DataFrame.ofMaps rows

        Assert.Equal(DataType.Struct [ { Name = "a"; DataType = DataType.Int64 }; { Name = "b"; DataType = DataType.String } ], df.["r"].DataType)
        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), df.["t"].DataType)
        Assert.Equal<Map<string, int64>[]>([| Map [ "k", 2L ]; Map.empty |], df.["t"].ToArray<Map<string, int64>>())


    [<Fact>]
    member _.``F# Map members run in queries as over a list`` () =
        let rows =
            [| { Id = 1; Marks = Map [ "math", 90; "art", 70 ] }
               { Id = 2; Marks = Map [ "music", 80 ] }
               { Id = 3; Marks = Map.empty } |]
        use df = DataFrame.ofRecords rows
        let source = df.AsQueryable rows

        let filtered =
            query {
                for r in source do
                where (r.Marks.ContainsKey "math" && r.Marks.["math"] > 50)
                select r.Id
            }
        // Run in the engine, not in .NET.
        let plan = filtered.Explain()
        Assert.Contains("map.contains_key", plan)
        Assert.Contains("map.get", plan)
        Assert.Equal<int list>([ 1 ], filtered |> Seq.toList)

        let counts = query { for r in source do select r.Marks.Count } |> Seq.toList
        Assert.Equal<int list>([ 2; 1; 0 ], counts)

        // TryFind: Some value, None for a missing key.
        let tried = query { for r in source do select (r.Marks.TryFind "music") }
        Assert.Contains("map.get", tried.Explain())
        Assert.Equal<int option list>([ None; Some 80; None ], tried |> Seq.toList)
