namespace Kaminari.Samidare

open System
open System.Collections.Generic
open System.Runtime.CompilerServices

/// F# options for C# callers (part of Samidare's facade for C#): a value
/// or null, a nullable value type, and options made from values.
[<AbstractClass; Sealed; Extension>]
type IrOption =
    /// The value, or null when there is none.
    [<Extension>]
    static member OrNull(option: 'T option) : 'T =
        match option with
        | Some value -> value
        | None -> Unchecked.defaultof<'T>

    /// The value as a nullable value type.
    [<Extension>]
    static member ToNullable(option: 'T option) : Nullable<'T> = Option.toNullable option

    /// Whether there is a value (an F# None is null to C#).
    [<Extension>]
    static member HasValue(option: 'T option) : bool = option.IsSome

    /// An option holding `value`.
    static member Some(value: 'T) : 'T option = Some value

    /// An option holding `value`, none for null.
    static member OfObj(value: 'T) : 'T option when 'T: null = Option.ofObj value

    /// An option holding the nullable's value, none without one.
    static member OfNullable(value: Nullable<'T>) : 'T option = Option.ofNullable value

/// F# lists for C# callers: an F# list is an IReadOnlyList already; these build one.
[<AbstractClass; Sealed>]
type IrList =
    /// A list of `items`, in order.
    static member Of([<ParamArray>] items: 'T array) : 'T list = List.ofArray items

    /// A list of `items`, in order.
    static member OfSeq(items: IEnumerable<'T>) : 'T list = List.ofSeq items

    /// The empty list.
    static member Empty<'T>() : 'T list = []
