namespace Polars.NET.Linq.IR

/// Collects the sources a LogicalPlan scans while it is built. Scan nodes refer to sources by slot only,
/// so the finished plan carries no source instances; the bindings travel beside it.
[<Sealed>]
type internal SourceRegistry() =
    let bindings = ResizeArray<SourceBinding>()

    /// Renumbers every Scan slot of a plan by the given offset.
    static member OffsetSlots (offset: int) (plan: LogicalPlan) : LogicalPlan =
        if offset = 0 then
            plan
        else
            plan
            |> LogicalPlanOps.rewrite (function
                | LogicalPlan.Scan(LogicalPlanSource.Slot index, schema) ->
                    Some(LogicalPlan.Scan(LogicalPlanSource.Slot(index + offset), schema))
                | _ -> None)

    /// Registers a source and returns a Scan over its new slot.
    member _.Scan(binding: SourceBinding, schema: LogicalSchema) : LogicalPlan =
        bindings.Add binding
        LogicalPlan.Scan(LogicalPlanSource.Slot(bindings.Count - 1), schema)

    /// Adopts a plan compiled against its own bindings, renumbering its slots into this registry.
    /// Shared ids are only unique within the plan that declared them, so its Shared nodes are unwrapped; the
    /// plan being built finds its common subplans again once it is complete.
    member _.Graft(plan: LogicalPlan, sources: SourceBinding array) : LogicalPlan =
        let offset = bindings.Count
        bindings.AddRange sources
        plan
        |> LogicalPlanOps.rewrite (function
            | LogicalPlan.Shared(_, input) -> Some input
            | _ -> None)
        |> SourceRegistry.OffsetSlots offset

    /// The bindings registered so far, indexed by slot.
    member _.ToArray() : SourceBinding array = bindings.ToArray()
