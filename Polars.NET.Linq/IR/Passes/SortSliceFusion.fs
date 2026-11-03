namespace Polars.NET.Linq.IR.Passes

open System
open Polars.NET.Linq.IR

/// Optimization pass that fuses consecutive Sort and Limit (Slice) operators.
module internal SortSliceFusion =

    /// Fuses consecutive Limit nodes and eliminates identity slices.
    let rec fuseLimit (plan: LogicalPlan) : LogicalPlan =
        match plan with
        // Identity slice: skip 0, take all -> eliminate
        | LogicalPlan.Limit(input, 0L, count) when count = UInt32.MaxValue ->
            fuseLimit input

        // Consecutive limits: Limit(Limit(input, o1, c1), o2, c2)
        | LogicalPlan.Limit(LogicalPlan.Limit(innerInput, innerOffset, innerCount), outerOffset, outerCount) ->
            let fusedInput = fuseLimit innerInput
            let newOffset = innerOffset + outerOffset
            let newCount =
                if outerOffset >= int64 innerCount then
                    0u
                else
                    let remainingInner = innerCount - uint32 outerOffset
                    min remainingInner outerCount
            LogicalPlan.Limit(fusedInput, newOffset, newCount)

        // Empty sort: eliminate
        | LogicalPlan.Sort(input, []) ->
            fuseLimit input

        // Consecutive sorts with no intermediate limit/grouping:
        // Merge orderings preserving priority of earlier orderings
        | LogicalPlan.Sort(LogicalPlan.Sort(innerInput, innerOrders), outerOrders) ->
            let fusedInput = fuseLimit innerInput
            // Keep unique sort expressions (first occurrence wins)
            let seen = System.Collections.Generic.HashSet<string>()
            let combined =
                outerOrders @ innerOrders
                |> List.filter (fun o ->
                    let key = sprintf "%A" o.Expr
                    seen.Add key)
            LogicalPlan.Sort(fusedInput, combined)

        // Recursive tree mapping
        | other ->
            LogicalPlanOps.mapInputs fuseLimit other

    /// Entry point for Sort & Slice fusion pass.
    let apply (plan: LogicalPlan) : LogicalPlan =
        fuseLimit plan

