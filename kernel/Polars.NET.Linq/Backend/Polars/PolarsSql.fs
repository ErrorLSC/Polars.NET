namespace Polars.NET.Linq.Backend.Polars

open Polars.NET.Core
open Kaminari.Samidare.IR
open Kaminari.Samidare.IR.Passes
open Kaminari.Samidare.Config
open Kaminari.Samidare.Sql

/// SQL queries on Polars: the SQL frontend's plan (Samidare.Sql) optimized as a LINQ query's is, and lowered.
module internal PolarsSql =

    /// The optimized IR plan of a SQL query over the catalog's tables, and the sources its Scan slots read.
    let plan (catalog: ISqlCatalog) (text: string) : LogicalPlan * SourceBinding array =
        let backend = PolarsQueryBackend.Instance
        let sql = SqlQueries.translate catalog backend.SupportsAssertions text
        // The frontend shares a common table expression's plan among its references; their filters move down to it
        // first (a shared plan is a barrier to pushdown), constant ones drop the branches of unions they empty, and the
        // shared plan is filtered by what its references read. The rest of the optimization comes after.
        let optimized =
            EnumResolution.resolvePlan sql.Plan
            |> FilterNullSimplification.apply
            |> PredicatePushdown.apply
            |> ConstantFilterFolding.apply
            |> SharedConsumerFilters.apply
            |> LogicalOptimizer.optimizeComplete SamidareConfig.AllowUnstableOrdering SamidareConfig.CollectColumnStatistics sql.Sources
        optimized, sql.Sources

    /// A SQL query over the catalog's tables as a LazyFrame.
    let lazyFrame (catalog: ISqlCatalog) (text: string) : LazyFrameHandle =
        let optimized, sources = plan catalog text
        PolarsPhysicalLowerer.lowerPlan sources optimized
