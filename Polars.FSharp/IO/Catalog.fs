namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module UnityCatalogExtensions =

    type LazyFrame with
        /// <summary>
        /// Starts building a Merge (Upsert) operation using this <see cref="LazyFrame"/> as the source.
        /// </summary>
        member this.MergeCatalogRecords(
            catalog: UnityCatalog,
            catalogName: string,
            schemaName: string,
            tableName: string,
            mergeKeys: string array,
            ?canEvolve: bool,
            ?cloudOptions: CloudOptions) =
            
            catalog.MergeCatalogRecords(
                catalogName, 
                schemaName, 
                tableName, 
                this, 
                mergeKeys, 
                ?canEvolve = canEvolve, 
                ?cloudOptions = cloudOptions
            )

    type DataFrame with
        /// <summary>
        /// Starts building a Merge (Upsert) operation using this <see cref="DataFrame"/> as the source.
        /// </summary>
        member this.MergeCatalogRecords(
            catalog: UnityCatalog,
            catalogName: string,
            schemaName: string,
            tableName: string,
            mergeKeys: string array,
            ?canEvolve: bool,
            ?cloudOptions: CloudOptions) =
            
            catalog.MergeCatalogRecords(
                catalogName, 
                schemaName, 
                tableName, 
                this, 
                mergeKeys, 
                ?canEvolve = canEvolve, 
                ?cloudOptions = cloudOptions
            )

    type LazyFrame with
        /// <summary>
        /// Sinks the <see cref="LazyFrame"/> to a Unity Catalog table.
        /// </summary>
        member this.SinkCatalogTable(
            catalog: UnityCatalog,
            catalogName: string,
            schemaName: string,
            tableName: string,
            ?partitionBy: Selector,
            ?mode: DeltaSaveMode,
            ?canEvolve: bool,
            ?includeKeys: bool,
            ?keysPreGrouped: bool,
            ?maxRowsPerFile: int,
            ?approxBytesPerFile: int64,
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: uint32,
            ?dataPageSize: uint32,
            ?compatLevel: int,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions) =
            
            catalog.SinkCatalogTable(
                catalogName, 
                schemaName, 
                tableName, 
                this,
                ?partitionBy = partitionBy,
                ?mode = mode,
                ?canEvolve = canEvolve,
                ?includeKeys = includeKeys,
                ?keysPreGrouped = keysPreGrouped,
                ?maxRowsPerFile = maxRowsPerFile,
                ?approxBytesPerFile = approxBytesPerFile,
                ?compression = compression,
                ?compressionLevel = compressionLevel,
                ?statistics = statistics,
                ?rowGroupSize = rowGroupSize,
                ?dataPageSize = dataPageSize,
                ?compatLevel = compatLevel,
                ?maintainOrder = maintainOrder,
                ?syncOnClose = syncOnClose,
                ?mkdir = mkdir,
                ?cloudOptions = cloudOptions
            )

    type DataFrame with
        /// <summary>
        /// Writes the <see cref="DataFrame"/> into a Unity Catalog table by converting it to a <see cref="LazyFrame"/>.
        /// </summary>
        member this.WriteCatalogTable(
            catalog: UnityCatalog,
            catalogName: string,
            schemaName: string,
            tableName: string,
            ?partitionBy: Selector,
            ?mode: DeltaSaveMode,
            ?canEvolve: bool,
            ?includeKeys: bool,
            ?keysPreGrouped: bool,
            ?maxRowsPerFile: int,
            ?approxBytesPerFile: int64,
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: uint32,
            ?dataPageSize: uint32,
            ?compatLevel: int,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions) =
            
            catalog.SinkCatalogTable(
                catalogName, 
                schemaName, 
                tableName, 
                this.Lazy(),
                ?partitionBy = partitionBy,
                ?mode = mode,
                ?canEvolve = canEvolve,
                ?includeKeys = includeKeys,
                ?keysPreGrouped = keysPreGrouped,
                ?maxRowsPerFile = maxRowsPerFile,
                ?approxBytesPerFile = approxBytesPerFile,
                ?compression = compression,
                ?compressionLevel = compressionLevel,
                ?statistics = statistics,
                ?rowGroupSize = rowGroupSize,
                ?dataPageSize = dataPageSize,
                ?compatLevel = compatLevel,
                ?maintainOrder = maintainOrder,
                ?syncOnClose = syncOnClose,
                ?mkdir = mkdir,
                ?cloudOptions = cloudOptions
            )
    