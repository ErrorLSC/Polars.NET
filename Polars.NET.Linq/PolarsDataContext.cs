using System.Reflection;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using Microsoft.FSharp.Core;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Linq;

/// <summary>
/// Represents the primary data context for Polars.NET LINQ operations.
/// It serves as the gateway between .NET LINQ expressions and the Polars SQL execution engine.
/// </summary>
/// <remarks>
/// <para>
/// By inheriting from <see cref="DataConnection"/>, this class enables <c>linq2db</c> to 
/// treat Polars as a relational database. It manages table registrations, schema mappings, 
/// and the routing of generated SQL queries to the underlying Rust core.
/// </para>
/// <para>
/// This context should be used within a <c>using</c> block to ensure that resources 
/// associated with the underlying <see cref="IPolarsSqlContext"/> and ADO.NET abstractions 
/// are properly released.
/// </para>
/// </remarks>
public class PolarsDataContext : DataConnection, IDisposable
{
    private readonly IPolarsSqlContext _polarsContext;
    private readonly bool _ownsContext; 
    /// <summary>
    /// Initializes a new instance of the <see cref="PolarsDataContext"/> class.
    /// </summary>
    /// <param name="polarsContext">The underlying Polars SQL context where dataframes are registered.</param>
    /// <param name="ownsContext">
    /// If <see langword="true"/>, the <see cref="PolarsDataContext"/> will dispose of the 
    /// Polars SQL context when it is disposed.
    /// </param>
    public PolarsDataContext(IPolarsSqlContext polarsContext, bool ownsContext = false) 
        : base(CreateOptions(polarsContext))
    {
        InlineParameters = true;
        _polarsContext = polarsContext;
        _ownsContext = ownsContext;
    }
    private static DataOptions CreateOptions(IPolarsSqlContext polarsContext)
    {
        var dataProvider = LinqToDB.DataProvider.PostgreSQL.PostgreSQLTools.GetDataProvider(
            LinqToDB.DataProvider.PostgreSQL.PostgreSQLVersion.v15);
        var mockConn = new PolarsDbConnection(polarsContext);
        
        return new DataOptions()
            .UseConnection(dataProvider, mockConn)
            .WithOptions<SqlOptions>(o => o with { GenerateFinalAliases = true });
    }

    private void BuildSchemaMapping<T>(string tableName, IPolarsSchema schema) where T : class
    {
        var schemaDict = schema.ToDictionary();
        
        try
        {
            var mappingBuilder = new FluentMappingBuilder(this.MappingSchema);
            var entityBuilder = mappingBuilder.Entity<T>();
            var properties = typeof(T).GetProperties();

            foreach (var prop in properties)
            {
                var matchedColumn = schemaDict.Keys.FirstOrDefault(k => 
                    string.Equals(k, prop.Name, StringComparison.OrdinalIgnoreCase));

                if (matchedColumn != null)
                {
                    var polarsDataType = schemaDict[matchedColumn];
                    var expectedNetType = ArrowTypeResolver.GetNetTypeFromArrowType(polarsDataType.GetArrowType());
                    
                    var actualNetType = prop.PropertyType;
                    
                    // Unwrap C# Nullable<T>
                    var nullableUnderlying = Nullable.GetUnderlyingType(actualNetType);
                    if (nullableUnderlying != null)
                    {
                        actualNetType = nullableUnderlying;
                    }
                    // Unwrap F#Option<T> & ValueOption<T>
                    else if (actualNetType.IsGenericType)
                    {
                        var genericDef = actualNetType.GetGenericTypeDefinition();

                        if (genericDef == typeof(FSharpOption<>) || genericDef == typeof(FSharpValueOption<>))
                        {
                            actualNetType = actualNetType.GetGenericArguments()[0];
                        }
                    }
                    // Schema Check
                    if (expectedNetType != actualNetType && expectedNetType != typeof(object))
                    {
                        throw new InvalidOperationException(
                            $"[Polars.NET] Table: '{tableName}' Column mapping failed.\n" +
                            $"Polars Column: '{matchedColumn}' 's type is {polarsDataType}, its Dotnet type is '{expectedNetType.Name}'.\n" +
                            $"But your model {typeof(T).Name}.{prop.Name} defined as '{actualNetType.Name}' (unwrapped). Please modify your record or cast to correct schema.");
                    }

                    entityBuilder.HasAttribute(prop, new ColumnAttribute { Name = matchedColumn });
                }
            }
            mappingBuilder.Build();
        }
        finally
        {
            foreach(var dt in schemaDict.Values) { dt.Dispose(); }
        }
    }
    // ====================================================================
    // LazyFrame Register
    // ====================================================================
    /// <summary>
    /// Registers a <see cref="IPolarsLazyFrame"/> as a queryable table within the current context.
    /// This method automatically orchestrates the mapping between the .NET type <typeparamref name="T"/> 
    /// and the underlying Polars schema.
    /// </summary>
    /// <typeparam name="T">The class or record type that represents the table structure.</typeparam>
    /// <param name="tableName">The identifier of the table to be used in LINQ and SQL expressions.</param>
    /// <param name="lf">The <see cref="IPolarsLazyFrame"/> containing the data source and its computation plan.</param>
    /// <param name="providedSchema">
    /// Optional. A specific schema to override the default. If <see langword="null"/>, the schema is 
    /// automatically inferred from the provided <paramref name="lf"/>.
    /// </param>
    /// <returns>An <see cref="ITable{T}"/> instance ready for fluent LINQ querying.</returns>
    /// <summary>
    /// Core registration method. If tableName is null or empty, a unique random name is generated.
    /// </summary>
    public ITable<T> RegisterTable<T>(IPolarsLazyFrame lf,string? tableName=null,  IPolarsSchema? providedSchema = null) 
        where T : class
    {
        var actualTableName = string.IsNullOrWhiteSpace(tableName) 
            ? $"tmp_{Guid.NewGuid():N}" 
            : tableName;

        var schema = providedSchema ?? lf.Schema; 
        
        BuildSchemaMapping<T>(actualTableName, schema); 
        _polarsContext.Register(actualTableName, lf);   
        
        return this.GetTable<T>().TableName(actualTableName);
    }
    /// <summary>
    /// Registers a <see cref="IPolarsLazyFrame"/> using a dummy collection to infer the generic type <typeparamref name="T"/>.
    /// </summary>
    /// <remarks>
    /// This overload is particularly useful in F# when working with anonymous records, 
    /// as it allows the compiler to resolve the complex generic type without manual specification.
    /// </remarks>
    /// <typeparam name="T">The class or record type (usually inferred from <paramref name="dummy"/>).</typeparam>
    /// <param name="tableName">The name of the table in the SQL context.</param>
    /// <param name="lf">The source <see cref="IPolarsLazyFrame"/>.</param>
    /// <param name="dummy">A collection (usually the source data) used solely for type inference.</param>
    /// <returns>An <see cref="ITable{T}"/> for LINQ querying.</returns>
    public ITable<T> RegisterTable<T>(IPolarsLazyFrame lf, IEnumerable<T> dummy,string? tableName=null)
        where T : class 
        => RegisterTable<T>(lf,tableName);
    /// <summary>
    /// Registers a Polars LazyFrame as a queryable table in the current DataContext.
    /// The table name is automatically inferred from the <c>[Table]</c> attribute on type <typeparamref name="T"/>.
    /// If the attribute is missing, it falls back to using the class name.
    /// </summary>
    /// <typeparam name="T">The entity type representing the schema. Must be a class.</typeparam>
    /// <param name="lazyFrame">The logical plan (LazyFrame) to register.</param>
    /// <returns>A LINQ-enabled <see cref="IQueryable{T}"/> ready for further querying.</returns>
    public IQueryable<T> RegisterTable<T>(IPolarsLazyFrame lazyFrame)
        where T : class
    {
        // 1. Attempt to extract the Linq2DB TableAttribute
        var tableAttr = typeof(T).GetCustomAttribute<TableAttribute>();
        
        // 2. Fallback strategy: Attribute Name -> Class Name
        string tableName = !string.IsNullOrWhiteSpace(tableAttr?.Name) 
            ? tableAttr.Name 
            : typeof(T).Name;

        // 3. Delegate to the underlying explicit registration method
        return RegisterTable<T>(lazyFrame,tableName);
    }
    // ====================================================================
    // DataFrame Register
    // ====================================================================
    /// <summary>
    /// Core registration method. If tableName is null or empty, a unique random name is generated.
    /// </summary>
    public ITable<T> RegisterTable<T>(IPolarsDataFrame df,string? tableName=null,  IPolarsSchema? providedSchema = null) 
        where T : class
    {
        var actualTableName = string.IsNullOrWhiteSpace(tableName) 
            ? $"tmp_{Guid.NewGuid():N}" 
            : tableName;

        var schema = providedSchema ?? df.Schema; 
        
        BuildSchemaMapping<T>(actualTableName, schema); 
        _polarsContext.Register(actualTableName, df);   
        
        return this.GetTable<T>().TableName(actualTableName);
    }
    /// <summary>
    /// Registers an eager <see cref="IPolarsDataFrame"/> using a dummy collection to simplify generic type inference.
    /// </summary>
    /// <remarks>
    /// This is the preferred overload for F# anonymous records, enabling the compiler to automatically 
    /// determine the structure of <typeparamref name="T"/> without explicit type annotations.
    /// </remarks>
    /// <typeparam name="T">The class or record type (inferred from <paramref name="dummyDataForInference"/>).</typeparam>
    /// <param name="tableName">The name of the table in the SQL context.</param>
    /// <param name="df">The source <see cref="IPolarsDataFrame"/>.</param>
    /// <param name="dummyDataForInference">A sample collection used only to guide the compiler's type inference.</param>
    /// <returns>An <see cref="ITable{T}"/> for LINQ querying.</returns>
    public ITable<T> RegisterTable<T>(IPolarsDataFrame df,  IEnumerable<T> dummyDataForInference,string? tableName=null) where T : class
        => RegisterTable<T>(df,tableName);

    /// <summary>
    /// Registers an eagerly materialized Polars DataFrame as a queryable table in the current DataContext.
    /// The table name is automatically inferred from the <c>[Table]</c> attribute on type <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The entity type representing the schema. Must be a class.</typeparam>
    /// <param name="dataFrame">The materialized DataFrame to register.</param>
    /// <returns>A LINQ-enabled <see cref="IQueryable{T}"/> ready for further querying.</returns>
    public IQueryable<T> RegisterTable<T>(IPolarsDataFrame dataFrame)
     where T : class
    {
        var tableAttr = typeof(T).GetCustomAttribute<TableAttribute>();
        
        string tableName = !string.IsNullOrWhiteSpace(tableAttr?.Name) 
            ? tableAttr.Name 
            : typeof(T).Name;

        return RegisterTable<T>(dataFrame,tableName);
    }
    // ====================================================================
    // Series Register
    // ====================================================================
    /// <summary>
    /// Registers a single <see cref="IPolarsSeries"/> as a queryable virtual table.
    /// This allows performing LINQ operations on a standalone Series as if it were a single-column collection.
    /// </summary>
    /// <typeparam name="T">The underlying .NET type of the series elements (e.g., int, double, string).</typeparam>
    /// <param name="s">The <see cref="IPolarsSeries"/> to register.</param>
    /// <returns>
    /// An <see cref="IQueryable{T}"/> representing the elements of the series.
    /// </returns>
    /// <remarks>
    /// This method performs a "Zero Side-Effect" normalization:
    /// <list type="bullet">
    ///   <item>It temporarily renames the series to <c>"value"</c> to ensure consistent mapping with <see cref="SeriesWrapper{T}"/>.</item>
    ///   <item>It restores the original name immediately after creating the internal DataFrame source.</item>
    ///   <item>If the series lacks a name, a unique GUID-based table name is generated automatically.</item>
    /// </list>
    /// </remarks>
    public IQueryable<T> RegisterSeries<T>(IPolarsSeries s)
    {
        ValidateSeriesArrowType<T>(s);

        var originalSeriesName = s.Name;
        
        var tableName = string.IsNullOrEmpty(originalSeriesName) 
            ? $"series_{Guid.NewGuid():N}" 
            : originalSeriesName;

        IPolarsDataFrame df;
        
        // ==========================================
        // Zero Side-Effect
        // ==========================================
        try
        {
            s.Rename("value"); 

            df = s.ToFrame(); 
        }
        finally
        {
            s.Rename(originalSeriesName);
        }

        _polarsContext.Register(tableName, df);

        return this.GetTable<SeriesWrapper<T>>()
                    .TableName(tableName)
                    .Select(row => row.Value); 
    }


    // ====================================================================
    // Type Defender
    // ====================================================================
    private static void ValidateSeriesArrowType<T>(IPolarsSeries s)
    {
        var arrowType = s.DataType.GetArrowType(); 

        Type expectedNetType = ArrowTypeResolver.GetNetTypeFromArrowType(arrowType);
        
        Type userType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

        if (userType != expectedNetType && expectedNetType != typeof(object))
        {
            throw new InvalidOperationException(
                $"[Polars.NET.Linq] Series Dtype mismatch: \n" +
                $"Series '{s.Name}' Arrow Type is {arrowType.GetType().Name}" +
                $"It can only be queried as {expectedNetType.Name} or its nullable type。\n" +
                $"But you tried to RegisterSeries<{userType.Name}>(), which will lead to fatal errors");
        }
    }
    internal IPolarsLazyFrame ExecuteToLazyFrame(string sql)
        => _polarsContext.Execute(sql);

    // ====================================================================
    // Dispose
    // ====================================================================
    
    private bool _disposed; 
    /// <summary>
    /// Dispose unmanaged resource
    /// </summary>
    public new void Dispose()
    {
        ((IDisposable)this).Dispose();
    }

    void IDisposable.Dispose()
    {
        if (_disposed) return;

        if (_ownsContext)
        {
            _polarsContext?.Dispose();
        }
        
        base.Dispose();
        
        _disposed = true;

        GC.SuppressFinalize(this); 
    }
}

internal class SeriesWrapper<T>
{
    [Column("value")]
    public required T Value { get; set; }
}
