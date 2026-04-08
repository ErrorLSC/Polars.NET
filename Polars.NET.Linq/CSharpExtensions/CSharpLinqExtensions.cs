using System.Reflection;
using Apache.Arrow.Adbc;
using LinqToDB;
using LinqToDB.Mapping;
using Polars.CSharp;
using Polars.NET.Core;

namespace Polars.NET.Linq.CSharpExtensions;

/// <summary>
/// Extension method for Polars.CSharp
/// </summary>
public static class CSharpLinqExtensions
{
    /// <summary>
    /// Translates and eagerly executes the LINQ query, materializing the results into a fully computed Polars Series.
    /// </summary>
    public static Polars.CSharp.Series ToSeries<T>(this IQueryable<T> query,string seriesName = "")
    {
        IPolarsSeries coreInterface = PolarsQueryableExtensions.ToISeries(query, seriesName);
        return coreInterface.AsSeries();
    }

    /// <summary>
    /// Translates the LINQ query into a Polars LazyFrame. 
    /// This builds the logical execution plan (LINQ -> SQL -> Polars) without eagerly materializing the data.
    /// </summary>
    public static Polars.CSharp.LazyFrame ToLazyFrame<T>(this IQueryable<T> query)
    {
        IPolarsLazyFrame coreInterface = PolarsQueryableExtensions.ToILazyFrame(query);
        return coreInterface.AsLazyFrame();
    }

    /// <summary>
    /// Translates and eagerly executes the LINQ query, materializing the results into a fully computed Polars DataFrame.
    /// </summary>
    public static Polars.CSharp.DataFrame ToDataFrame<T>(this IQueryable<T> query,Engine engine=Engine.Auto,bool useStreaming=false)
        => query.ToLazyFrame().Collect(engine,useStreaming);

    /// <summary>
    /// Asynchronously translates the LINQ query into a Polars LazyFrame. 
    /// This builds the logical execution plan without eagerly materializing the data.
    /// </summary>
    public static async Task<Polars.CSharp.LazyFrame> ToLazyFrameAsync<T>(
        this IQueryable<T> query, 
        CancellationToken cancellationToken = default)
    {
        IPolarsLazyFrame coreInterface = await PolarsQueryableExtensions
            .ToILazyFrameAsync(query, cancellationToken)
            .ConfigureAwait(false);
            
        return coreInterface.AsLazyFrame();
    }

    /// <summary>
    /// Asynchronously translates and eagerly executes the LINQ query, 
    /// materializing the results into a fully computed Polars DataFrame.
    /// </summary>
    public static async Task<Polars.CSharp.DataFrame> ToDataFrameAsync<T>(
        this IQueryable<T> query, 
        Engine engine=Engine.Auto,
        bool useStreaming = false,
        CancellationToken cancellationToken = default)
    {
        IPolarsDataFrame coreInterface = await PolarsQueryableExtensions
            .ToIDataFrameAsync(query, (PlEngine)engine, useStreaming, cancellationToken)
            .ConfigureAwait(false);
            
        return coreInterface.AsDataFrame();
    }
    /// <summary>
    /// Executes a LINQ query against an ADBC data source and materializes the result into a <see cref="Polars.CSharp.DataFrame"/>.
    /// This method translates the LINQ expression to SQL, injects necessary column aliases to ensure correct property-to-column 
    /// mapping, and executes the query via the provided ADBC connection.
    /// </summary>
    /// <typeparam name="T">The type of the records being queried.</typeparam>
    /// <param name="query">The LINQ query expression to be translated and executed.</param>
    /// <param name="connection">The active <see cref="AdbcConnection"/> used to run the query.</param>
    /// <returns>A <see cref="Polars.CSharp.DataFrame"/> containing the materialized result set.</returns>
    public static Polars.CSharp.DataFrame ToDataFrameAdbc<T>(this IQueryable<T> query, AdbcConnection connection)
    {
        // Get raw SQL
        var rawSql = query.ToSqlQuery().Sql;
        
        // Inject Aliases into SQL
        Type originalType = typeof(T);
        var pushdownSql = PolarsSqlTranslator.InjectAliases(rawSql, originalType);
        
        // Run query
        var dfInterface = DataFrame.ReadAdbc(connection, pushdownSql);
        
        // Convert to DataFrame
        return dfInterface.AsDataFrame();
    }
    /// <summary>
    /// Syntactic sugar to seamlessly convert a Polars DataFrame into a LINQ-enabled <see cref="IQueryable{T}"/>.
    /// It automatically infers the target table name from the <c>[Table]</c> attribute on the entity type.
    /// </summary>
    /// <typeparam name="T">The entity type representing the schema. Must be a class.</typeparam>
    /// <param name="df">The source DataFrame.</param>
    /// <param name="db">The active <see cref="PolarsDataContext"/> to bind the query to.</param>
    /// <returns>A LINQ expression tree root linked to the DataFrame.</returns>
    public static IQueryable<T> AsQueryable<T>(this Polars.CSharp.DataFrame df, PolarsDataContext db)
    where T : class
    {
        var tableAttr = typeof(T).GetCustomAttribute<TableAttribute>();
        string tableName = !string.IsNullOrWhiteSpace(tableAttr?.Name) ? tableAttr.Name : typeof(T).Name;
        
        return db.RegisterTable<T>(df,tableName);
    }

    /// <summary>
    /// Syntactic sugar to seamlessly convert a Polars LazyFrame into a LINQ-enabled <see cref="IQueryable{T}"/>.
    /// It automatically infers the target table name from the <c>[Table]</c> attribute on the entity type.
    /// </summary>
    /// <typeparam name="T">The entity type representing the schema. Must be a class.</typeparam>
    /// <param name="lf">The source logical plan (LazyFrame).</param>
    /// <param name="db">The active <see cref="PolarsDataContext"/> to bind the query to.</param>
    /// <returns>A LINQ expression tree root linked to the LazyFrame.</returns>
    public static IQueryable<T> AsQueryable<T>(this Polars.CSharp.LazyFrame lf, PolarsDataContext db)
    where T : class
    {
        var tableAttr = typeof(T).GetCustomAttribute<TableAttribute>();
        string tableName = !string.IsNullOrWhiteSpace(tableAttr?.Name) ? tableAttr.Name : typeof(T).Name;
        
        return db.RegisterTable<T>(lf,tableName);
    }
    /// <summary>
    /// Syntactic sugar to seamlessly convert a Polars DataFrame into a LINQ-enabled <see cref="IQueryable{T}"/>.
    /// Automatically provisions a transient <see cref="PolarsDataContext"/> under the hood.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this Polars.CSharp.DataFrame df)
    where T : class
    {
        var transientDb = new PolarsDataContext(new SqlContext(), ownsContext: true);
        
        return df.AsQueryable<T>(transientDb);
    }

    /// <summary>
    /// Syntactic sugar to seamlessly convert a Polars LazyFrame into a LINQ-enabled <see cref="IQueryable{T}"/>.
    /// Automatically provisions a transient <see cref="PolarsDataContext"/> under the hood.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this Polars.CSharp.LazyFrame lf)
    where T : class
    {
        var transientDb = new PolarsDataContext(new SqlContext(), ownsContext: true);
        return lf.AsQueryable<T>(transientDb);
    }
    /// <summary>
    /// Syntactic sugar to seamlessly convert a Polars Series into a LINQ-enabled <see cref="IQueryable{T}"/> of scalars.
    /// Evaluates element-wise queries over the 1D array.
    /// </summary>
    /// <typeparam name="T">The scalar type of the Series (e.g., int, double, string).</typeparam>
    /// <param name="series">The source 1D Series.</param>
    /// <param name="db">The active <see cref="PolarsDataContext"/> to bind the query to.</param>
    /// <returns>A LINQ expression tree root linked to the Series values.</returns>
    public static IQueryable<T> AsQueryable<T>(this Polars.CSharp.Series series, PolarsDataContext db)
        => db.RegisterSeries<T>(series);

    /// <summary>
    /// Syntactic sugar to seamlessly convert a Polars Series into a LINQ-enabled <see cref="IQueryable{T}"/> of scalars.
    /// Automatically provisions a transient <see cref="PolarsDataContext"/> under the hood.
    /// </summary>
    /// <typeparam name="T">The scalar type of the Series (e.g., int, double, string).</typeparam>
    /// <param name="series">The source 1D Series.</param>
    /// <returns>A LINQ expression tree root linked to the Series values.</returns>
    public static IQueryable<T> AsQueryable<T>(this Polars.CSharp.Series series)
    {
        var transientDb = new PolarsDataContext(new SqlContext(), ownsContext: true);
        
        return series.AsQueryable<T>(transientDb);
    }
    /// <summary>
    /// Syntactic sugar for Anonymous Types
    /// Infers the generic type 'T' from the provided prototype collection.
    /// Generates a random table name since anonymous types cannot have [Table] attributes.
    /// </summary>
    /// <typeparam name="T">The inferred anonymous type.</typeparam>
    /// <param name="df">The source DataFrame.</param>
    /// <param name="prototype">The collection used purely for C# compiler type inference.</param>
    /// <param name="db">Optional: The DataContext. If null, a transient one is created.</param>
    /// <param name="tableName">Optional: Provide a name, otherwise a random one is generated.</param>
    /// <returns>A LINQ-enabled <see cref="IQueryable{T}"/>.</returns>
    public static IQueryable<T> AsQueryable<T>(
        this Polars.CSharp.DataFrame df, 
        IEnumerable<T> prototype,
        PolarsDataContext? db = null,
        string? tableName = null) 
        where T : class
    {
        db ??= new PolarsDataContext(new SqlContext(), ownsContext: true);

        return db.RegisterTable<T>(df,tableName);
    }
    /// <summary>
    /// Syntactic sugar for Anonymous Types
    /// Infers the generic type 'T' from the provided prototype collection.
    /// Generates a random table name since anonymous types cannot have [Table] attributes.
    /// </summary>
    /// <typeparam name="T">The inferred anonymous type.</typeparam>
    /// <param name="lf">The source DataFrame.</param>
    /// <param name="prototype">The collection used purely for C# compiler type inference.</param>
    /// <param name="db">Optional: The DataContext. If null, a transient one is created.</param>
    /// <param name="tableName">Optional: Provide a name, otherwise a random one is generated.</param>
    /// <returns>A LINQ-enabled <see cref="IQueryable{T}"/>.</returns>
    public static IQueryable<T> AsQueryable<T>(
        this Polars.CSharp.LazyFrame lf, 
        IEnumerable<T> prototype,
        PolarsDataContext? db = null,
        string? tableName = null) 
        where T : class
    {
        db ??= new PolarsDataContext(new SqlContext(), ownsContext: true);

        return db.RegisterTable<T>(lf,tableName);
    }
}
