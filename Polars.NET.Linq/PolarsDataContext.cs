#pragma warning disable CS1591 
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Linq;
public class PolarsDataContext : DataConnection, IDisposable
{
    private readonly IPolarsSqlContext _polarsContext;
    private readonly bool _ownsContext; 
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
                    var actualNetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

                    if (expectedNetType != actualNetType && expectedNetType != typeof(object))
                    {
                        throw new InvalidOperationException(
                            $"[Polars.NET] Table: '{tableName}' Column mapping failed.\n" +
                            $"Polars Column: '{matchedColumn}' 's type is {polarsDataType}, its Dotnet type is '{expectedNetType.Name}'.\n" +
                            $"But your model {typeof(T).Name}.{prop.Name} defined as '{actualNetType.Name}'. Please modify your record or cast correct schema.");
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
    public ITable<T> RegisterTable<T>(string tableName, IPolarsLazyFrame lf, IPolarsSchema? providedSchema = null) 
        where T : class
    {
        var schema = providedSchema ?? lf.Schema; 
        
        BuildSchemaMapping<T>(tableName, schema); 
        _polarsContext.Register(tableName, lf);   
        
        return this.GetTable<T>().TableName(tableName);
    }
    public ITable<T> RegisterTable<T>(string tableName, IPolarsLazyFrame lf, IEnumerable<T> dummy) where T : class 
        => RegisterTable<T>(tableName, lf);
    // ====================================================================
    // DataFrame Register
    // ====================================================================
    public ITable<T> RegisterTable<T>(string tableName, IPolarsDataFrame df, IPolarsSchema? providedSchema = null) 
        where T : class
    {
        var schema = providedSchema ?? df.Schema; 
        
        BuildSchemaMapping<T>(tableName, schema); 
        _polarsContext.Register(tableName, df);   
        
        return this.GetTable<T>().TableName(tableName);
    }
    
    public ITable<T> RegisterTable<T>(string tableName, IPolarsDataFrame df, IEnumerable<T> dummyDataForInference) where T : class
    {
        return RegisterTable<T>(tableName, df);
    }
    // ====================================================================
    // Series Register
    // ====================================================================
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
    public IPolarsLazyFrame ExecuteToLazyFrame(string rawSql)
    {
        var sanitizedSql = SqlSanitizer.Clean(rawSql);

        return _polarsContext.Execute(sanitizedSql);
    }
    // ====================================================================
    // Dispose
    // ====================================================================
    
    public new void Dispose()
    {
        if (_ownsContext)
        {
            _polarsContext?.Dispose();
        }
        base.Dispose();
    }

    void IDisposable.Dispose()
    {
        Dispose();
    }
}

internal class SeriesWrapper<T>
{
    [Column("value")]
    public required T Value { get; set; }
}
