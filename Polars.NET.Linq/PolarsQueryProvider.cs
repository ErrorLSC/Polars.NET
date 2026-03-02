using System.Linq.Expressions;
using Polars.CSharp;
using LinqToDB;
using LinqToDB.DataProvider.PostgreSQL;

namespace Polars.NET.Linq;

public class PolarsQueryProvider : IQueryProvider
{
    private readonly SqlContext _sqlContext;
    private readonly string _tableName;

    public PolarsQueryProvider(SqlContext sqlContext, string tableName)
    {
        _sqlContext = sqlContext;
        _tableName = tableName;
    }

    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = expression.Type.GetGenericArguments().First();
        var queryableType = typeof(PolarsQueryable<>).MakeGenericType(elementType);
        return (IQueryable)Activator.CreateInstance(queryableType, this, expression)!;
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new PolarsQueryable<TElement>(this, expression);
    }


    public object? Execute(Expression expression)
    {
        throw new NotImplementedException();
    }

    public TResult Execute<TResult>(Expression expression)
    {
        // 1. 获取纯净的 Provider，并注入我们的 "特洛伊木马" Connection
        var dataProvider = PostgreSQLTools.GetDataProvider(
            PostgreSQLVersion.v15);
            
        var mockConnection = new PolarsDbConnection(_sqlContext);
        var options = new DataOptions().UseConnection(dataProvider, mockConnection)
            .WithOptions<SqlOptions>(o => o with { GenerateFinalAliases = true });;

        // 注意：这里必须物化数据后才能释放 db，所以我们依然使用 using
        using var db = new LinqToDB.Data.DataConnection(options)
        {
            InlineParameters = true 
        };

        // 2. 利用 Visitor 把外部的 PolarsQueryable 替换为 DataConnection 的 Queryable
        var visitor = new PolarsToLinq2DbVisitor(db);
        var linq2dbExpression = visitor.Visit(expression);

        // 3. 【核心分流】：判断 TResult 是集合还是标量
        var isEnumerable = typeof(TResult).IsGenericType && 
                        typeof(TResult).GetGenericTypeDefinition() == typeof(IEnumerable<>);

        if (isEnumerable)
        {
            // 场景 A：集合查询 (ToList, foreach)
            var elementType = typeof(TResult).GetGenericArguments()[0];
            var createQueryMethod = typeof(IQueryProvider).GetMethods()
                .First(m => m.Name == "CreateQuery" && m.IsGenericMethod)
                .MakeGenericMethod(elementType);
            
            // 让 linq2db 生成它自己的 Queryable
            var linq2dbQueryable = (IQueryable)createQueryMethod.Invoke(db.GetTable<object>().Provider, [linq2dbExpression])!;
            
            // 【关键】：瞬间物化！利用 linq2db 的强类型 Mapper 从你的 ArrowToDbStream 中疯狂抽取数据
            var listType = typeof(List<>).MakeGenericType(elementType);
            var list = (System.Collections.IList)Activator.CreateInstance(listType)!;
            
            // 这里的 foreach 会触发底层的 Mock DbCommand -> Polars -> ArrowToDbStream -> linq2db Mapper
            foreach (var item in linq2dbQueryable)
            {
                list.Add(item!);
            }
            
            return (TResult)list;
        }
        else
        {
            // 场景 B：标量查询 (Count, FirstOrDefault)
            // 这个时候 linq2db 的 Execute 就能正常工作了
            return db.GetTable<object>().Provider.Execute<TResult>(linq2dbExpression);
        }
    }
}

internal class PolarsToLinq2DbVisitor : ExpressionVisitor
{
    private readonly IDataContext _dataContext;

    public PolarsToLinq2DbVisitor(IDataContext dataContext)
    {
        _dataContext = dataContext;
    }

    // 核心提取逻辑：不管你是怎么传进来的，只要本体是 PolarsQueryable 就替换！
    private Expression? TryGetLinq2DbTable(object? value)
    {
        if (value == null) return null;
        
        var type = value.GetType();
        // 检查真实的运行时类型
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(PolarsQueryable<>))
        {
            var elementType = type.GetGenericArguments()[0];
            var tableNameProp = type.GetProperty("TableName");
            var actualTableName = (string)tableNameProp!.GetValue(value)!;

            var getTableMethod = typeof(DataExtensions)
                .GetMethod("GetTable", [typeof(IDataContext)])!
                .MakeGenericMethod(elementType);

            var table = getTableMethod.Invoke(null, [_dataContext]);

            var tableNameMethod = typeof(LinqExtensions)
                .GetMethod("TableName")!
                .MakeGenericMethod(elementType);
                
            table = tableNameMethod.Invoke(null, [table, actualTableName]);

            return ((IQueryable)table!).Expression;
        }
        return null;
    }

    // 拦截 1：直接作为根节点的查询
    protected override Expression VisitConstant(ConstantExpression node)
    {
        var tableExpr = TryGetLinq2DbTable(node.Value);
        if (tableExpr != null) return tableExpr;
        
        return base.VisitConstant(node);
    }

    // 拦截 2：被 C# 闭包捕获的外部查询变量 (比如 from e in empQuery)
    protected override Expression VisitMember(MemberExpression node)
    {
        // 只要这个变量的声明类型是 IQueryable，我们就有理由怀疑它是我们的人
        if (typeof(IQueryable).IsAssignableFrom(node.Type))
        {
            try 
            {
                // 强制执行闭包获取真实对象
                var getter = Expression.Lambda(node).Compile();
                var value = getter.DynamicInvoke();
                
                // 检查真实对象是不是 PolarsQueryable
                var tableExpr = TryGetLinq2DbTable(value);
                if (tableExpr != null) return tableExpr;
            }
            catch 
            {
                // 忽略无法求值的成员
            }
        }

        return base.VisitMember(node);
    }
}
