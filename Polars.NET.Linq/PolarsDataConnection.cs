using System;
using LinqToDB;
using LinqToDB.Data;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Linq
{
    public class PolarsDataContext : DataConnection, IDisposable
    {
        private readonly IPolarsSqlContext _polarsContext;
        private readonly bool _ownsContext; // 核心标志位：是否由我们负责销毁
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

        // DTO 注册
        public ITable<T> RegisterTable<T>(string tableName, IPolarsDataFrame df) where T : class
        {
            _polarsContext.Register(tableName, df);
            return this.GetTable<T>().TableName(tableName);
        }

        // 匿名对象推断注册 (幽灵参数)
        public ITable<T> RegisterTable<T>(string tableName, IPolarsDataFrame df, IEnumerable<T> dummyDataForInference) where T : class
        {
            return RegisterTable<T>(tableName, df);
        }

        public ITable<T> RegisterTable<T>(string tableName, IPolarsLazyFrame lf) where T : class
        {
            _polarsContext.Register(tableName, lf);
            return this.GetTable<T>().TableName(tableName);
        }

        public ITable<T> RegisterTable<T>(string tableName, IPolarsLazyFrame lf, IEnumerable<T> dummy) where T : class 
            => RegisterTable<T>(tableName, lf);

        // ====================================================================
        // Series 专属注册区：自带 Arrow 级类型校验，直接返回 IQueryable<T>
        // ====================================================================
        public IQueryable<T> RegisterSeries<T>(IPolarsSeries s)
        {
            // 1. 强类型拦截防御
            ValidateSeriesArrowType<T>(s);

            // 2. 记住它原本的模样（真实列名）
            var originalSeriesName = s.Name;
            
            // 如果它本来连名字都没有，我们就随便给表起个名；如果有，表名就用它的原名
            var tableName = string.IsNullOrEmpty(originalSeriesName) 
                ? $"series_{Guid.NewGuid():N}" 
                : originalSeriesName;

            IPolarsDataFrame df;
            
            // ==========================================
            // 【核心修复】：无痕借用机制 (Zero Side-Effect)
            // ==========================================
            try
            {
                // 暂时改名叫 value，为了去套 linq2db 的衣服
                s.Rename("value"); 
                
                // 这一瞬间，生成的 DataFrame 里这列就叫 "value" 了！
                df = s.ToFrame(); 
            }
            finally
            {
                // ！！！完璧归赵 ！！！
                // 不管 ToFrame 成功还是报错，立刻把原名还给用户的 Series
                s.Rename(originalSeriesName);
            }

            // 3. 用真实的表名，注册那个包含 "value" 列的 DataFrame
            _polarsContext.Register(tableName, df);

            // 4. 返回干净的 LINQ 查询对象
            return this.GetTable<SeriesWrapper<T>>()
                       .TableName(tableName)
                       .Select(row => row.Value); 
        }

        // ====================================================================
        // 核心守卫逻辑
        // ====================================================================
        private void ValidateSeriesArrowType<T>(IPolarsSeries s)
        {
            // 注意：这里假设你的 IPolarsSeries 暴露了获取底层 IArrowType 的方法或属性
            // 如果是在 DataType 里，可能是 s.DataType.GetArrowType() 之类的，请替换为你实际的 API
            var arrowType = s.DataType.GetArrowType(); 

            // 1. 调用你的神级映射方法，拿到 Polars 底层内存对应的 .NET 真实类型
            Type expectedNetType = ArrowTypeResolver.GetNetTypeFromArrowType(arrowType);
            
            // 2. 获取用户传入的泛型 T 的实际类型
            // 极其重要：处理 T 是 int? (Nullable<int>) 的情况！底层 Arrow 都是可空的，所以剥离 Nullable 外壳比较
            Type userType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

            // 3. 强类型拦截 (排除 object 兜底的情况)
            if (userType != expectedNetType && expectedNetType != typeof(object))
            {
                throw new InvalidOperationException(
                    $"[Polars.NET.Linq] 类型不匹配致命错误！\n" +
                    $"Series '{s.Name}' 的底层 Arrow 类型是 {arrowType.GetType().Name}，" +
                    $"它只能被查询为 {expectedNetType.Name} (或可空类型)。\n" +
                    $"但你试图使用 RegisterSeries<{userType.Name}>()，这将导致内存读取越界或 SQL 解析崩溃！");
            }
        }

        // ====================================================================
        // 终极护盾：隐藏父类的 Dispose 并拦截释放信号
        // ====================================================================
        
        // 1. 使用 new 关键字隐藏父类的 public Dispose
        public new void Dispose()
        {
            if (_ownsContext)
            {
                // 超度底层 Rust 内存
                _polarsContext?.Dispose();
            }
            // 别忘了调用父类原生的释放逻辑还给 linq2db
            base.Dispose();
        }

        // 2. 显式实现 IDisposable，确保 using(...) 语法糖能够精准命中我们的新方法
        void IDisposable.Dispose()
        {
            this.Dispose();
        }
    }

    internal class SeriesWrapper<T>
    {
        [LinqToDB.Mapping.Column("value")]
        public required T Value { get; set; }
    }
}