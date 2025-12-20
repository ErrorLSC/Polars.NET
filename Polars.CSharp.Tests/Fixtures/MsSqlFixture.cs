using Testcontainers.MsSql;

namespace Polars.CSharp.Tests.Fixtures
{
    // 这个类负责管理 Docker 容器的生命周期
    public class MsSqlFixture : IAsyncLifetime
    {
        private readonly MsSqlContainer _container;

        public MsSqlFixture()
        {
            // 定义容器配置：使用最新的 SQL Server 2022
            _container = new MsSqlBuilder()
                .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
                .Build();
        }

        // 测试开始前自动调用：启动 Docker 容器
        public Task InitializeAsync() => _container.StartAsync();

        // 测试结束后自动调用：销毁容器
        public async Task DisposeAsync() => await _container.DisposeAsync();

        // 获取动态生成的连接字符串
        public string ConnectionString => _container.GetConnectionString();
    }
}