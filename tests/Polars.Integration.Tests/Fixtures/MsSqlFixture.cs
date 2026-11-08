using Testcontainers.MsSql;
using Microsoft.Data.SqlClient; 

namespace Polars.Integration.Tests.Fixtures
{
    public class MsSqlFixture : IAsyncLifetime
    {
        private readonly MsSqlContainer _msSqlContainer;

        public MsSqlFixture()
        {
            _msSqlContainer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .Build();
        }

        public string ConnectionString => _msSqlContainer.GetConnectionString();

        public async Task InitializeAsync()
        {
            await _msSqlContainer.StartAsync();

            await InitializeDatabaseDataAsync();
        }

        public Task DisposeAsync()
        {
            return _msSqlContainer.DisposeAsync().AsTask();
        }

        private async Task InitializeDatabaseDataAsync()
        {
            using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            var cmdText = @"
                CREATE TABLE TestData (
                    Id INT PRIMARY KEY,
                    Name NVARCHAR(50),
                    Value FLOAT,
                    IsActive BIT
                );

                INSERT INTO TestData (Id, Name, Value, IsActive) VALUES 
                (1, 'Alice', 12.5, 1),
                (2, 'Bob', 33.3, 0),
                (3, 'Charlie', 0.0, 1);
            ";

            using var command = new SqlCommand(cmdText, connection);
            await command.ExecuteNonQueryAsync();
        }
    }
}