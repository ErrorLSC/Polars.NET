using Xunit;

namespace Polars.CSharp.Tests
{
    public class SelectorTests
    {
        [Fact]
        public void Test_Selector_All_Exclude()
        {
            // 1. 准备数据 (4列)
            var df = DataFrame.FromColumns(new 
            {
                Id = new[] { 1, 2 },
                Name = new[] { "Alice", "Bob" },
                Age = new[] { 25, 30 },
                Secret = new[] { "pass1", "pass2" }
            });

            // 2. 使用 Selector: All().Exclude(...)
            // 场景：保留所有业务数据，剔除 ID 和 敏感信息
            // 这里发生了隐式转换: Selector -> Expr
            var result = df.Select(
                Polars.All().Exclude("Id", "Secret")
            );

            // 3. 验证结果
            Assert.Equal(2, result.Width);
            
            // 应该只剩下 Name 和 Age
            Assert.Equal("Name", result.Column(0).Name);
            Assert.Equal("Age", result.Column(1).Name);
            
            // 确保排除的列不存在 (抛异常)
            Assert.Throws<ArgumentException>(() => result["Id"]);
            Assert.Throws<ArgumentException>(() => result["Secret"]);
        }

        [Fact]
        public void Test_Selector_Operation()
        {
            // Selector 不仅能筛选，还能直接由 Expr 的能力
            // 比如：选取除了 Id 以外的所有列，并把它们都乘以 2 (假设都是数值)
            
            var df = DataFrame.FromColumns(new 
            {
                Id = new[] { 1, 2 },
                Val1 = new[] { 10, 20 },
                Val2 = new[] { 100, 200 }
            });

            // select (All - "Id") * 2
            var result = df.Select(
                (Polars.All().Exclude("Id") * 2).Name.Suffix("_Scaled") 
                // 注意：Polars 里的 Selector 运算通常会保持原名，
                // 或者变成 struct，这里简单测试 broadcast 乘法
            );

            // 验证
            Assert.Equal(20, result[0, "Val1_Scaled"]); // 10 * 2
            Assert.Equal(200, result[0, "Val2_Scaled"]); // 100 * 2
            
            // Id 列不应该参与计算 (也不在结果里，因为我们没选它)
            Assert.Throws<ArgumentException>(() => result["Id"]);
        }
    }
}