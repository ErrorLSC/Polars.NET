using System.Text.RegularExpressions;

namespace Polars.NET.Linq;

internal static partial class SqlSanitizer
{
    // 专门处理 ESCAPE 的正则
    [GeneratedRegex(@"\s+ESCAPE\s+'.'")]
    private static partial Regex EscapeRegex();

    /// <summary>
    /// 清理 linq2db 生成的，但 Polars 暂不支持的 SQL 语法糖
    /// </summary>
    public static string Clean(string rawSql)
    {
        var sql = EscapeRegex().Replace(rawSql, "");
        // Console.WriteLine(sql);
        // 未来如果需要清理其他的，可以在这里继续加
        // sql = SomeOtherRegex().Replace(sql, "");
        
        return sql;
    }
}
