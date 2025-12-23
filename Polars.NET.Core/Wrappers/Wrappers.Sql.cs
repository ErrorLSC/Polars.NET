namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    public static SqlContextHandle SqlContextNew() 
        => ErrorHelper.Check(NativeBindings.pl_sql_context_new());

    public static void SqlRegister(SqlContextHandle ctx, string name, LazyFrameHandle lf)
    {
        // 使用 void 版本的 Helper
        UseUtf8String(name, namePtr => 
        {
            NativeBindings.pl_sql_context_register(ctx, namePtr, lf);
            
            // 注册操作会将 LazyFrame 的所有权移交给 Rust 的 SQL Context
            // 所以我们必须通知 C# 不要再释放它
            lf.TransferOwnership();
            
            // 检查 Rust 端是否有错误
            ErrorHelper.CheckVoid();
        });
    }

    public static LazyFrameHandle SqlExecute(SqlContextHandle ctx, string query)
    {
        // 使用泛型 T 版本的 Helper
        return UseUtf8String(query, queryPtr => 
        {
            return ErrorHelper.Check(
                NativeBindings.pl_sql_context_execute(ctx, queryPtr)
            );
        });
    }
}