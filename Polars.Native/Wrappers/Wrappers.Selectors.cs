namespace Polars.Native;

public static partial class PolarsWrapper
{
    public static SelectorHandle SelectorAll() 
        => ErrorHelper.Check(NativeBindings.pl_selector_all());
    public static SelectorHandle CloneSelector(SelectorHandle sel)
    {
        // Clone 操作不消耗原 Handle，只做 Check
        return ErrorHelper.Check(NativeBindings.pl_selector_clone(sel));
    }
    public static SelectorHandle SelectorExclude(SelectorHandle sel, string[] names)
    {
        // 使用 Helper 自动处理内存分配和释放
        return UseUtf8StringArray(names, ptrs => 
        {
            var h = NativeBindings.pl_selector_exclude(sel, ptrs, (UIntPtr)ptrs.Length);
            sel.TransferOwnership();
            return ErrorHelper.Check(h);
        });
    }

    public static ExprHandle SelectorToExpr(SelectorHandle sel)
    {
        var h = NativeBindings.pl_selector_into_expr(sel);
        sel.TransferOwnership(); // 转换后 Selector 就没用了，变成了 Expr
        return ErrorHelper.Check(h);
    }
}