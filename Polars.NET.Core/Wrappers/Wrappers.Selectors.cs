namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    public static SelectorHandle SelectorAll() 
        => ErrorHelper.Check(NativeBindings.pl_selector_all());
    public static SelectorHandle CloneSelector(SelectorHandle sel)
        => ErrorHelper.Check(NativeBindings.pl_selector_clone(sel));
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
    // --- String Matchers ---
        
    public static SelectorHandle SelectorStartsWith(string pattern)
         => ErrorHelper.Check(NativeBindings.pl_selector_starts_with(pattern));


    public static SelectorHandle SelectorEndsWith(string pattern)
        => ErrorHelper.Check(NativeBindings.pl_selector_ends_with(pattern));

    public static SelectorHandle SelectorContains(string pattern)
        => ErrorHelper.Check(NativeBindings.pl_selector_contains(pattern));

    public static SelectorHandle SelectorMatch(string regex)
        => ErrorHelper.Check(NativeBindings.pl_selector_match(regex));

    // --- Type Selectors ---

    public static SelectorHandle SelectorByDtype(PlDataType kind)
    {
        // Enum 直接转 int 传给 Rust
        return ErrorHelper.Check(NativeBindings.pl_selector_by_dtype((int)kind));
    }

    public static SelectorHandle SelectorNumeric()
        => ErrorHelper.Check(NativeBindings.pl_selector_numeric());

    // --- Set Operations ---

    public static SelectorHandle SelectorAnd(SelectorHandle left, SelectorHandle right)
    {
        var h = NativeBindings.pl_selector_and(left, right);
        
        // [Ownership] Rust 端的 Intersect 消耗了 left 和 right
        left.TransferOwnership();
        right.TransferOwnership();
        
        return ErrorHelper.Check(h);
    }

    public static SelectorHandle SelectorOr(SelectorHandle left, SelectorHandle right)
    {
        var h = NativeBindings.pl_selector_or(left, right);
        left.TransferOwnership();
        right.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static SelectorHandle SelectorNot(SelectorHandle sel)
    {
        var h = NativeBindings.pl_selector_not(sel);
        sel.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle SelectorToExpr(SelectorHandle sel)
    {
        var h = NativeBindings.pl_selector_into_expr(sel);
        sel.TransferOwnership(); // 转换后 Selector 就没用了，变成了 Expr
        return ErrorHelper.Check(h);
    }
}