using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    public static SelectorHandle SelectorAll() 
        => ErrorHelper.Check(NativeBindings.pl_selector_all());
    public static SelectorHandle CloneSelector(SelectorHandle sel)
        => ErrorHelper.Check(NativeBindings.pl_selector_clone(sel));
    /// <summary>
    /// Select columns by name.
    /// </summary>
    public static SelectorHandle SelectorCols(string[] names)
    {
        return UseUtf8StringArray(names, ptrs => 
        {
            return ErrorHelper.Check(NativeBindings.pl_selector_cols(ptrs, (UIntPtr)ptrs.Length));
        });
    }

    public static SelectorHandle SelectorExclude(SelectorHandle sel, string[] names)
    {
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

    public static SelectorHandle SelectorByDtype(PlDataType kind) => ErrorHelper.Check(NativeBindings.pl_selector_by_dtype((int)kind));
    public static SelectorHandle SelectorNumeric() => ErrorHelper.Check(NativeBindings.pl_selector_numeric());
    public static SelectorHandle SelectorEmpty() => ErrorHelper.Check(NativeBindings.pl_selector_empty());
    public static SelectorHandle SelectorInteger() => ErrorHelper.Check(NativeBindings.pl_selector_integer());
    public static SelectorHandle SelectorUnsignedInteger() => ErrorHelper.Check(NativeBindings.pl_selector_unsigned_integer());
    public static SelectorHandle SelectorSignedInteger() => ErrorHelper.Check(NativeBindings.pl_selector_signed_integer());
    public static SelectorHandle SelectorFloat() => ErrorHelper.Check(NativeBindings.pl_selector_float());
    public static SelectorHandle SelectorDecimal() => ErrorHelper.Check(NativeBindings.pl_selector_decimal());
    public static SelectorHandle SelectorEnum() => ErrorHelper.Check(NativeBindings.pl_selector_enum_type());
    public static SelectorHandle SelectorNested() => ErrorHelper.Check(NativeBindings.pl_selector_nested());
    public static SelectorHandle SelectorStruct() => ErrorHelper.Check(NativeBindings.pl_selector_struct());
    public static SelectorHandle SelectorTemporal() => ErrorHelper.Check(NativeBindings.pl_selector_temporal());
    public static SelectorHandle SelectorList(SelectorHandle? selector)
    {
        var sePtr = selector?.TransferOwnership() ?? IntPtr.Zero;
        var h = NativeBindings.pl_selector_list(sePtr);
        return ErrorHelper.Check(h);
    }
    public static SelectorHandle SelectorArray(SelectorHandle? selector, long? width)
    {
        var sePtr = selector?.TransferOwnership() ?? IntPtr.Zero;
        nuint w = width.HasValue && width.Value > 0 ? (nuint)width.Value : 0;
        var h = NativeBindings.pl_selector_array(sePtr,w);
        return ErrorHelper.Check(h);
    }
    public static SelectorHandle SelectorDatetime(PlTimeUnit unit, string? timeZone)
    {
        var h = NativeBindings.pl_selector_datetime(unit,timeZone);
        return ErrorHelper.Check(h);
    }
    public static SelectorHandle SelectorDuration(PlTimeUnit unit)
    {
        var h = NativeBindings.pl_selector_duration(unit);
        return ErrorHelper.Check(h);
    }

    // --- Set Operations ---

    public static SelectorHandle SelectorAnd(SelectorHandle left, SelectorHandle right)
    {
        var h = NativeBindings.pl_selector_and(left, right);
        
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
    public static SelectorHandle SelectorXor(SelectorHandle left, SelectorHandle right)
    {
        var h = NativeBindings.pl_selector_xor(left, right);
        left.TransferOwnership();
        right.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static SelectorHandle SelectorSub(SelectorHandle left, SelectorHandle right)
    {
        var h = NativeBindings.pl_selector_sub(left, right);
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
        sel.TransferOwnership(); 
        return ErrorHelper.Check(h);
    }
}