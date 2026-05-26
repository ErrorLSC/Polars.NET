using Polars.NET.Core.Native;

namespace Polars.NET.Core;
public readonly partial struct PolarsWrapper
{
    public static CategoriesHandle CategoriesNew(string? name,string? nameSpace,PlCategoricalPhysical code)
        => ErrorHelper.Check(NativeBindings.pl_categories_new(name,nameSpace,code));
    public static CategoriesHandle CategoriesRandom(string nameSpace,PlCategoricalPhysical code)
        => ErrorHelper.Check(NativeBindings.pl_categories_random(nameSpace,code));
    public static CategoriesHandle CategoriesGlobal()
        => ErrorHelper.Check(NativeBindings.pl_categories_global());
    public static FrozenCategoriesHandle CategoriesFreeze(CategoriesHandle categories)
        => ErrorHelper.Check(NativeBindings.pl_categories_freeze(categories));
    public static bool CategoriesIsGlobal(CategoriesHandle categories)
    {
        int status = NativeBindings.pl_categories_is_global(categories,out bool result);
        ErrorHelper.CheckStatus(status);
        return result;
    }
    public static string CategoriesGetName(CategoriesHandle categories)
    {
        int status = NativeBindings.pl_categories_get_name(categories,out nint name);
        ErrorHelper.CheckStatus(status);
        return ErrorHelper.CheckString(name);
    }
    public static string CategoriesGetNameSpace(CategoriesHandle categories)
    {
        int status = NativeBindings.pl_categories_get_namespace(categories,out nint name);
        ErrorHelper.CheckStatus(status);
        return ErrorHelper.CheckString(name);
    }
    public static ulong CategoriesHash(CategoriesHandle categories)
    {
        bool status = NativeBindings.pl_categories_hash(categories,out ulong hash);
        ErrorHelper.CheckBool(status);
        return hash;
    }
    public static PlCategoricalPhysical CategoriesPhysical(CategoriesHandle categories)
    {
        bool status = NativeBindings.pl_categories_physical(categories,out PlCategoricalPhysical physical);
        ErrorHelper.CheckBool(status);
        return physical;
    }
    public static FrozenCategoriesHandle FrozenCategoriesNew(string[] enums)
        => ErrorHelper.Check(NativeBindings.pl_frozencategories_new(enums,(nuint)enums.Length));
    public static ulong FrozenCategoriesHash(FrozenCategoriesHandle categories)
    {
        bool status = NativeBindings.pl_frozencategories_hash(categories,out ulong hash);
        ErrorHelper.CheckBool(status);
        return hash;
    }
    public static SeriesHandle FrozenCategoriesGetCategories(FrozenCategoriesHandle categories)
        => ErrorHelper.Check(NativeBindings.pl_frozencategories_get_categories(categories));  
    public static PlCategoricalPhysical FrozenCategoriesPhysical(FrozenCategoriesHandle categories)
    {
        bool status = NativeBindings.pl_frozencategories_physical(categories,out PlCategoricalPhysical physical);
        ErrorHelper.CheckBool(status);
        return physical;
    }
}