using System.Diagnostics.CodeAnalysis;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Wrappers;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Helpers;

namespace Polars.NET.Core.Arrow;

/// <summary>
/// Resolves .NET types to native Polars data types.
/// </summary>
public static class NativeDataTypeResolver
{
    /// <summary>
    /// Gets a native Polars DataTypeHandle for a .NET type.
    /// </summary>
    public static DataTypeHandle GetDataTypeHandle([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type type)
    {
        // An enum is the Enum of its members, or the underlying integer of a [Flags] enum.
        var core = ClrTypeHelper.UnwrapCoreType(type);
        if (core.IsEnum)
        {
            if (!EnumCategories.UsesEnumDataType(core))
            {
                return GetDataTypeHandle(Enum.GetUnderlyingType(core));
            }

            using var frozen = PolarsWrapper.FrozenCategoriesNew(EnumCategories.GetCategories(core));
            return PolarsWrapper.NewEnumType(frozen);
        }

        var arrowType = ArrowTypeResolver.GetArrowTypeFromNetType(type);
        return PolarsWrapper.MapArrowToDataTypeHandle(arrowType);
    }
}
