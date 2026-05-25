
namespace Polars.CSharp;

public readonly partial struct Polars
{
    /// <summary>
    /// Register the extension type for the given extension name.
    /// </summary>
    /// <typeparam name="T">The custom extension class inheriting from BaseExtension.</typeparam>
    /// <param name="extName">The registered name.</param>
    /// <param name="factory">The factory delegate to deserialize the extension type.</param>
    public static void RegisterExtensionType<T>(string extName, ExtensionFactory factory) where T : BaseExtension 
        => ExtensionRegistry.RegisterExtensionType<T>(extName, factory);
    
    /// <summary>
    /// Register the extension type to be passed through purely as physical storage.
    /// </summary>
    /// <param name="extName">The registered name.</param>
    /// <param name="asStorage">Must be true. If false, use the generic generic method to register a class.</param>
    public static void RegisterExtensionType(string extName, bool asStorage)
    {
        if (!asStorage)
        {
            throw new ArgumentException(
                "When 'asStorage' is false, you must provide an extension class factory using the generic RegisterExtensionType<T>(...) method."
            );
        }

        ExtensionRegistry.RegisterExtensionTypeAsStorage(extName);
    }

    /// <summary>
    /// Unregister the extension type for the given extension name.
    /// </summary>
    /// <param name="extName">The registered name.</param>
    public static void UnregisterExtensionType(string extName) => ExtensionRegistry.UnregisterExtensionType(extName);
    /// <summary>
    /// Get the extension type registration info for the given extension name.
    /// </summary>
    /// <param name="extName">The registered name.</param>
    /// <returns>An <see cref="ExtensionInfo"/> indicating how the type is registered.</returns>
    public static ExtensionInfo GetExtensionType(string extName)
    {
        if (ExtensionRegistry.TryGetResolution(extName, out var factory, out bool asStorage))
        {
            if (asStorage)
            {
                return new ExtensionInfo.AsStorage();
            }
            
            if (factory is not null)
            {
                return new ExtensionInfo.AsClass(factory);
            }
        }

        return new ExtensionInfo.NotFound();
    }
}