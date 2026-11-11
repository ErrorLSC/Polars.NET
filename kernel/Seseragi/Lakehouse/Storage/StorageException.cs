using System.Net;

namespace Kaminari.Seseragi.Lakehouse.Storage;

/// <summary>A request to an object store failed.</summary>
public sealed class StorageException : Exception
{
    /// <summary>Creates the exception.</summary>
    public StorageException(string message, HttpStatusCode? status = null, string? code = null, Exception? inner = null)
        : base(message, inner)
    {
        Status = status;
        Code = code;
    }

    /// <summary>The HTTP status the store answered with, when it answered.</summary>
    public HttpStatusCode? Status { get; }

    /// <summary>The store's own error code (such as <c>AccessDenied</c>), when it sent one.</summary>
    public string? Code { get; }
}
