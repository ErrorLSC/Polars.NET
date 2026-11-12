using System.Net;

namespace Kaminari.Seseragi.Lakehouse.Catalog;

/// <summary>
/// A catalog request failed: the service answered with an error status, could not be reached,
/// or sent a response that could not be understood.
/// </summary>
public class CatalogException : Exception
{
    /// <summary>The HTTP status of the response, or null when no response arrived.</summary>
    public HttpStatusCode? StatusCode { get; }

    /// <summary>The response body, truncated, when the service sent one.</summary>
    public string? ResponseBody { get; }

    /// <summary>Creates an exception with a message.</summary>
    public CatalogException(string message) : base(message) { }

    /// <summary>Creates an exception that wraps the failure underneath it.</summary>
    public CatalogException(string message, Exception inner) : base(message, inner) { }

    /// <summary>Creates an exception for an error response.</summary>
    public CatalogException(string message, HttpStatusCode statusCode, string? responseBody) : base(message)
    {
        StatusCode = statusCode;
        ResponseBody = responseBody;
    }
}
