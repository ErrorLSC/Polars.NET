namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>A Delta table could not be read or its log is not valid.</summary>
public class DeltaException : Exception
{
    /// <summary>Creates an exception with a message.</summary>
    public DeltaException(string message) : base(message) { }

    /// <summary>Creates an exception that wraps the failure underneath it.</summary>
    public DeltaException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>There is no Delta table at the location: the log directory holds no commit.</summary>
public sealed class DeltaTableNotFoundException(string message) : DeltaException(message);

/// <summary>The table needs a protocol version or table feature this kernel does not implement.</summary>
public sealed class DeltaProtocolException(string message) : DeltaException(message);
