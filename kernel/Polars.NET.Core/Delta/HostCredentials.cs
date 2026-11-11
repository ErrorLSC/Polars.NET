using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Kaminari.Seseragi.Lakehouse.Storage;
using Polars.NET.Core.Native;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Renewable credentials (<see cref="RenewableCredentials"/>) named in cloud options, so that every store a table
/// operation opens (the managed clients, and the engine's own stores reading and writing data files) asks the same
/// source when its credentials are due: the options carry <see cref="Key"/> with the source's id, which the native
/// side takes out and turns into a credential provider calling back here.
/// </summary>
internal static class HostCredentials
{
    /// <summary>The configuration key naming a source (the native side knows it too).</summary>
    public const string Key = "polars_net_credential_source";

    private static readonly ConcurrentDictionary<ulong, RenewableCredentials> Sources = new();
    private static long _lastId;
    private static bool _registered;

    /// <summary>Makes <paramref name="source"/> reachable by id; returns the id. Sources stay registered for the process.</summary>
    public static unsafe ulong Register(RenewableCredentials source)
    {
        ArgumentNullException.ThrowIfNull(source);
        lock (Sources)
        {
            // Before any id is handed out, so no store can ask before the callback is there.
            if (!_registered)
            {
                NativeBindings.pl_cloud_set_credential_callback(&OnCredentials);
                _registered = true;
            }
        }
        ulong id = (ulong)Interlocked.Increment(ref _lastId);
        Sources[id] = source;
        return id;
    }

    /// <summary>The source registered under <paramref name="id"/>.</summary>
    public static RenewableCredentials Get(ulong id) =>
        Sources.TryGetValue(id, out RenewableCredentials? source) ? source : throw new InvalidOperationException($"The credential source {id} is unknown.");

    /// <summary>The source named in configuration keys, if one is.</summary>
    public static RenewableCredentials? Find(IEnumerable<KeyValuePair<string, string>> options)
    {
        foreach (var (key, value) in options)
        {
            if (key != Key) continue;
            return ulong.TryParse(value, out ulong id) && Sources.TryGetValue(id, out RenewableCredentials? source)
                ? source
                : throw new InvalidOperationException($"The credential source '{value}' is unknown.");
        }
        return null;
    }

    /// <summary>The time to ask again, as the engine takes it: whole seconds since the epoch, rounded up, never in the past.</summary>
    internal static ulong RenewAtSeconds(DateTimeOffset? renewAt, DateTimeOffset now)
    {
        if (renewAt is not { } at) return ulong.MaxValue;
        if (at < now) at = now;
        long ms = at.ToUnixTimeMilliseconds();
        return (ulong)((ms + 999) / 1000);
    }

    /// <summary>The credentials as the native side reads them: <c>key TAB value</c> lines.</summary>
    internal static string Payload(VendedCredentials credentials)
    {
        var text = new StringBuilder();
        foreach (var (key, value) in credentials.Options) text.Append(key).Append('\t').Append(value).Append('\n');
        return text.ToString();
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe int OnCredentials(ulong id, byte* output, nuint capacity, nuint* outLength, ulong* outRenewAt)
    {
        string text;
        int status;
        try
        {
            if (!Sources.TryGetValue(id, out RenewableCredentials? source))
                throw new InvalidOperationException($"The credential source {id} is unknown.");
            VendedCredentials current = source.Current(out DateTimeOffset? renewAt);
            *outRenewAt = RenewAtSeconds(renewAt, DateTimeOffset.UtcNow);
            text = Payload(current);
            status = 0;
        }
        catch (Exception e)
        {
            text = $"Renewing the storage credentials failed: {e.Message}";
            status = 1;
        }
        int length = Encoding.UTF8.GetByteCount(text);
        *outLength = (nuint)length;
        if ((nuint)length > capacity) return 2;
        Encoding.UTF8.GetBytes(text, new Span<byte>(output, length));
        return status;
    }
}
