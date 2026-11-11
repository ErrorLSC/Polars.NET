using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Seseragi.Lakehouse.Tests;

/// <summary>A request as the fake server saw it.</summary>
public sealed record SeenRequest(string Method, string PathAndQuery, IReadOnlyDictionary<string, string> Headers, string Body = "");

/// <summary>
/// A loopback HTTP/1.1 server answering with <c>respond</c> (status and body), for metadata services and token
/// endpoints that only exist in a cloud. It reads requests leniently (a PUT without a length is fine, as real
/// metadata services take it) and closes every connection after one answer.
/// </summary>
public sealed class FakeHttpServer : IDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly Func<SeenRequest, (int Status, string Body)> _respond;
    private volatile bool _stopped;

    public FakeHttpServer(Func<SeenRequest, (int Status, string Body)> respond)
    {
        _respond = respond;
        _listener.Start();
        Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";
        new Thread(Serve) { IsBackground = true }.Start();
    }

    /// <summary>The server's address, without a trailing slash.</summary>
    public string Url { get; }

    /// <summary>Headers every answer carries, as <c>Name: value</c> lines.</summary>
    public string[] ResponseHeaders { get; init; } = [];

    /// <summary>The requests seen so far, in order.</summary>
    public ConcurrentQueue<SeenRequest> Requests { get; } = new();

    private void Serve()
    {
        while (!_stopped)
        {
            TcpClient client;
            try
            {
                client = _listener.AcceptTcpClient();
            }
            catch (Exception e) when (e is SocketException or ObjectDisposedException or InvalidOperationException)
            {
                return;
            }
            ThreadPool.QueueUserWorkItem(_ => Handle(client));
        }
    }

    private void Handle(TcpClient client)
    {
        using (client)
        {
            try
            {
                NetworkStream stream = client.GetStream();
                string head = ReadHead(stream);
                string[] lines = head.Split("\r\n");
                string[] requestLine = lines[0].Split(' ');
                var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (string line in lines.Skip(1))
                {
                    int colon = line.IndexOf(':');
                    if (colon > 0) headers[line[..colon].Trim()] = line[(colon + 1)..].Trim();
                }
                byte[] content = [];
                if (headers.TryGetValue("Content-Length", out string? length) && int.Parse(length) is > 0 and var n)
                {
                    content = new byte[n];
                    stream.ReadExactly(content);
                }

                var seen = new SeenRequest(requestLine[0], requestLine[1], headers, Encoding.UTF8.GetString(content));
                Requests.Enqueue(seen);
                var (status, body) = _respond(seen);
                byte[] bytes = Encoding.UTF8.GetBytes(body);
                string extra = string.Concat(ResponseHeaders.Select(h => h + "\r\n"));
                string response = $"HTTP/1.1 {status} {(HttpStatusCode)status}\r\nContent-Length: {bytes.Length}\r\n{extra}Connection: close\r\n\r\n";
                stream.Write(Encoding.ASCII.GetBytes(response));
                stream.Write(bytes);
            }
            catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException or EndOfStreamException)
            {
                // The client went away or the server is stopping.
            }
        }
    }

    private static string ReadHead(NetworkStream stream)
    {
        var head = new List<byte>();
        int b;
        while ((b = stream.ReadByte()) >= 0)
        {
            head.Add((byte)b);
            int c = head.Count;
            if (c >= 4 && head[c - 4] == '\r' && head[c - 3] == '\n' && head[c - 2] == '\r' && head[c - 1] == '\n') break;
        }
        return Encoding.ASCII.GetString(head.ToArray(), 0, Math.Max(0, head.Count - 4));
    }

    public void Dispose()
    {
        _stopped = true;
        _listener.Stop();
    }
}
