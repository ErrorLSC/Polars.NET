using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using Kaminari.Sazanami.Network;

namespace Sazanami.Tests;

/// <summary><see cref="HttpSender"/>: what is retried, how long it waits, what it gives up with, and counting.</summary>
public sealed class HttpSenderTests
{
    private static readonly RetrySettings Quick = new(3, TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(5));

    /// <summary>Answers each request with the next of its steps (the last repeats): a status, or an exception.</summary>
    private sealed class Script(params Func<HttpRequestMessage, HttpResponseMessage>[] steps) : HttpMessageHandler
    {
        public int Calls;

        protected override HttpResponseMessage Send(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            int at = Interlocked.Increment(ref Calls) - 1;
            HttpResponseMessage response = steps[Math.Min(at, steps.Length - 1)](request);
            response.RequestMessage = request;
            return response;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(Send(request, cancellationToken));
    }

    private static Func<HttpRequestMessage, HttpResponseMessage> Status(HttpStatusCode status, string body = "", TimeSpan? retryAfter = null) => _ =>
    {
        var response = new HttpResponseMessage(status) { Content = new StringContent(body) };
        if (retryAfter is { } wait) response.Headers.RetryAfter = new RetryConditionHeaderValue(wait);
        return response;
    };

    private static Func<HttpRequestMessage, HttpResponseMessage> Throw(Exception e) => _ => throw e;

    private static HttpSender Sender(Script script, RetrySettings? retry = null) => new(new HttpClient(script), retry ?? Quick);

    private static HttpRequestMessage Get() => new(HttpMethod.Get, "http://host/x");

    [Theory]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.TooManyRequests)]
    [InlineData(HttpStatusCode.RequestTimeout)]
    public async Task RetryableAnswers_AreRetried(HttpStatusCode status)
    {
        var script = new Script(Status(status), Status(HttpStatusCode.OK, "done"));
        HttpAnswer answer = Sender(script).Send(Get);
        Assert.Equal(HttpStatusCode.OK, answer.Status);
        Assert.Equal("done"u8.ToArray(), answer.Body);
        Assert.True(answer.Retried);
        Assert.Equal(2, script.Calls);

        script = new Script(Status(status), Status(HttpStatusCode.OK, "done"));
        answer = await Sender(script).SendAsync(Get);
        Assert.Equal("done"u8.ToArray(), answer.Body);
        Assert.Equal(2, script.Calls);
    }

    [Fact]
    public async Task OtherAnswers_AreReturned_AndRetryRunsOutOnTheLastAnswer()
    {
        foreach (HttpStatusCode status in (HttpStatusCode[])[HttpStatusCode.NotFound, HttpStatusCode.NotImplemented, HttpStatusCode.PreconditionFailed])
        {
            var script = new Script(Status(status, "no"));
            Assert.Equal(status, Sender(script).Send(Get).Status);
            Assert.Equal(1, script.Calls);
        }
        // Named by the caller: retried.
        var named = new Script(Status(HttpStatusCode.Conflict), Status(HttpStatusCode.OK));
        Assert.Equal(HttpStatusCode.OK, (await Sender(named).SendAsync(Get, s => s == HttpStatusCode.Conflict)).Status);
        // Retries run out: the last answer is returned, whatever its status.
        var always = new Script(Status(HttpStatusCode.BadGateway, "bad"));
        HttpAnswer last = Sender(always).Send(Get);
        Assert.Equal(HttpStatusCode.BadGateway, last.Status);
        Assert.Equal(4, always.Calls); // 1 + 3 retries
    }

    [Fact]
    public async Task NetworkFailures_AreRetried_ThenGiveUpWithTheLastFailure()
    {
        var flaky = new Script(Throw(new HttpRequestException("reset")), Status(HttpStatusCode.OK, "ok"));
        Assert.Equal("ok"u8.ToArray(), Sender(flaky).Send(Get).Body);

        var dead = new Script(Throw(new HttpRequestException("refused")));
        var e = Assert.Throws<HttpRetriesExhaustedException>(() => Sender(dead).Send(Get));
        Assert.Equal("The request failed after 3 retries: refused", e.Message);
        Assert.IsType<HttpRequestException>(e.InnerException);
        Assert.Equal(4, dead.Calls);

        dead = new Script(Throw(new IOException("broken pipe")));
        var async = await Assert.ThrowsAsync<HttpRetriesExhaustedException>(() => Sender(dead).SendAsync(Get));
        Assert.Equal(3, async.Retries);
        // A connect timeout surfaces as a cancellation the caller did not ask for: retried.
        var timeout = new Script(Throw(new TaskCanceledException("connect timeout")), Status(HttpStatusCode.OK));
        Assert.Equal(HttpStatusCode.OK, (await Sender(timeout).SendAsync(Get)).Status);
    }

    /// <summary>A failure the answer decided (thrown by the caller's reader) is not retried.</summary>
    [Fact]
    public async Task FailuresTheAnswerDecided_AreNotRetried()
    {
        var script = new Script(Status(HttpStatusCode.PreconditionFailed));
        await Assert.ThrowsAsync<HttpAnswerException>(() => Sender(script).SendAsync<int>(Get, (_, _, _) => throw new HttpAnswerException("changed")));
        Assert.Equal(1, script.Calls);
        Assert.Throws<FileNotFoundException>(() => Sender(script).Send<int>(Get, (_, _) => throw new FileNotFoundException("gone")));
        Assert.Equal(2, script.Calls);
    }

    [Fact]
    public async Task ACancellationByTheCaller_IsNotRetried()
    {
        using var cts = new CancellationTokenSource();
        var script = new Script(_ =>
        {
            cts.Cancel();
            throw new TaskCanceledException();
        });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Sender(script).SendAsync(Get, cancellationToken: cts.Token));
        Assert.Equal(1, script.Calls);
        // During a wait too.
        using var waiting = new CancellationTokenSource();
        var slow = new Script(Status(HttpStatusCode.ServiceUnavailable, retryAfter: TimeSpan.FromSeconds(30)));
        Task<HttpAnswer> sent = new HttpSender(new HttpClient(slow), RetrySettings.Default).SendAsync(Get, cancellationToken: waiting.Token);
        waiting.CancelAfter(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sent);
    }

    [Fact]
    public void RetryAfter_LengthensTheWait_UpToTheMaxBackoff()
    {
        var asks = new Script(Status(HttpStatusCode.ServiceUnavailable, retryAfter: TimeSpan.FromMilliseconds(300)), Status(HttpStatusCode.OK));
        var w = Stopwatch.StartNew();
        Sender(asks, Quick with { MaxBackoff = TimeSpan.FromSeconds(5) }).Send(Get);
        Assert.InRange(w.ElapsedMilliseconds, 250, 4000);

        var greedy = new Script(Status(HttpStatusCode.ServiceUnavailable, retryAfter: TimeSpan.FromSeconds(60)), Status(HttpStatusCode.OK));
        w.Restart();
        Sender(greedy, Quick with { MaxBackoff = TimeSpan.FromMilliseconds(100) }).Send(Get);
        Assert.InRange(w.ElapsedMilliseconds, 0, 2000);
    }

    [Fact]
    public void AHead_ReadsNoBody()
    {
        var script = new Script(_ =>
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent([]) };
            response.Content.Headers.ContentLength = 1234;
            return response;
        });
        using HttpAnswer answer = Sender(script).Send(() => new HttpRequestMessage(HttpMethod.Head, "http://host/x"));
        Assert.True(answer.Body.IsEmpty);
        Assert.Equal(1234, answer.ContentHeaders.ContentLength);
    }

    /// <summary>A failure while the caller reads the body sends the request again; the reader starts afresh.</summary>
    [Fact]
    public async Task AFailedBodyRead_IsRetried()
    {
        var script = new Script(Status(HttpStatusCode.PartialContent, "abcd"));
        int reads = 0;
        string text = await Sender(script).SendAsync(Get, async (response, retried, ct) =>
        {
            if (Interlocked.Increment(ref reads) == 1) throw new IOException("connection reset while reading");
            Assert.True(retried);
            return await response.Content.ReadAsStringAsync(ct);
        });
        Assert.Equal("abcd", text);
        Assert.Equal(2, script.Calls);
        Assert.Equal(2, Sender(new Script(Status(HttpStatusCode.OK, "x"))).Send(Get, (r, _) => r.Content.ReadAsStringAsync().Result.Length + 1));
    }

    [Fact]
    public async Task EveryAttempt_IsCounted_OnItsFlowOnly()
    {
        var script = new Script(Status(HttpStatusCode.ServiceUnavailable), Status(HttpStatusCode.OK));
        Assert.Equal(2, HttpSender.CountRequests(() => Sender(script).Send(Get)));
        script = new Script(Status(HttpStatusCode.ServiceUnavailable), Status(HttpStatusCode.OK));
        Assert.Equal(2, await HttpSender.CountRequestsAsync(() => Sender(script).SendAsync(Get)));
        int outer = HttpSender.CountRequests(() =>
        {
            Sender(new Script(Status(HttpStatusCode.OK))).Send(Get);
            Assert.Equal(1, HttpSender.CountRequests(() => Sender(new Script(Status(HttpStatusCode.OK))).Send(Get)));
        });
        Assert.Equal(1, outer); // the inner flow counts its own
    }
}
