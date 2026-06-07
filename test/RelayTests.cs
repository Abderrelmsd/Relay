using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Wolverine;

namespace Relay.Tests;

public sealed class FakeTime(DateTimeOffset now) : TimeProvider
{
    public DateTimeOffset Now { get; set; } = now;
    public override DateTimeOffset GetUtcNow() => Now;
    public void Advance(TimeSpan by) => Now += by;
}

/// <summary>Scriptable destination that records deliveries and can fail on demand.</summary>
public sealed class TestDestination(string name) : IRelayDestination
{
    public string Name => name;
    public List<(Guid Id, string Payload, string? Key, int Attempt)> Delivered { get; } = [];
    public int Calls;
    public Func<RelayMessage, Exception?>? Fail { get; set; }
    public TimeSpan Delay { get; set; }
    private int _concurrent, _maxConcurrentPerKey;
    private readonly Dictionary<string, int> _perKey = [];
    public int MaxConcurrentPerKey => _maxConcurrentPerKey;

    public async Task DeliverAsync(RelayMessage m, CancellationToken ct)
    {
        Interlocked.Increment(ref Calls);
        var k = m.OrderingKey ?? "";
        lock (_perKey) { _perKey[k] = _perKey.GetValueOrDefault(k) + 1; _maxConcurrentPerKey = Math.Max(_maxConcurrentPerKey, _perKey[k]); }
        try
        {
            if (Delay > TimeSpan.Zero) await Task.Delay(Delay, ct);
            if (Fail?.Invoke(m) is { } ex) throw ex;
            lock (Delivered) Delivered.Add((m.Id, Encoding.UTF8.GetString(m.Payload), m.OrderingKey, m.Attempts));
        }
        finally { lock (_perKey) _perKey[k]--; }
    }
}

public sealed class Env : IAsyncDisposable
{
    public FakeTime Time { get; } = new(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
    public ServiceProvider Sp { get; }
    public IRelayOutbox Outbox => Sp.GetRequiredService<IRelayOutbox>();
    public IRelayProcessor Processor => Sp.GetRequiredService<IRelayProcessor>();
    public IRelayAdmin Admin => Sp.GetRequiredService<IRelayAdmin>();
    public InMemoryRelayStore Store => (InMemoryRelayStore)Sp.GetRequiredService<IRelayStore>();
    public TestDestination Dest { get; } = new("hook");

    public Env(Action<RelayOptions>? configure = null, Action<IServiceCollection>? more = null, IRelayStore? store = null, bool defaultDest = true)
    {
        var s = new ServiceCollection();
        s.AddLogging(b => b.SetMinimumLevel(LogLevel.None));
        s.AddSingleton<TimeProvider>(Time);
        if (store is not null) s.AddSingleton(store);
        s.AddRelay(o =>
        {
            o.WorkerId = "worker-1"; o.BaseDelay = TimeSpan.FromSeconds(10); o.MaxDelay = TimeSpan.FromMinutes(10); o.MaxAttempts = 4;
            o.MaxConcurrency = 1; o.CircuitFailureThreshold = 0; configure?.Invoke(o);
        });
        if (defaultDest) s.AddSingleton<IRelayDestination>(Dest);
        more?.Invoke(s);
        Sp = s.BuildServiceProvider();
    }

    public Task<Guid> Send(string payload = "p", string? key = null, string dest = "hook", DateTimeOffset? notBefore = null, Guid? id = null)
        => Outbox.EnqueueAsync(new RelayEnvelope { Destination = dest, Payload = Encoding.UTF8.GetBytes(payload), OrderingKey = key, NotBefore = notBefore, MessageId = id });

    public async Task Drain(int passes = 10) { for (var i = 0; i < passes; i++) if (await Processor.ProcessOnceAsync() == 0) break; }

    public ValueTask DisposeAsync() => Sp.DisposeAsync();
}

public class DeliveryTests
{
    [Fact]
    public async Task Delivered_messages_are_removed_and_keep_no_history()
    {
        await using var env = new Env();
        var id = await env.Send("hello", key: "k1");
        var m = env.Store.Snapshot().Single();
        Assert.Equal(("hook", "k1", "hello"), (m.Destination, m.OrderingKey, Encoding.UTF8.GetString(m.Payload)));

        Assert.Equal(1, await env.Processor.ProcessOnceAsync());

        Assert.Equal([(id, "hello", "k1", 1)], env.Dest.Delivered);
        Assert.Empty(env.Store.Snapshot());
        var stats = await env.Admin.GetStatsAsync();
        Assert.Equal((0, 0, 0), (stats.Pending, stats.InFlight, stats.DeadLettered));
    }

    [Fact]
    public async Task Payload_is_opaque_bytes_delivered_untouched()
    {
        await using var env = new Env();
        byte[] blob = [0, 1, 2, 255, 254, 0, 0];
        await env.Outbox.EnqueueAsync(new RelayEnvelope { Destination = "hook", Payload = blob, Headers = new Dictionary<string, string> { ["x"] = "1" } });
        byte[]? seen = null; string? header = null;
        await using var env2 = new Env(defaultDest: false, more: s => s.AddRelayDestination("bin", (m, _) => { seen = m.Payload; header = m.Headers["x"]; return Task.CompletedTask; }));
        await env2.Outbox.EnqueueAsync(new RelayEnvelope { Destination = "bin", Payload = blob, Headers = new Dictionary<string, string> { ["x"] = "1" } });
        await env2.Processor.ProcessOnceAsync();
        Assert.Equal(blob, seen);
        Assert.Equal("1", header);
    }

    [Fact]
    public async Task Delayed_messages_wait_for_their_time()
    {
        await using var env = new Env();
        await env.Send("later", notBefore: env.Time.Now.AddMinutes(5));
        Assert.Equal(0, await env.Processor.ProcessOnceAsync());
        env.Time.Advance(TimeSpan.FromMinutes(5));
        Assert.Equal(1, await env.Processor.ProcessOnceAsync());
    }

    [Fact]
    public async Task Enqueue_is_idempotent_for_a_repeated_message_id()
    {
        await using var env = new Env();
        var id = Guid.NewGuid();
        await env.Send("a", id: id); await env.Send("a-again", id: id);
        Assert.Single(env.Store.Snapshot());
        await env.Drain();
        Assert.Equal(["a"], env.Dest.Delivered.Select(d => d.Payload));
    }

    [Fact]
    public async Task Failed_attempts_retry_with_exponential_backoff_then_succeed()
    {
        await using var env = new Env();
        var failures = 2;
        env.Dest.Fail = _ => failures-- > 0 ? new InvalidOperationException("boom") : null;
        await env.Send("x");

        await env.Processor.ProcessOnceAsync();                       // attempt 1 fails
        var afterFirst = env.Store.Snapshot().Single();
        Assert.Equal((RelayStatus.Pending, 1), (afterFirst.Status, afterFirst.Attempts));
        Assert.InRange((afterFirst.AvailableAt - env.Time.Now).TotalSeconds, 5, 10);   // base 10s, equal jitter
        Assert.Equal(0, await env.Processor.ProcessOnceAsync());      // not due yet

        env.Time.Advance(TimeSpan.FromSeconds(11));
        await env.Processor.ProcessOnceAsync();                       // attempt 2 fails
        var afterSecond = env.Store.Snapshot().Single();
        Assert.InRange((afterSecond.AvailableAt - env.Time.Now).TotalSeconds, 10, 20);  // doubled
        Assert.Equal(2, afterSecond.AttemptLog!.Count);

        env.Time.Advance(TimeSpan.FromSeconds(21));
        await env.Processor.ProcessOnceAsync();                       // attempt 3 succeeds
        Assert.Equal(3, env.Dest.Delivered.Single().Attempt);
        Assert.Empty(env.Store.Snapshot());                           // success ⇒ row (and its attempt log) gone
    }

    [Fact]
    public async Task After_max_attempts_the_message_is_dead_lettered_with_its_attempt_log()
    {
        await using var env = new Env();
        env.Dest.Fail = m => new InvalidOperationException($"fail #{m.Attempts}");
        var id = await env.Send("doomed");

        for (var i = 0; i < 4; i++) { await env.Processor.ProcessOnceAsync(); env.Time.Advance(TimeSpan.FromMinutes(11)); }

        Assert.Equal(4, env.Dest.Calls);
        var dead = Assert.Single(await env.Admin.ListDeadLettersAsync());
        Assert.Equal((id, RelayStatus.DeadLettered, 4), (dead.Id, dead.Status, dead.Attempts));
        Assert.Equal(["InvalidOperationException: fail #1", "InvalidOperationException: fail #2", "InvalidOperationException: fail #3", "InvalidOperationException: fail #4"], dead.AttemptLog!.Select(a => a.Error));
        Assert.Equal(0, await env.Processor.ProcessOnceAsync()); // never retried automatically again
        Assert.Equal(1, (await env.Admin.GetStatsAsync()).DeadLettered);
    }

    [Fact]
    public async Task Permanent_failures_dead_letter_immediately()
    {
        await using var env = new Env();
        env.Dest.Fail = _ => new RelayPermanentFailureException("400 bad request");
        await env.Send("bad");
        await env.Processor.ProcessOnceAsync();
        Assert.Equal(1, env.Dest.Calls);
        Assert.Contains("400 bad request", Assert.Single(await env.Admin.ListDeadLettersAsync()).AttemptLog![0].Error);
    }

    [Fact]
    public async Task Retry_after_controls_the_next_attempt_time_and_is_capped()
    {
        await using var env = new Env();
        env.Dest.Fail = _ => new RelayRetryAfterException(TimeSpan.FromMinutes(3), "429");
        await env.Send("x");
        await env.Processor.ProcessOnceAsync();
        Assert.Equal(env.Time.Now.AddMinutes(3), env.Store.Snapshot().Single().AvailableAt);

        await using var capped = new Env();
        capped.Dest.Fail = _ => new RelayRetryAfterException(TimeSpan.FromDays(2), "429");
        await capped.Send("x");
        await capped.Processor.ProcessOnceAsync();
        Assert.Equal(capped.Time.Now.AddMinutes(10), capped.Store.Snapshot().Single().AvailableAt); // MaxDelay
    }

    [Fact]
    public async Task Dead_letters_can_be_requeued_or_purged()
    {
        await using var env = new Env();
        env.Dest.Fail = _ => new RelayPermanentFailureException("nope");
        var a = await env.Send("a"); var b = await env.Send("b");
        await env.Drain();
        Assert.Equal(2, (await env.Admin.ListDeadLettersAsync("hook")).Count);
        Assert.Empty(await env.Admin.ListDeadLettersAsync("other"));

        env.Dest.Fail = null;
        Assert.True(await env.Admin.RequeueAsync(a));
        Assert.True(await env.Admin.PurgeAsync(b));
        Assert.False(await env.Admin.PurgeAsync(Guid.NewGuid()));
        var requeued = env.Store.Snapshot().Single();
        Assert.Equal((0, null), (requeued.Attempts, requeued.AttemptLog)); // fresh start
        await env.Drain();
        Assert.Equal(["a"], env.Dest.Delivered.Select(d => d.Payload));
        Assert.Empty(env.Store.Snapshot());
    }

    [Fact]
    public async Task Messages_for_unregistered_destinations_are_left_alone_without_burning_attempts()
    {
        await using var env = new Env();
        await env.Send("x", dest: "someone-elses");
        Assert.Equal(0, await env.Processor.ProcessOnceAsync());
        var m = env.Store.Snapshot().Single();
        Assert.Equal((RelayStatus.Pending, 0), (m.Status, m.Attempts));
    }

    [Fact]
    public async Task Shutdown_during_delivery_releases_the_message_without_counting_the_attempt()
    {
        await using var env = new Env();
        env.Dest.Delay = TimeSpan.FromSeconds(30);
        await env.Send("slow");
        using var cts = new CancellationTokenSource(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => env.Processor.ProcessOnceAsync(cts.Token));
        var m = env.Store.Snapshot().Single();
        Assert.Equal((RelayStatus.Pending, 0), (m.Status, m.Attempts));
    }
}

public sealed class PrefixDestination(string prefix) : IRelayDestination
{
    public string Name => prefix;
    public bool Handles(string destination) => destination.StartsWith(prefix + ":", StringComparison.Ordinal);
    public List<string> Seen { get; } = [];
    public Func<string, Exception?>? Fail { get; set; }
    public Task DeliverAsync(RelayMessage m, CancellationToken ct)
    {
        if (Fail?.Invoke(m.Destination) is { } ex) throw ex;
        lock (Seen) Seen.Add($"{m.Destination}:{Encoding.UTF8.GetString(m.Payload)}");
        return Task.CompletedTask;
    }
}

public class DestinationFamilyTests
{
    [Fact]
    public async Task A_destination_can_serve_a_family_of_names_each_with_its_own_circuit_and_ordering_scope()
    {
        var family = new PrefixDestination("hook");
        family.Fail = d => d == "hook:bad" ? new InvalidOperationException("down") : null;
        await using var env = new Env(o => { o.CircuitFailureThreshold = 1; o.MaxAttempts = 50; }, s => s.AddSingleton<IRelayDestination>(family), defaultDest: false);
        await env.Send("1", dest: "hook:bad"); await env.Send("2", dest: "hook:good"); await env.Send("3", dest: "hook:bad"); await env.Send("4", dest: "hook:good");

        await env.Drain();
        Assert.Equal(["hook:good:2", "hook:good:4"], family.Seen);           // the bad endpoint's circuit never blocked the good one
        Assert.Equal(["hook:bad", "hook:bad"], env.Store.Snapshot().Select(m => m.Destination));
    }
}

public sealed class ObservingDestination : IRelayDestination
{
    public string Name => "obs";
    public List<(Guid Id, string Reason)> Dead { get; } = [];
    public bool ThrowInCallback { get; set; }
    public Task DeliverAsync(RelayMessage m, CancellationToken ct) => throw (Encoding.UTF8.GetString(m.Payload) == "perm" ? new RelayPermanentFailureException("nope") : new InvalidOperationException("boom"));
    public Task OnDeadLetteredAsync(RelayMessage m, string reason, CancellationToken ct)
    {
        if (ThrowInCallback) throw new InvalidOperationException("callback bug");
        Dead.Add((m.Id, reason));
        return Task.CompletedTask;
    }
}

public class DeadLetterCallbackTests
{
    [Fact]
    public async Task Destinations_are_told_when_a_message_is_dead_lettered_permanently_or_after_exhausting_attempts()
    {
        var obs = new ObservingDestination();
        await using var env = new Env(more: s => s.AddSingleton<IRelayDestination>(obs), defaultDest: false);
        var perm = await env.Send("perm", dest: "obs");
        await env.Processor.ProcessOnceAsync();
        Assert.Equal([(perm, "nope")], obs.Dead);

        var tired = await env.Send("retry", dest: "obs");
        for (var i = 0; i < 4; i++) { await env.Processor.ProcessOnceAsync(); env.Time.Advance(TimeSpan.FromMinutes(11)); }
        Assert.Equal(tired, obs.Dead[1].Id);
        Assert.Contains("boom", obs.Dead[1].Reason);
        Assert.Equal(2, obs.Dead.Count); // not called for intermediate retries
    }

    [Fact]
    public async Task A_throwing_callback_cannot_break_delivery_processing()
    {
        var obs = new ObservingDestination { ThrowInCallback = true };
        await using var env = new Env(more: s => s.AddSingleton<IRelayDestination>(obs), defaultDest: false);
        await env.Send("perm", dest: "obs");
        await env.Processor.ProcessOnceAsync();
        Assert.Single(await env.Admin.ListDeadLettersAsync());
    }
}

public class OrderingTests
{
    [Fact]
    public async Task Same_key_messages_are_delivered_in_enqueue_order()
    {
        await using var env = new Env();
        foreach (var i in Enumerable.Range(1, 6)) await env.Send($"m{i}", key: "order-1");
        await env.Drain();
        Assert.Equal(["m1", "m2", "m3", "m4", "m5", "m6"], env.Dest.Delivered.Select(d => d.Payload));
    }

    [Fact]
    public async Task A_failing_head_blocks_its_key_but_not_other_keys_or_unkeyed_messages()
    {
        await using var env = new Env();
        env.Dest.Fail = m => Encoding.UTF8.GetString(m.Payload) == "a1" ? new InvalidOperationException("boom") : null;
        await env.Send("a1", key: "A"); await env.Send("a2", key: "A"); await env.Send("b1", key: "B"); await env.Send("free");

        await env.Processor.ProcessOnceAsync();
        Assert.Equal(["b1", "free"], env.Dest.Delivered.Select(d => d.Payload).Order());      // a2 must wait behind a1
        Assert.DoesNotContain(env.Dest.Delivered, d => d.Payload == "a2");

        env.Time.Advance(TimeSpan.FromSeconds(30)); env.Dest.Fail = null;
        await env.Drain();
        Assert.Equal(["a1", "a2"], env.Dest.Delivered.Where(d => d.Key == "A").Select(d => d.Payload)); // order preserved after recovery
    }

    [Fact]
    public async Task Dead_lettering_the_head_unblocks_the_rest_of_the_key()
    {
        await using var env = new Env();
        env.Dest.Fail = m => Encoding.UTF8.GetString(m.Payload) == "poison" ? new RelayPermanentFailureException("bad") : null;
        await env.Send("poison", key: "K"); await env.Send("after", key: "K");
        await env.Drain();
        Assert.Equal(["after"], env.Dest.Delivered.Select(d => d.Payload));
        Assert.Single(await env.Admin.ListDeadLettersAsync());
    }

    [Fact]
    public async Task Keys_are_scoped_per_destination()
    {
        await using var env = new Env(more: s => s.AddRelayDestination("other", (m, _) => Task.CompletedTask));
        env.Dest.Fail = _ => new InvalidOperationException("boom");
        await env.Send("h1", key: "same", dest: "hook");
        await env.Send("o1", key: "same", dest: "other");
        await env.Processor.ProcessOnceAsync();
        Assert.Equal(["hook"], env.Store.Snapshot().Select(m => m.Destination)); // "other" delivered despite the shared key
    }

    [Fact]
    public async Task At_most_one_message_per_key_is_ever_in_flight_even_with_parallel_workers()
    {
        await using var env = new Env(o => { o.MaxConcurrency = 8; o.BatchSize = 50; });
        env.Dest.Delay = TimeSpan.FromMilliseconds(15);
        foreach (var i in Enumerable.Range(0, 5)) foreach (var key in new[] { "A", "B", "C" }) await env.Send($"{key}{i}", key: key);
        await env.Drain(50);
        Assert.Equal(15, env.Dest.Delivered.Count);
        Assert.Equal(1, env.Dest.MaxConcurrentPerKey);
        foreach (var key in new[] { "A", "B", "C" })
            Assert.Equal(Enumerable.Range(0, 5).Select(i => $"{key}{i}"), env.Dest.Delivered.Where(d => d.Key == key).Select(d => d.Payload));
    }
}

public class ReliabilityTests
{
    [Fact]
    public async Task Expired_leases_are_reclaimed_and_the_stale_worker_cannot_complete()
    {
        var store = new InMemoryRelayStore();
        await using var env = new Env(store: store);
        var id = await env.Send("x");

        var first = await store.ClaimAsync("crashed-worker", 10, TimeSpan.FromMinutes(2), env.Time.Now, new HashSet<string>(), default);
        Assert.Single(first);                                                     // claimed… then the worker dies
        Assert.Empty(await store.ClaimAsync("w2", 10, TimeSpan.FromMinutes(2), env.Time.Now, new HashSet<string>(), default)); // lease still valid

        env.Time.Advance(TimeSpan.FromMinutes(3));
        var second = Assert.Single(await store.ClaimAsync("w2", 10, TimeSpan.FromMinutes(2), env.Time.Now, new HashSet<string>(), default));
        Assert.Equal(2, second.Attempts);
        Assert.False(await store.CompleteAsync(id, "crashed-worker", default));   // fenced out
        Assert.True(await store.CompleteAsync(id, "w2", default));
    }

    [Fact]
    public async Task Circuit_breaker_stops_hammering_a_failing_destination_then_probes_and_recovers()
    {
        await using var env = new Env(o => { o.CircuitFailureThreshold = 2; o.CircuitBreakDuration = TimeSpan.FromMinutes(1); o.MaxAttempts = 50; });
        env.Dest.Fail = _ => new InvalidOperationException("down");
        foreach (var i in Enumerable.Range(1, 5)) await env.Send($"m{i}");

        await env.Processor.ProcessOnceAsync();
        Assert.Equal(2, env.Dest.Calls);                                          // two failures opened the circuit; the rest weren't tried
        Assert.All(env.Store.Snapshot().Skip(2), m => Assert.Equal(0, m.Attempts));   // untouched: not the messages' fault

        Assert.Equal(0, await env.Processor.ProcessOnceAsync());                  // still open: nothing is even claimed
        Assert.Equal(2, env.Dest.Calls);

        env.Dest.Fail = null;
        env.Time.Advance(TimeSpan.FromMinutes(2));                                // break over (also past retry delays)
        await env.Drain();
        Assert.Equal(5, env.Dest.Delivered.Count);
        Assert.Empty(env.Store.Snapshot());
    }

    [Fact]
    public async Task One_destinations_circuit_does_not_affect_another()
    {
        var other = new TestDestination("other");
        await using var env = new Env(o => o.CircuitFailureThreshold = 1, more: s => s.AddSingleton<IRelayDestination>(other));
        env.Dest.Fail = _ => new InvalidOperationException("down");
        await env.Send("h"); await env.Send("o", dest: "other");
        await env.Processor.ProcessOnceAsync();
        await env.Processor.ProcessOnceAsync();
        Assert.Equal(["o"], other.Delivered.Select(d => d.Payload));
    }

    [Fact]
    public async Task Half_open_admits_a_single_probe()
    {
        await using var env = new Env(o => { o.CircuitFailureThreshold = 1; o.CircuitBreakDuration = TimeSpan.FromMinutes(1); o.MaxAttempts = 50; o.MaxConcurrency = 1; });
        env.Dest.Fail = _ => new InvalidOperationException("down");
        foreach (var i in Enumerable.Range(1, 3)) await env.Send($"m{i}");
        await env.Processor.ProcessOnceAsync();                    // opens
        env.Time.Advance(TimeSpan.FromMinutes(5));
        var before = env.Dest.Calls;
        await env.Processor.ProcessOnceAsync();                    // probe fails → reopens; siblings not tried
        Assert.Equal(before + 1, env.Dest.Calls);
    }
}

public class HostingTests
{
    [Fact]
    public async Task Background_dispatcher_delivers_and_stops_cleanly()
    {
        var dest = new TestDestination("hook");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRelay(o => o.PollInterval = TimeSpan.FromMilliseconds(10));
        services.AddSingleton<IRelayDestination>(dest);
        services.AddRelayDispatcher();
        await using var sp = services.BuildServiceProvider();

        var hosted = sp.GetServices<Microsoft.Extensions.Hosting.IHostedService>().Single();
        await hosted.StartAsync(default);
        await sp.GetRequiredService<IRelayOutbox>().EnqueueAsync(new RelayEnvelope { Destination = "hook", Payload = "hi"u8.ToArray() });

        var until = DateTime.UtcNow.AddSeconds(5);
        while (dest.Delivered.Count == 0 && DateTime.UtcNow < until) await Task.Delay(20);
        await hosted.StopAsync(default);

        Assert.Equal(["hi"], dest.Delivered.Select(d => d.Payload));
    }

    [Fact]
    public async Task Wolverine_hands_messages_to_relay_which_then_owns_delivery()
    {
        var dest = new TestDestination("hook");
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddRelay();
        builder.Services.AddSingleton<IRelayDestination>(dest);
        builder.UseWolverine(opts => opts.IncludeRelay());
        using var host = builder.Build();
        await host.StartAsync();

        var bus = host.Services.GetRequiredService<Wolverine.IMessageBus>();
        var id = Guid.NewGuid();
        await bus.InvokeAsync(new EnqueueRelayMessage("hook", "via-wolverine"u8.ToArray(), OrderingKey: "k", MessageId: id));

        var store = (InMemoryRelayStore)host.Services.GetRequiredService<IRelayStore>();
        Assert.Equal(id, store.Snapshot().Single().Id);           // handed over…
        await host.Services.GetRequiredService<IRelayProcessor>().ProcessOnceAsync();
        Assert.Equal(["via-wolverine"], dest.Delivered.Select(d => d.Payload)); // …and delivered by Relay
        await host.StopAsync();
    }
}
