using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Relay;

internal sealed class RelayOutbox(IEnumerable<IRelayStore> stores, TimeProvider time) : IRelayOutbox
{
    // The first store registered receives messages enqueued without a transaction.
    private readonly IRelayStore store = stores.First();

    public async Task<Guid> EnqueueAsync(RelayEnvelope e, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(e.Destination)) throw new ArgumentException("Destination is required.", nameof(e));
        var now = time.GetUtcNow();
        var id = e.MessageId ?? Guid.NewGuid();
        await store.EnqueueAsync(new RelayMessage(id, e.Destination, e.OrderingKey, e.Payload, e.Headers ?? new Dictionary<string, string>(), now, e.NotBefore ?? now, TenantId: e.TenantId), ct);
        return id;
    }
}

/// <summary>Per-destination circuit breaker (per process): consecutive transient failures open it; after the break one probe message is admitted.</summary>
internal sealed class DestinationBreakers(IOptions<RelayOptions> options, TimeProvider time)
{
    private sealed class State { public int Failures; public DateTimeOffset? OpenedAt; public bool Probing; }
    private readonly ConcurrentDictionary<string, State> _states = new();
    private readonly RelayOptions _o = options.Value;

    public IReadOnlySet<string> OpenDestinations()
    {
        if (_o.CircuitFailureThreshold <= 0) return new HashSet<string>();
        var now = time.GetUtcNow();
        return _states.Where(kv => { lock (kv.Value) return kv.Value.OpenedAt is { } o && (now < o + _o.CircuitBreakDuration || kv.Value.Probing); }).Select(kv => kv.Key).ToHashSet();
    }

    /// <summary>True if delivery may proceed. In half-open state only one probe is admitted.</summary>
    public bool TryAcquire(string destination)
    {
        if (_o.CircuitFailureThreshold <= 0) return true;
        var s = _states.GetOrAdd(destination, _ => new State());
        lock (s)
        {
            if (s.OpenedAt is null) return true;
            if (time.GetUtcNow() < s.OpenedAt + _o.CircuitBreakDuration || s.Probing) return false;
            s.Probing = true;
            return true;
        }
    }

    public void Success(string destination)
    {
        if (!_states.TryGetValue(destination, out var s)) return;
        lock (s) { s.Failures = 0; s.OpenedAt = null; s.Probing = false; }
    }

    public void Failure(string destination)
    {
        if (_o.CircuitFailureThreshold <= 0) return;
        var s = _states.GetOrAdd(destination, _ => new State());
        lock (s)
        {
            s.Probing = false;
            if (s.OpenedAt is not null || ++s.Failures >= _o.CircuitFailureThreshold) s.OpenedAt = time.GetUtcNow();
        }
    }

    public bool IsOpen(string destination) => OpenDestinations().Contains(destination);
}

/// <summary>One claim-and-deliver pass. Exposed so hosts/tests can drive delivery deterministically; <see cref="RelayDispatcher"/> loops it.</summary>
public interface IRelayProcessor
{
    /// <summary>Claims a batch and delivers it. Returns the number of messages handled (delivered, retried or dead-lettered).</summary>
    Task<int> ProcessOnceAsync(CancellationToken cancellationToken = default);
}

internal sealed class RelayProcessor(IEnumerable<IRelayStore> stores, IEnumerable<IRelayDestination> destinations, DestinationBreakers breakers,
    IOptions<RelayOptions> options, TimeProvider time, ILogger<RelayProcessor> logger) : IRelayProcessor
{
    private readonly IRelayStore[] _stores = stores.ToArray();
    private readonly RelayOptions _o = options.Value;
    private readonly IRelayDestination[] _all = destinations.ToArray();
    private readonly ConcurrentDictionary<string, IRelayDestination?> _resolved = new(StringComparer.Ordinal);
    private readonly string _worker = options.Value.WorkerId ?? $"{Environment.MachineName}:{Environment.ProcessId}:{Guid.NewGuid():N}"[..40];

    public async Task<int> ProcessOnceAsync(CancellationToken ct = default)
    {
        var handled = 0;
        foreach (var store in _stores) handled += await ProcessStoreAsync(store, ct);
        return handled;
    }

    // Each store (one per database, say) is claimed and delivered in turn; circuit breakers are shared, as destinations are.
    private async Task<int> ProcessStoreAsync(IRelayStore store, CancellationToken ct)
    {
        var claimed = await store.ClaimAsync(_worker, _o.BatchSize, _o.LeaseDuration, time.GetUtcNow(), breakers.OpenDestinations(), ct);
        if (claimed.Count == 0) return 0;

        var handled = 0;
        await Parallel.ForEachAsync(claimed, new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, _o.MaxConcurrency), CancellationToken = ct }, async (m, token) =>
        {
            if (await Deliver(store, m, token)) Interlocked.Increment(ref handled);
        });
        return handled;
    }

    private async Task<bool> Deliver(IRelayStore store, RelayMessage m, CancellationToken ct)
    {
        var destination = _resolved.GetOrAdd(m.Destination, name => _all.FirstOrDefault(d => d.Name == name) ?? _all.FirstOrDefault(d => d.Handles(name)));
        if (destination is null)
        {
            // No handler registered here (maybe another service owns it): hand it back rather than burn attempts.
            logger.LogWarning("No Relay destination named '{Destination}' is registered on this instance", m.Destination);
            await store.ReleaseAsync(m.Id, _worker, ct);
            return false;
        }
        if (!breakers.TryAcquire(m.Destination))
        {
            await store.ReleaseAsync(m.Id, _worker, ct); // circuit open: not the message's fault
            return false;
        }

        var started = Stopwatch.GetTimestamp();
        try
        {
            await destination.DeliverAsync(m, ct);
            breakers.Success(m.Destination);
            await store.CompleteAsync(m.Id, _worker, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await store.ReleaseAsync(m.Id, _worker, CancellationToken.None); // shutting down
            return false;
        }
        catch (RelayPermanentFailureException ex)
        {
            breakers.Success(m.Destination); // the destination answered; the message is at fault
            await store.DeadLetterAsync(m.Id, _worker, Record(m, started, ex), ct);
            logger.LogWarning(ex, "Message {Id} to {Destination} dead-lettered: permanent failure", m.Id, m.Destination);
            await NotifyDeadLettered(destination, m, ex.Message);
        }
        catch (Exception ex)
        {
            breakers.Failure(m.Destination);
            var attempt = Record(m, started, ex);
            if (m.Attempts >= _o.MaxAttempts)
            {
                await store.DeadLetterAsync(m.Id, _worker, attempt, ct);
                logger.LogError(ex, "Message {Id} to {Destination} dead-lettered after {Attempts} attempts", m.Id, m.Destination, m.Attempts);
                await NotifyDeadLettered(destination, m, $"{ex.GetType().Name}: {ex.Message}");
            }
            else
            {
                var delay = ex is RelayRetryAfterException r ? Clamp(r.Delay) : Backoff(m.Attempts);
                await store.RetryLaterAsync(m.Id, _worker, time.GetUtcNow() + delay, attempt, ct);
                logger.LogWarning(ex, "Message {Id} to {Destination} failed (attempt {Attempt}); retrying in {Delay}", m.Id, m.Destination, m.Attempts, delay);
            }
        }
        return true;
    }

    private async Task NotifyDeadLettered(IRelayDestination destination, RelayMessage m, string reason)
    {
        try { await destination.OnDeadLetteredAsync(m, reason, CancellationToken.None); }
        catch (Exception ex) { logger.LogError(ex, "Dead-letter callback for {Id} failed", m.Id); }
    }

    private AttemptRecord Record(RelayMessage m, long started, Exception ex)
        => new(m.Attempts, time.GetUtcNow(), Stopwatch.GetElapsedTime(started), $"{ex.GetType().Name}: {ex.Message}");

    private TimeSpan Clamp(TimeSpan d) => d < TimeSpan.Zero ? TimeSpan.Zero : d > _o.MaxDelay ? _o.MaxDelay : d;

    internal TimeSpan Backoff(int attempt)
    {
        var exp = Math.Min(_o.MaxDelay.TotalMilliseconds, _o.BaseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));
        return TimeSpan.FromMilliseconds(exp / 2 + Random.Shared.NextDouble() * exp / 2);
    }
}

/// <summary>Background loop that keeps calling the processor, sleeping when the queue is idle.</summary>
internal sealed class RelayDispatcher(IRelayProcessor processor, IOptions<RelayOptions> options, ILogger<RelayDispatcher> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var handled = 0;
            try { handled = await processor.ProcessOnceAsync(stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex) { logger.LogError(ex, "Relay dispatch pass failed"); }
            if (handled == 0)
                try { await Task.Delay(options.Value.PollInterval, stoppingToken); } catch (OperationCanceledException) { break; }
        }
    }
}

/// <summary>Operations API (Atrium surfaces this): inspect and manage the dead-letter queue.</summary>
public interface IRelayAdmin
{
    Task<IReadOnlyList<RelayMessage>> ListDeadLettersAsync(string? destination = null, int skip = 0, int take = 50, CancellationToken ct = default);
    Task<bool> RequeueAsync(Guid messageId, CancellationToken ct = default);
    Task<bool> PurgeAsync(Guid messageId, CancellationToken ct = default);
    Task<RelayStats> GetStatsAsync(CancellationToken ct = default);
}

/// <summary>Across every store: dead letters listed store by store, a requeue or purge applied where the message is, stats summed.</summary>
internal sealed class RelayAdmin(IEnumerable<IRelayStore> stores, TimeProvider time) : IRelayAdmin
{
    private readonly IRelayStore[] _stores = stores.ToArray();

    public async Task<IReadOnlyList<RelayMessage>> ListDeadLettersAsync(string? destination = null, int skip = 0, int take = 50, CancellationToken ct = default)
    {
        var all = new List<RelayMessage>();
        take = Math.Clamp(take, 1, 500);
        foreach (var store in _stores) all.AddRange(await store.ListDeadLettersAsync(destination, 0, Math.Max(0, skip) + take, ct));
        return [.. all.OrderBy(m => m.CreatedAt).Skip(Math.Max(0, skip)).Take(take)];
    }

    public async Task<bool> RequeueAsync(Guid messageId, CancellationToken ct = default)
    {
        foreach (var store in _stores) if (await store.RequeueDeadLetterAsync(messageId, time.GetUtcNow(), ct)) return true;
        return false;
    }

    public async Task<bool> PurgeAsync(Guid messageId, CancellationToken ct = default)
    {
        foreach (var store in _stores) if (await store.PurgeDeadLetterAsync(messageId, ct)) return true;
        return false;
    }

    public async Task<RelayStats> GetStatsAsync(CancellationToken ct = default)
    {
        var all = new List<RelayStats>();
        foreach (var store in _stores) all.Add(await store.GetStatsAsync(ct));
        return new RelayStats(all.Sum(s => s.Pending), all.Sum(s => s.InFlight), all.Sum(s => s.DeadLettered), all.Min(s => s.OldestPendingAt));
    }
}
