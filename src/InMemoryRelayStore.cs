namespace Relay;

/// <summary>Reference implementation of the store contract (single process). Used for tests and dev; the Postgres store follows the same rules.</summary>
public sealed class InMemoryRelayStore : IRelayStore
{
    private readonly Lock _gate = new();
    private readonly List<RelayMessage> _messages = [];
    private long _sequence;

    public Task<bool> EnqueueAsync(RelayMessage message, CancellationToken ct)
    {
        lock (_gate)
        {
            if (_messages.Any(m => m.Id == message.Id)) return Task.FromResult(false);
            _messages.Add(message with { Sequence = ++_sequence, Status = RelayStatus.Pending, Attempts = 0, AttemptLog = null });
            return Task.FromResult(true);
        }
    }

    public Task<IReadOnlyList<RelayMessage>> ClaimAsync(string workerId, int max, TimeSpan lease, DateTimeOffset now, IReadOnlySet<string> excluded, CancellationToken ct)
    {
        lock (_gate)
        {
            var claimed = new List<RelayMessage>();
            foreach (var m in _messages.Where(m => m.Status != RelayStatus.DeadLettered).OrderBy(m => m.Sequence).ToList())
            {
                if (claimed.Count >= max) break;
                var claimable = (m.Status == RelayStatus.Pending && m.AvailableAt <= now) || (m.Status == RelayStatus.InFlight && m.ClaimedUntil <= now);
                if (!claimable || excluded.Contains(m.Destination)) continue;
                if (m.OrderingKey is not null && _messages.Any(o => o.Sequence < m.Sequence && o.Destination == m.Destination && o.OrderingKey == m.OrderingKey && o.Status != RelayStatus.DeadLettered))
                    continue; // an earlier undelivered message for this key exists (or is in flight)

                var updated = m with { Status = RelayStatus.InFlight, ClaimedBy = workerId, ClaimedUntil = now + lease, Attempts = m.Attempts + 1 };
                _messages[_messages.FindIndex(x => x.Id == m.Id)] = updated;
                claimed.Add(updated);
            }
            return Task.FromResult<IReadOnlyList<RelayMessage>>(claimed);
        }
    }

    private bool Update(Guid id, string workerId, Func<RelayMessage, RelayMessage?> change)
    {
        lock (_gate)
        {
            var i = _messages.FindIndex(m => m.Id == id && m.Status == RelayStatus.InFlight && m.ClaimedBy == workerId);
            if (i < 0) return false; // lease lost: someone else owns it now
            var next = change(_messages[i]);
            if (next is null) _messages.RemoveAt(i); else _messages[i] = next;
            return true;
        }
    }

    public Task<bool> CompleteAsync(Guid id, string workerId, CancellationToken ct) => Task.FromResult(Update(id, workerId, _ => null));

    public Task<bool> RetryLaterAsync(Guid id, string workerId, DateTimeOffset availableAt, AttemptRecord attempt, CancellationToken ct)
        => Task.FromResult(Update(id, workerId, m => m with { Status = RelayStatus.Pending, AvailableAt = availableAt, ClaimedBy = null, ClaimedUntil = null, AttemptLog = [.. m.AttemptLog ?? [], attempt] }));

    public Task<bool> DeadLetterAsync(Guid id, string workerId, AttemptRecord attempt, CancellationToken ct)
        => Task.FromResult(Update(id, workerId, m => m with { Status = RelayStatus.DeadLettered, ClaimedBy = null, ClaimedUntil = null, AttemptLog = [.. m.AttemptLog ?? [], attempt] }));

    public Task<bool> ReleaseAsync(Guid id, string workerId, CancellationToken ct)
        => Task.FromResult(Update(id, workerId, m => m with { Status = RelayStatus.Pending, ClaimedBy = null, ClaimedUntil = null, Attempts = Math.Max(0, m.Attempts - 1) }));

    public Task<IReadOnlyList<RelayMessage>> ListDeadLettersAsync(string? destination, int skip, int take, CancellationToken ct)
    {
        lock (_gate)
            return Task.FromResult<IReadOnlyList<RelayMessage>>([.. _messages.Where(m => m.Status == RelayStatus.DeadLettered && (destination is null || m.Destination == destination))
                .OrderBy(m => m.Sequence).Skip(skip).Take(take)]);
    }

    public Task<bool> RequeueDeadLetterAsync(Guid id, DateTimeOffset now, CancellationToken ct)
    {
        lock (_gate)
        {
            var i = _messages.FindIndex(m => m.Id == id && m.Status == RelayStatus.DeadLettered);
            if (i < 0) return Task.FromResult(false);
            _messages[i] = _messages[i] with { Status = RelayStatus.Pending, Attempts = 0, AvailableAt = now, AttemptLog = null };
            return Task.FromResult(true);
        }
    }

    public Task<bool> PurgeDeadLetterAsync(Guid id, CancellationToken ct)
    {
        lock (_gate) return Task.FromResult(_messages.RemoveAll(m => m.Id == id && m.Status == RelayStatus.DeadLettered) > 0);
    }

    public Task<RelayStats> GetStatsAsync(CancellationToken ct)
    {
        lock (_gate)
            return Task.FromResult(new RelayStats(_messages.Count(m => m.Status == RelayStatus.Pending), _messages.Count(m => m.Status == RelayStatus.InFlight),
                _messages.Count(m => m.Status == RelayStatus.DeadLettered), _messages.Where(m => m.Status == RelayStatus.Pending).Select(m => (DateTimeOffset?)m.CreatedAt).Min()));
    }

    /// <summary>Test hook: all rows, in sequence order.</summary>
    public IReadOnlyList<RelayMessage> Snapshot() { lock (_gate) return [.. _messages.OrderBy(m => m.Sequence)]; }
}
