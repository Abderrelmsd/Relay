namespace Relay;

public enum RelayStatus { Pending = 0, InFlight = 1, DeadLettered = 2 }

/// <summary>One failed delivery attempt. Only dead-lettered messages keep their attempt log.</summary>
public sealed record AttemptRecord(int Attempt, DateTimeOffset At, TimeSpan Duration, string Error);

/// <summary>A queued message. <see cref="Payload"/> is opaque bytes: Relay never inspects it.</summary>
public sealed record RelayMessage(
    Guid Id,
    string Destination,
    string? OrderingKey,
    byte[] Payload,
    IReadOnlyDictionary<string, string> Headers,
    DateTimeOffset CreatedAt,
    DateTimeOffset AvailableAt,
    int Attempts = 0,
    RelayStatus Status = RelayStatus.Pending,
    string? ClaimedBy = null,
    DateTimeOffset? ClaimedUntil = null,
    string? TenantId = null,
    long Sequence = 0,
    IReadOnlyList<AttemptRecord>? AttemptLog = null);

/// <summary>What to enqueue. Same <see cref="OrderingKey"/> + <see cref="Destination"/> ⇒ delivered strictly in enqueue order.</summary>
public sealed class RelayEnvelope
{
    public required string Destination { get; init; }
    public required byte[] Payload { get; init; }
    public string? OrderingKey { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
    public string? TenantId { get; init; }
    /// <summary>Earliest delivery time (delayed messages).</summary>
    public DateTimeOffset? NotBefore { get; init; }
    /// <summary>Supply to make enqueue idempotent for retried producers (default: new id).</summary>
    public Guid? MessageId { get; init; }
}

public sealed record RelayStats(long Pending, long InFlight, long DeadLettered, DateTimeOffset? OldestPendingAt);

/// <summary>Throw from a destination to dead-letter immediately (no retries), e.g. HTTP 400 from a webhook.</summary>
public sealed class RelayPermanentFailureException(string message, Exception? inner = null) : Exception(message, inner);

/// <summary>Throw from a destination to control when the next attempt happens (e.g. honouring Retry-After).</summary>
public sealed class RelayRetryAfterException(TimeSpan delay, string message, Exception? inner = null) : Exception(message, inner)
{
    public TimeSpan Delay { get; } = delay;
}
