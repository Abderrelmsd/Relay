namespace Relay;

/// <summary>Producer API: durable enqueue. Delivery is at-least-once — destinations must be idempotent (use the message id).</summary>
public interface IRelayOutbox
{
    /// <summary>Persists the message. Returns its id. Enqueueing an existing <see cref="RelayEnvelope.MessageId"/> is a no-op (idempotent producers).</summary>
    Task<Guid> EnqueueAsync(RelayEnvelope envelope, CancellationToken cancellationToken = default);
}

/// <summary>Consumer side: where messages for a destination name go. Throw to fail the attempt.</summary>
public interface IRelayDestination
{
    string Name { get; }

    /// <summary>
    /// Whether this handler serves <paramref name="destination"/>. Defaults to an exact match on <see cref="Name"/>; override to serve a
    /// family of names (e.g. <c>webhook:{endpointId}</c>) so each gets its own circuit breaker and ordering scope.
    /// </summary>
    bool Handles(string destination) => destination == Name;

    /// <summary>Called after a message for this destination was dead-lettered (permanent failure or attempts exhausted), so owners can record the final failure. Exceptions are swallowed.</summary>
    Task OnDeadLetteredAsync(RelayMessage message, string reason, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>Deliver <paramref name="message"/>. Return normally = delivered. Throw = retry with backoff; <see cref="RelayPermanentFailureException"/> = dead-letter now; <see cref="RelayRetryAfterException"/> = retry at a chosen time.</summary>
    Task DeliverAsync(RelayMessage message, CancellationToken cancellationToken);
}

/// <summary>Persistence contract. Claiming must honour per-key ordering: a keyed message is claimable only when no earlier undelivered (Pending or InFlight) message exists for the same (destination, key).</summary>
public interface IRelayStore
{
    /// <summary>Inserts; false if a message with that id already exists.</summary>
    Task<bool> EnqueueAsync(RelayMessage message, CancellationToken ct);

    /// <summary>Atomically claims up to <paramref name="max"/> deliverable messages (Pending and due, or InFlight with an expired lease), incrementing Attempts.</summary>
    Task<IReadOnlyList<RelayMessage>> ClaimAsync(string workerId, int max, TimeSpan lease, DateTimeOffset now, IReadOnlySet<string> excludedDestinations, CancellationToken ct);

    /// <summary>Delivered: the message is removed (no history is kept for successes).</summary>
    Task<bool> CompleteAsync(Guid id, string workerId, CancellationToken ct);

    /// <summary>Failed transiently: back to Pending at <paramref name="availableAt"/> with the attempt appended to its log.</summary>
    Task<bool> RetryLaterAsync(Guid id, string workerId, DateTimeOffset availableAt, AttemptRecord attempt, CancellationToken ct);

    /// <summary>Gives up: status DeadLettered, attempt appended. Later messages of the same key are then unblocked.</summary>
    Task<bool> DeadLetterAsync(Guid id, string workerId, AttemptRecord attempt, CancellationToken ct);

    /// <summary>Returns a claimed message without counting the attempt (circuit open, shutdown).</summary>
    Task<bool> ReleaseAsync(Guid id, string workerId, CancellationToken ct);

    Task<IReadOnlyList<RelayMessage>> ListDeadLettersAsync(string? destination, int skip, int take, CancellationToken ct);

    /// <summary>Dead letter → Pending, attempts reset, log cleared.</summary>
    Task<bool> RequeueDeadLetterAsync(Guid id, DateTimeOffset now, CancellationToken ct);

    Task<bool> PurgeDeadLetterAsync(Guid id, CancellationToken ct);
    Task<RelayStats> GetStatsAsync(CancellationToken ct);
}
