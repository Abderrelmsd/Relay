using Wolverine;

namespace Relay;

/// <summary>
/// The hand-off message. Publish it through Wolverine from inside a business handler: Wolverine's own transactional outbox
/// guarantees it is handed over exactly when the business transaction commits. <see cref="RelayEnqueueHandler"/> then moves it into
/// Relay's outbox — and from that point <b>Relay</b> owns claiming, retry, circuit breaking and dead-lettering (not Wolverine).
/// </summary>
public sealed record EnqueueRelayMessage(
    string Destination,
    byte[] Payload,
    string? OrderingKey = null,
    Dictionary<string, string>? Headers = null,
    string? TenantId = null,
    DateTimeOffset? NotBefore = null,
    Guid? MessageId = null);

/// <summary>Wolverine handler for <see cref="EnqueueRelayMessage"/>.</summary>
public static class RelayEnqueueHandler
{
    public static Task Handle(EnqueueRelayMessage message, IRelayOutbox outbox, CancellationToken cancellationToken)
        => outbox.EnqueueAsync(new RelayEnvelope
        {
            Destination = message.Destination, Payload = message.Payload, OrderingKey = message.OrderingKey, Headers = message.Headers,
            TenantId = message.TenantId, NotBefore = message.NotBefore, MessageId = message.MessageId,
        }, cancellationToken);
}

public static class WolverineRelayExtensions
{
    /// <summary>Registers Relay's hand-off handler with Wolverine: <c>opts.IncludeRelay()</c>.</summary>
    public static WolverineOptions IncludeRelay(this WolverineOptions options)
    {
        options.Discovery.IncludeAssembly(typeof(EnqueueRelayMessage).Assembly);
        return options;
    }
}
