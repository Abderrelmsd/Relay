namespace Relay;

public sealed class RelayOptions
{
    public const string SectionName = "Relay";

    /// <summary>Attempts (including the first) before a message is dead-lettered.</summary>
    public int MaxAttempts { get; set; } = 8;
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromHours(1);
    /// <summary>How long a claim lasts before another worker may take the message (crash recovery). Must exceed the slowest delivery.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(2);
    public int BatchSize { get; set; } = 50;
    public int MaxConcurrency { get; set; } = 8;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Consecutive transient failures of one destination that open its circuit (0 disables).</summary>
    public int CircuitFailureThreshold { get; set; } = 5;
    public TimeSpan CircuitBreakDuration { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Identifies this instance in claims. Default: machine name + process id.</summary>
    public string? WorkerId { get; set; }
}
