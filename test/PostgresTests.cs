using System.Text;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Relay.Tests;

public sealed class PgFixture : IAsyncLifetime
{
    private PostgreSqlContainer? _c;
    public bool Available { get; private set; }
    public string? Reason { get; private set; }
    public NpgsqlDataSource? DataSource { get; private set; }

    public async ValueTask InitializeAsync()
    {
        try
        {
            _c = new PostgreSqlBuilder("postgres:17-alpine").Build();
            await _c.StartAsync();
            DataSource = NpgsqlDataSource.Create(_c.GetConnectionString());
            await new PostgresRelayStore(DataSource).EnsureSchemaAsync();
            Available = true;
        }
        catch (Exception ex) { Reason = "Docker/Postgres unavailable: " + ex.GetType().Name; }
    }

    public async ValueTask DisposeAsync()
    {
        if (DataSource is not null) await DataSource.DisposeAsync();
        if (_c is not null) await _c.DisposeAsync();
    }
}

public class PostgresTests(PgFixture pg) : IClassFixture<PgFixture>
{
    private static RelayMessage Msg(string payload, string? key = null, string dest = "d") =>
        new(Guid.NewGuid(), dest, key, Encoding.UTF8.GetBytes(payload), new Dictionary<string, string> { ["h"] = "v" }, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddSeconds(-1));

    private static readonly IReadOnlySet<string> None = new HashSet<string>();

    [Fact]
    public async Task Claim_complete_retry_dead_letter_roundtrip()
    {
        Assert.SkipUnless(pg.Available, pg.Reason);
        var store = new PostgresRelayStore(pg.DataSource!);
        var dest = "rt-" + Guid.NewGuid().ToString("N")[..6];
        var m = Msg("hello", "k", dest);
        Assert.True(await store.EnqueueAsync(m, default));
        Assert.False(await store.EnqueueAsync(m, default)); // idempotent

        var claimed = Assert.Single((await store.ClaimAsync("w1", 10, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow, None, default)).Where(x => x.Destination == dest));
        Assert.Equal((1, "hello"), (claimed.Attempts, Encoding.UTF8.GetString(claimed.Payload)));
        Assert.Equal("v", claimed.Headers["h"]);

        var attempt = new AttemptRecord(1, DateTimeOffset.UtcNow, TimeSpan.FromMilliseconds(5), "boom");
        Assert.False(await store.RetryLaterAsync(claimed.Id, "someone-else", DateTimeOffset.UtcNow, attempt, default)); // fenced
        Assert.True(await store.RetryLaterAsync(claimed.Id, "w1", DateTimeOffset.UtcNow.AddSeconds(-1), attempt, default));
        var again = Assert.Single((await store.ClaimAsync("w1", 10, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow, None, default)).Where(x => x.Destination == dest));
        Assert.Equal(2, again.Attempts);
        Assert.True(await store.DeadLetterAsync(again.Id, "w1", attempt with { Attempt = 2 }, default));
        var dead = Assert.Single(await store.ListDeadLettersAsync(dest, 0, 10, default));
        Assert.Equal(2, dead.AttemptLog!.Count);
        Assert.True(await store.RequeueDeadLetterAsync(dead.Id, DateTimeOffset.UtcNow, default));
        var re = Assert.Single((await store.ClaimAsync("w1", 10, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow.AddSeconds(1), None, default)).Where(x => x.Destination == dest));
        Assert.True(await store.CompleteAsync(re.Id, "w1", default));
    }

    [Fact]
    public async Task Keyed_messages_are_claimed_one_at_a_time_in_order_and_workers_do_not_overlap()
    {
        Assert.SkipUnless(pg.Available, pg.Reason);
        var store = new PostgresRelayStore(pg.DataSource!);
        var dest = "ord-" + Guid.NewGuid().ToString("N")[..6];
        foreach (var i in Enumerable.Range(1, 3)) await store.EnqueueAsync(Msg("m" + i, "K", dest), default);

        var a = (await store.ClaimAsync("A", 10, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow, None, default)).Where(x => x.Destination == dest).ToList();
        var b = (await store.ClaimAsync("B", 10, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow, None, default)).Where(x => x.Destination == dest).ToList();
        Assert.Equal(["m1"], a.Select(x => Encoding.UTF8.GetString(x.Payload)));
        Assert.Empty(b);                                             // m2 waits behind in-flight m1
        await store.CompleteAsync(a[0].Id, "A", default);
        var next = (await store.ClaimAsync("B", 10, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow, None, default)).Where(x => x.Destination == dest).ToList();
        Assert.Equal(["m2"], next.Select(x => Encoding.UTF8.GetString(x.Payload)));
    }

    [Fact]
    public async Task Concurrent_claimers_never_receive_the_same_message()
    {
        Assert.SkipUnless(pg.Available, pg.Reason);
        var store = new PostgresRelayStore(pg.DataSource!);
        var dest = "par-" + Guid.NewGuid().ToString("N")[..6];
        foreach (var i in Enumerable.Range(0, 40)) await store.EnqueueAsync(Msg("m" + i, null, dest), default);

        var results = await Task.WhenAll(Enumerable.Range(0, 6).Select(w => Task.Run(async () =>
            (await store.ClaimAsync("w" + w, 10, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow, None, default)).Where(x => x.Destination == dest).Select(x => x.Id).ToList())));
        var all = results.SelectMany(r => r).ToList();
        Assert.Equal(all.Count, all.Distinct().Count());
        Assert.Equal(40, all.Count == 40 ? 40 : all.Count + (await store.ClaimAsync("w9", 100, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow, None, default)).Count(x => x.Destination == dest));
    }

    [Fact]
    public async Task Enqueue_in_the_callers_transaction_rolls_back_with_it()
    {
        Assert.SkipUnless(pg.Available, pg.Reason);
        var store = new PostgresRelayStore(pg.DataSource!);
        var dest = "tx-" + Guid.NewGuid().ToString("N")[..6];
        await using (var conn = await pg.DataSource!.OpenConnectionAsync())
        {
            await using var tx = await conn.BeginTransactionAsync();
            await store.EnqueueInTransactionAsync(conn, tx, Msg("rolled-back", null, dest));
            await tx.RollbackAsync();
        }
        Assert.Empty((await store.ClaimAsync("w", 100, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow, None, default)).Where(x => x.Destination == dest));

        await using (var conn = await pg.DataSource!.OpenConnectionAsync())
        {
            await using var tx = await conn.BeginTransactionAsync();
            await store.EnqueueInTransactionAsync(conn, tx, Msg("committed", null, dest));
            await tx.CommitAsync();
        }
        Assert.Single((await store.ClaimAsync("w", 100, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow, None, default)).Where(x => x.Destination == dest));
    }
}
