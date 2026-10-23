using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Relay.Tests;

/// <summary>Named tables in their own schema, enqueue on the caller's transaction by table name, and one dispatcher draining two stores.</summary>
public class PostgresStoresTests(PgFixture pg) : IClassFixture<PgFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static RelayMessage Msg(string dest, string payload) =>
        new(Guid.NewGuid(), dest, null, Encoding.UTF8.GetBytes(payload), new Dictionary<string, string>(), DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddSeconds(-1));

    private async Task<string> SchemaTableAsync(string schema)
    {
        await using (var cmd = pg.DataSource!.CreateCommand($"CREATE SCHEMA IF NOT EXISTS {schema}")) await cmd.ExecuteNonQueryAsync(Ct);
        var table = schema + ".outbox";
        await new PostgresRelayStore(pg.DataSource!, table).EnsureSchemaAsync(Ct);
        return table;
    }

    [Fact]
    public async Task A_named_table_takes_messages_on_the_callers_transaction()
    {
        Assert.SkipUnless(pg.Available, pg.Reason);
        var table = await SchemaTableAsync("s" + Guid.NewGuid().ToString("N")[..8]);
        var store = new PostgresRelayStore(pg.DataSource!, table);

        await using var connection = await pg.DataSource!.OpenConnectionAsync(Ct);
        await using (var rolledBack = await connection.BeginTransactionAsync(Ct))
        {
            Assert.True(await PostgresRelayStore.EnqueueInTransactionAsync(connection, rolledBack, Msg("d", "gone"), table, Ct));
        }

        await using (var committed = await connection.BeginTransactionAsync(Ct))
        {
            Assert.True(await PostgresRelayStore.EnqueueInTransactionAsync(connection, committed, Msg("d", "kept"), table, Ct));
            await committed.CommitAsync(Ct);
        }

        var claimed = await store.ClaimAsync("w", 10, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow, new HashSet<string>(), Ct);
        Assert.Equal(["kept"], claimed.Select(m => Encoding.UTF8.GetString(m.Payload)));
        Assert.Throws<ArgumentException>(() => new PostgresRelayStore(pg.DataSource!, "x; DROP TABLE y"));
        Assert.Contains($"CREATE TABLE IF NOT EXISTS \"{table.Split('.')[0]}\".\"outbox\"", store.SchemaSql);
    }

    [Fact]
    public async Task One_dispatcher_drains_every_store()
    {
        Assert.SkipUnless(pg.Available, pg.Reason);
        var (first, second) = (await SchemaTableAsync("a" + Guid.NewGuid().ToString("N")[..8]), await SchemaTableAsync("b" + Guid.NewGuid().ToString("N")[..8]));
        var delivered = new List<string>();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(Microsoft.Extensions.Logging.ILogger<>), typeof(NullLogger<>));
        services.AddRelayPostgresStore(pg.DataSource!, first);
        services.AddRelayPostgresStore(pg.DataSource!, second);
        services.AddRelay();
        services.AddRelayDestination("collect", (m, _) => { lock (delivered) delivered.Add(Encoding.UTF8.GetString(m.Payload)); return Task.CompletedTask; });
        await using var sp = services.BuildServiceProvider();

        await using (var connection = await pg.DataSource!.OpenConnectionAsync(Ct))
        {
            await PostgresRelayStore.EnqueueInTransactionAsync(connection, null, Msg("collect", "from the first"), first, Ct);
            await PostgresRelayStore.EnqueueInTransactionAsync(connection, null, Msg("collect", "from the second"), second, Ct);
        }

        Assert.Equal(2, (await sp.GetRequiredService<IRelayAdmin>().GetStatsAsync(Ct)).Pending);
        Assert.Equal(2, await sp.GetRequiredService<IRelayProcessor>().ProcessOnceAsync(Ct));
        Assert.Equal(["from the first", "from the second"], delivered.Order());
        Assert.Equal(0, (await sp.GetRequiredService<IRelayAdmin>().GetStatsAsync(Ct)).Pending);

        // Without a transaction, a message goes to the first store.
        await sp.GetRequiredService<IRelayOutbox>().EnqueueAsync(new RelayEnvelope { Destination = "collect", Payload = "plain"u8.ToArray() }, Ct);
        Assert.Equal(1, (await new PostgresRelayStore(pg.DataSource!, first).GetStatsAsync(Ct)).Pending);
    }
}
