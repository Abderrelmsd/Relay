using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace Relay;

/// <summary>Postgres store. Claiming uses <c>FOR UPDATE SKIP LOCKED</c> so many workers/instances share one table safely; per-key ordering is enforced in the claim query.</summary>
public sealed class PostgresRelayStore(NpgsqlDataSource dataSource) : IRelayStore
{
    public const string CreateSql = """
        CREATE TABLE IF NOT EXISTS relay_messages (
          sequence      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          id            uuid        NOT NULL UNIQUE,
          destination   text        NOT NULL,
          ordering_key  text        NULL,
          payload       bytea       NOT NULL,
          headers       jsonb       NOT NULL DEFAULT '{}'::jsonb,
          created_at    timestamptz NOT NULL,
          available_at  timestamptz NOT NULL,
          attempts      integer     NOT NULL DEFAULT 0,
          status        smallint    NOT NULL DEFAULT 0,   -- 0 pending, 1 in flight, 2 dead-lettered
          claimed_by    text        NULL,
          claimed_until timestamptz NULL,
          tenant_id     text        NULL,
          attempt_log   jsonb       NULL                  -- kept only while the message is retried/dead-lettered; rows are deleted on success
        );
        CREATE INDEX IF NOT EXISTS ix_relay_due ON relay_messages (status, available_at);
        CREATE INDEX IF NOT EXISTS ix_relay_key ON relay_messages (destination, ordering_key, sequence) WHERE ordering_key IS NOT NULL;
        """;

    private const string Columns = "sequence, id, destination, ordering_key, payload, headers, created_at, available_at, attempts, status, claimed_by, claimed_until, tenant_id, attempt_log";
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public async Task EnsureSchemaAsync(CancellationToken ct = default)
    {
        await using var cmd = dataSource.CreateCommand(CreateSql);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<bool> EnqueueAsync(RelayMessage m, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        return await InsertAsync(conn, null, m, ct);
    }

    /// <summary>Enqueues on the caller's connection and transaction, so the message commits or rolls back with the business change (transactional outbox).</summary>
    public Task<bool> EnqueueInTransactionAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, RelayMessage message, CancellationToken ct = default)
        => InsertAsync(connection, transaction, message, ct);

    private static async Task<bool> InsertAsync(NpgsqlConnection conn, NpgsqlTransaction? tx, RelayMessage m, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO relay_messages (id, destination, ordering_key, payload, headers, created_at, available_at, tenant_id)
            VALUES (@id, @dest, @key, @payload, @headers, @created, @avail, @tenant)
            ON CONFLICT (id) DO NOTHING
            """, conn, tx);
        cmd.Parameters.AddWithValue("id", m.Id);
        cmd.Parameters.AddWithValue("dest", m.Destination);
        cmd.Parameters.AddWithValue("key", (object?)m.OrderingKey ?? DBNull.Value);
        cmd.Parameters.AddWithValue("payload", m.Payload);
        cmd.Parameters.Add(new NpgsqlParameter("headers", NpgsqlDbType.Jsonb) { Value = JsonSerializer.Serialize(m.Headers, Json) });
        cmd.Parameters.AddWithValue("created", m.CreatedAt);
        cmd.Parameters.AddWithValue("avail", m.AvailableAt);
        cmd.Parameters.AddWithValue("tenant", (object?)m.TenantId ?? DBNull.Value);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task<IReadOnlyList<RelayMessage>> ClaimAsync(string workerId, int max, TimeSpan lease, DateTimeOffset now, IReadOnlySet<string> excluded, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand($"""
            WITH candidates AS (
              SELECT m.sequence FROM relay_messages m
              WHERE ((m.status = 0 AND m.available_at <= @now) OR (m.status = 1 AND m.claimed_until <= @now))
                AND NOT (m.destination = ANY(@excluded))
                AND (m.ordering_key IS NULL OR NOT EXISTS (
                      SELECT 1 FROM relay_messages o
                      WHERE o.destination = m.destination AND o.ordering_key = m.ordering_key
                        AND o.sequence < m.sequence AND o.status IN (0, 1)))
              ORDER BY m.sequence
              LIMIT @max
              FOR UPDATE SKIP LOCKED)
            UPDATE relay_messages r
               SET status = 1, claimed_by = @worker, claimed_until = @until, attempts = r.attempts + 1
              FROM candidates c
             WHERE r.sequence = c.sequence
            RETURNING r.sequence, r.id, r.destination, r.ordering_key, r.payload, r.headers, r.created_at, r.available_at, r.attempts, r.status, r.claimed_by, r.claimed_until, r.tenant_id, r.attempt_log
            """);
        cmd.Parameters.AddWithValue("now", now);
        cmd.Parameters.AddWithValue("excluded", excluded.ToArray());
        cmd.Parameters.AddWithValue("max", max);
        cmd.Parameters.AddWithValue("worker", workerId);
        cmd.Parameters.AddWithValue("until", now + lease);
        return [.. (await ReadAsync(cmd, ct)).OrderBy(m => m.Sequence)];
    }

    public Task<bool> CompleteAsync(Guid id, string workerId, CancellationToken ct)
        => ExecAsync("DELETE FROM relay_messages WHERE id = @id AND status = 1 AND claimed_by = @w", id, workerId, ct);

    public Task<bool> RetryLaterAsync(Guid id, string workerId, DateTimeOffset availableAt, AttemptRecord attempt, CancellationToken ct)
        => ExecAsync("""
            UPDATE relay_messages SET status = 0, available_at = @at, claimed_by = NULL, claimed_until = NULL,
                   attempt_log = COALESCE(attempt_log, '[]'::jsonb) || @attempt
             WHERE id = @id AND status = 1 AND claimed_by = @w
            """, id, workerId, ct, c => { c.Parameters.AddWithValue("at", availableAt); AddAttempt(c, attempt); });

    public Task<bool> DeadLetterAsync(Guid id, string workerId, AttemptRecord attempt, CancellationToken ct)
        => ExecAsync("""
            UPDATE relay_messages SET status = 2, claimed_by = NULL, claimed_until = NULL,
                   attempt_log = COALESCE(attempt_log, '[]'::jsonb) || @attempt
             WHERE id = @id AND status = 1 AND claimed_by = @w
            """, id, workerId, ct, c => AddAttempt(c, attempt));

    public Task<bool> ReleaseAsync(Guid id, string workerId, CancellationToken ct)
        => ExecAsync("UPDATE relay_messages SET status = 0, claimed_by = NULL, claimed_until = NULL, attempts = GREATEST(attempts - 1, 0) WHERE id = @id AND status = 1 AND claimed_by = @w", id, workerId, ct);

    public async Task<IReadOnlyList<RelayMessage>> ListDeadLettersAsync(string? destination, int skip, int take, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand($"SELECT {Columns} FROM relay_messages WHERE status = 2 AND (@dest::text IS NULL OR destination = @dest) ORDER BY sequence OFFSET @skip LIMIT @take");
        cmd.Parameters.AddWithValue("dest", (object?)destination ?? DBNull.Value);
        cmd.Parameters.AddWithValue("skip", skip);
        cmd.Parameters.AddWithValue("take", take);
        return await ReadAsync(cmd, ct);
    }

    public async Task<bool> RequeueDeadLetterAsync(Guid id, DateTimeOffset now, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand("UPDATE relay_messages SET status = 0, attempts = 0, available_at = @now, attempt_log = NULL WHERE id = @id AND status = 2");
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("now", now);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task<bool> PurgeDeadLetterAsync(Guid id, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand("DELETE FROM relay_messages WHERE id = @id AND status = 2");
        cmd.Parameters.AddWithValue("id", id);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task<RelayStats> GetStatsAsync(CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand("""
            SELECT count(*) FILTER (WHERE status = 0), count(*) FILTER (WHERE status = 1), count(*) FILTER (WHERE status = 2),
                   min(created_at) FILTER (WHERE status = 0) FROM relay_messages
            """);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        await r.ReadAsync(ct);
        return new RelayStats(r.GetInt64(0), r.GetInt64(1), r.GetInt64(2), r.IsDBNull(3) ? null : r.GetFieldValue<DateTimeOffset>(3));
    }

    private async Task<bool> ExecAsync(string sql, Guid id, string worker, CancellationToken ct, Action<NpgsqlCommand>? more = null)
    {
        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("w", worker);
        more?.Invoke(cmd);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    private static void AddAttempt(NpgsqlCommand c, AttemptRecord a)
        => c.Parameters.Add(new NpgsqlParameter("attempt", NpgsqlDbType.Jsonb) { Value = JsonSerializer.Serialize(new[] { a }, Json) });

    private static async Task<List<RelayMessage>> ReadAsync(NpgsqlCommand cmd, CancellationToken ct)
    {
        var list = new List<RelayMessage>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            var log = r.IsDBNull(13) ? null : JsonSerializer.Deserialize<List<AttemptRecord>>(r.GetString(13), Json);
            list.Add(new RelayMessage(
                Id: r.GetGuid(1), Destination: r.GetString(2), OrderingKey: r.IsDBNull(3) ? null : r.GetString(3), Payload: r.GetFieldValue<byte[]>(4),
                Headers: JsonSerializer.Deserialize<Dictionary<string, string>>(r.GetString(5), Json) ?? [], CreatedAt: r.GetFieldValue<DateTimeOffset>(6),
                AvailableAt: r.GetFieldValue<DateTimeOffset>(7), Attempts: r.GetInt32(8), Status: (RelayStatus)r.GetInt16(9), ClaimedBy: r.IsDBNull(10) ? null : r.GetString(10),
                ClaimedUntil: r.IsDBNull(11) ? null : r.GetFieldValue<DateTimeOffset>(11), TenantId: r.IsDBNull(12) ? null : r.GetString(12), Sequence: r.GetInt64(0), AttemptLog: log));
        }
        return list;
    }
}
