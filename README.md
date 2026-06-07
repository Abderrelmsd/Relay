# Relay

A persistent outbox for reliable asynchronous delivery. You enqueue a message; Relay stores it durably and delivers it to a named destination, with retries, backoff, circuit breaking, dead-lettering and per-key ordering.

**Wolverine** performs the in-transaction hand-off when you use it; **Relay owns everything after that**: claiming, retrying, breaking, dead-lettering and ordering. Payloads are opaque bytes, and Relay never looks inside them.

Use it for anything that must survive crashes and outages, such as emails, webhooks and calls to flaky systems.

## Install

```bash
dotnet add package Relay
```

## Quick start

```csharp
services.AddRelay(o => o.MaxAttempts = 8);
services.AddRelayPostgres(dataSource);                     // or the in-memory default for development and tests
services.AddRelayDestination("email", async (msg, ct) => await SendAsync(msg.Payload, ct));
services.AddRelayDispatcher();                             // the background delivery loop

// Producer: durable, and idempotent by MessageId
await outbox.EnqueueAsync(new RelayEnvelope { Destination = "email", Payload = bytes, OrderingKey = "user-42" });

// Or through Wolverine, so the hand-off commits with your business transaction:
builder.UseWolverine(opts => opts.IncludeRelay());        // also reference WolverineFx.RuntimeCompilation, or pre-generate handler code
await bus.PublishAsync(new EnqueueRelayMessage("email", bytes, OrderingKey: "user-42"));
```

## Delivery guarantees

Delivery is **at-least-once**, so destinations must be idempotent. `message.Id` is stable across attempts; use it to de-duplicate.

Claims carry a lease (2 minutes by default). If a worker crashes, its messages are re-claimed after the lease expires. Completing, retrying or dead-lettering is fenced by the worker id, so a slow, stale worker cannot overwrite the result of the worker that took over.

## What a destination can do when it fails

| In the handler | Result |
|---|---|
| Throw any exception | Transient: retry with exponential backoff and jitter, capped by `MaxDelay` |
| Throw `RelayPermanentFailureException` | Dead-letter immediately |
| Throw `RelayRetryAfterException(delay)` | Retry at the time you choose |

## Ordering

Messages with the same `(Destination, OrderingKey)` are delivered strictly in the order they were enqueued, one at a time. A failing message at the head **blocks its key** until it succeeds or is dead-lettered; dead-lettering it unblocks the key. Keys are scoped per destination.

## Circuit breaker

Relay tracks consecutive transient failures per destination name (per process). At `CircuitFailureThreshold` it stops claiming that destination for `CircuitBreakDuration`, releases blocked messages without counting an attempt, and then lets one probe through. Permanent failures do not count against the destination.

A destination can serve a whole family of names through `Handles(name)`, for example `webhook:{endpointId}`, so each name gets its own breaker and ordering scope.

## Dead letters

Failed attempts are appended to an attempt log while a message is retried; successful messages are deleted and leave nothing behind. Dead letters keep their attempt log. `IRelayAdmin` lists, requeues and purges them.

## Transactional enqueue

On Postgres, enqueue inside your own transaction so the message commits with your data:
`PostgresRelayStore.EnqueueInTransactionAsync(connection, transaction, message)`.

## Stores

`IRelayStore` is the contract, and the in-memory store is its reference implementation. The Postgres store implements the same rules in one SQL statement using `FOR UPDATE SKIP LOCKED`. The Postgres integration tests need Docker and are skipped without it.

## Configuration

Section `Relay`.

| Option | Default | Meaning |
|---|---|---|
| `MaxAttempts` | `8` | Attempts before a message is dead-lettered |
| `BaseDelay` / `MaxDelay` | `5 s` / `1 h` | Backoff bounds |
| `LeaseDuration` | `2 min` | How long a claim lasts before another worker may take it |
| `BatchSize` | `50` | Messages claimed per poll |
| `MaxConcurrency` | `8` | Simultaneous deliveries |
| `PollInterval` | `1 s` | Wait when there is nothing to deliver |
| `CircuitFailureThreshold` | `5` | Consecutive failures that open a destination's circuit |
| `CircuitBreakDuration` | `1 min` | How long the circuit stays open |
| `WorkerId` | generated | Identifies this worker for fencing |

## Depends on

Nothing else from this set of packages. `TenantId` on a message is an optional plain string; hosts add tenancy as they need it.
