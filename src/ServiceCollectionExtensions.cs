using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;

namespace Relay;

internal sealed class DelegateDestination(string name, Func<RelayMessage, CancellationToken, Task> deliver) : IRelayDestination
{
    public string Name => name;
    public Task DeliverAsync(RelayMessage message, CancellationToken cancellationToken) => deliver(message, cancellationToken);
}

public static class ServiceCollectionExtensions
{
    /// <summary>Registers the outbox, processor and admin API (in-memory store unless another <see cref="IRelayStore"/> is registered first). Call <see cref="AddRelayDispatcher"/> to run delivery in the background.</summary>
    public static IServiceCollection AddRelay(this IServiceCollection services, Action<RelayOptions>? configure = null)
    {
        var o = services.AddOptions<RelayOptions>();
        if (configure is not null) o.Configure(configure);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IRelayStore, InMemoryRelayStore>();
        services.TryAddSingleton<DestinationBreakers>();
        services.TryAddSingleton<IRelayOutbox, RelayOutbox>();
        services.TryAddSingleton<IRelayProcessor, RelayProcessor>();
        services.TryAddSingleton<IRelayAdmin, RelayAdmin>();
        return services;
    }

    /// <summary>Runs the delivery loop as a hosted service.</summary>
    public static IServiceCollection AddRelayDispatcher(this IServiceCollection services)
        => services.AddHostedService<RelayDispatcher>();

    /// <summary>Uses Postgres. Call before <see cref="AddRelay"/>. Run <see cref="PostgresRelayStore.EnsureSchemaAsync"/> (or apply <see cref="PostgresRelayStore.CreateSql"/> in a migration) once.</summary>
    public static IServiceCollection AddRelayPostgres(this IServiceCollection services, NpgsqlDataSource dataSource)
    {
        services.RemoveAll<IRelayStore>();
        services.AddSingleton(new PostgresRelayStore(dataSource));
        services.AddSingleton<IRelayStore>(sp => sp.GetRequiredService<PostgresRelayStore>());
        return services;
    }

    public static IServiceCollection AddRelayDestination<T>(this IServiceCollection services) where T : class, IRelayDestination
        => services.AddSingleton<IRelayDestination, T>();

    public static IServiceCollection AddRelayDestination(this IServiceCollection services, string name, Func<RelayMessage, CancellationToken, Task> deliver)
        => services.AddSingleton<IRelayDestination>(new DelegateDestination(name, deliver));
}
