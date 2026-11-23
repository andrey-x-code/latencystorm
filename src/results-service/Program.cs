using LatencyStorm.ServiceDefaults;

await ServiceHost.RunAsync(args, "results-service", app => app.MapGrpcService<ManifestService>(), services =>
{
    services.AddGrpc();
    services.AddHostedService<ResultConsumer>();
});
