using LatencyStorm.ServiceDefaults;

await ServiceHost.RunAsync(args, "results-service", app =>
{
    app.MapGrpcService<ManifestService>();
    app.MapGet("/runs/{id:guid}/report", ReportApi.ReportAsync);
    app.MapGet("/runs/{id:guid}/timeline", ReportApi.TimelineAsync);
}, services =>
{
    services.AddGrpc();
    services.AddSingleton<ReportStore>();
    services.AddHostedService<ResultConsumer>();
});
