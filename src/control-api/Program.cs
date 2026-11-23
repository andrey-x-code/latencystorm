using LatencyStorm.ServiceDefaults;

using Grpc.Net.Client;
using LatencyStorm.Contracts.V1;

await ServiceHost.RunAsync(args, "control-api", app =>
{
    app.MapGrpcService<AgentConnection>();
    app.MapPost("/scenarios", (ScenarioRequest request, ControlApiService service, CancellationToken ct) =>
        service.CreateScenarioAsync(request, ct)).WithName("CreateScenario");

    app.MapGet("/scenarios", (ControlApiService service, CancellationToken ct) =>
        service.ListScenariosAsync(ct));

    app.MapGet("/scenarios/{id:guid}", (Guid id, ControlApiService service, CancellationToken ct) =>
        service.GetScenarioAsync(id, ct));

    app.MapPost("/runs", (RunRequest request, ControlApiService service, CancellationToken ct) =>
        service.CreateRunAsync(request, ct));

    app.MapGet("/runs", (ControlApiService service, CancellationToken ct) =>
        service.ListRunsAsync(ct));

    app.MapGet("/runs/{id:guid}", (Guid id, ControlApiService service, CancellationToken ct) =>
        service.GetRunAsync(id, ct));

    app.MapPost("/runs/{id:guid}/stop", (Guid id, ControlApiService service, CancellationToken ct) =>
        service.StopRunAsync(id, ct));

    app.MapGet("/openapi.json", (ControlApiService service) => service.GetOpenApi());
}, services =>
{
    services.AddGrpc();
    services.AddScoped<ControlApiService>();
    services.AddSingleton<AgentSlot>();
    services.AddSingleton(provider => GrpcChannel.ForAddress(
        provider.GetRequiredService<IConfiguration>()["RESULTS_GRPC_URL"] ?? "http://localhost:8081"));
    services.AddSingleton(provider => new ResultsIngestion.ResultsIngestionClient(provider.GetRequiredService<GrpcChannel>()));
});
