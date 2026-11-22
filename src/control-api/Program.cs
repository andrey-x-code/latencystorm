using LatencyStorm.ServiceDefaults;

await ServiceHost.RunAsync(args, "control-api", app =>
{
    app.MapPost("/scenarios", (ScenarioRequest request, ControlApiService service, CancellationToken ct) =>
        service.CreateScenarioAsync(request, ct)).WithName("CreateScenario");

    app.MapGet("/scenarios", (ControlApiService service, CancellationToken ct) =>
        service.ListScenariosAsync(ct));

    app.MapGet("/scenarios/{id:guid}", (Guid id, ControlApiService service, CancellationToken ct) =>
        service.GetScenarioAsync(id, ct));

    app.MapPost("/runs", (RunRequest request, ControlApiService service, CancellationToken ct) =>
        service.CreateRunAsync(request, ct));

    app.MapPost("/agents/{id:guid}/ready", (Guid id, ControlApiService service, CancellationToken ct) =>
        service.RegisterAgentAsync(id, ct));

    app.MapGet("/runs", (ControlApiService service, CancellationToken ct) =>
        service.ListRunsAsync(ct));

    app.MapGet("/runs/{id:guid}", (Guid id, ControlApiService service, CancellationToken ct) =>
        service.GetRunAsync(id, ct));

    app.MapPost("/runs/{id:guid}/stop", (Guid id, ControlApiService service, CancellationToken ct) =>
        service.StopRunAsync(id, ct));

    app.MapPost("/runs/{id:guid}/complete", (Guid id, ControlApiService service, CancellationToken ct) =>
        service.CompleteRunAsync(id, ct));

    app.MapGet("/openapi.json", (ControlApiService service) => service.GetOpenApi());
}, services => services.AddScoped<ControlApiService>());
