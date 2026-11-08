FROM mcr.microsoft.com/dotnet/sdk:10.0.403 AS build
WORKDIR /repo
COPY global.json ./
COPY src/service-defaults/ src/service-defaults/
COPY src/control-api/ src/control-api/
RUN dotnet restore src/control-api/LatencyStorm.ControlApi.csproj --locked-mode
RUN dotnet publish src/control-api/LatencyStorm.ControlApi.csproj -c Release --no-restore -o /out
FROM mcr.microsoft.com/dotnet/aspnet:10.0.14
WORKDIR /app
COPY --from=build /out .
ENV ASPNETCORE_URLS=http://+:8080
USER $APP_UID
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=4s --start-period=20s --retries=12 CMD ["dotnet", "LatencyStorm.ControlApi.dll", "--healthcheck"]
ENTRYPOINT ["dotnet", "LatencyStorm.ControlApi.dll"]
