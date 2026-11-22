# API управления v1

Control API хранит сценарии и состояние запусков в собственной базе PostgreSQL. Перед созданием запуска зарегистрируйте доступного агента:

```powershell
$agent = [guid]::NewGuid()
Invoke-RestMethod http://localhost:8080/agents/$agent/ready -Method Post
$scenario = Invoke-RestMethod http://localhost:8080/scenarios -Method Post -ContentType 'application/json' -Body (@{
  name='demo'; method='GET'; url='http://demo-target:8080/'; headers=@{};
  targetRps=10; durationSeconds=30; requestTimeoutMs=1000; concurrencyLimit=10
} | ConvertTo-Json)
Invoke-RestMethod http://localhost:8080/runs -Method Post -ContentType 'application/json' -Body (@{ scenarioId=$scenario.id } | ConvertTo-Json)
```

URL сценария должен быть абсолютным адресом HTTP или HTTPS. На входе API проверяются допустимые значения RPS, длительности, таймаута запроса и предела одновременных запросов. Запуск хранит неизменяемый JSON-снимок своего сценария. Одновременно может существовать только один запуск в состоянии `pending`, `running` или `stopping`; это ограничение обеспечивает PostgreSQL. Метод `POST /runs/{id}/stop` идемпотентен и переводит активный запуск в состояние `stopping`.

На текущем этапе доступен вспомогательный эндпоинт регистрации агента для локального управления запусками. Выдача назначений агентам по gRPC и генерация нагрузки будут реализованы на следующих этапах.
