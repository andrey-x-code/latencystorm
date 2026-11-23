# API управления v1

Control API хранит сценарии и состояние запусков в собственной базе PostgreSQL. Запустите стенд и дождитесь готовности Load Agent: он регистрируется автоматически по gRPC.

```powershell
$scenario = Invoke-RestMethod http://localhost:8080/scenarios -Method Post -ContentType 'application/json' -Body (@{
  name='demo'; method='GET'; url='http://demo-target:8080/'; headers=@{};
  targetRps=10; durationSeconds=30; requestTimeoutMs=1000; concurrencyLimit=10
} | ConvertTo-Json)
$run = Invoke-RestMethod http://localhost:8080/runs -Method Post -ContentType 'application/json' -Body (@{ scenarioId=$scenario.id } | ConvertTo-Json)
Invoke-RestMethod "http://localhost:8080/runs/$($run.id)"
Invoke-RestMethod "http://localhost:8080/runs/$($run.id)/stop" -Method Post
```

URL сценария должен быть абсолютным адресом HTTP или HTTPS. На входе API проверяются допустимые значения RPS, длительности, таймаута запроса и предела одновременных запросов. Запуск хранит неизменяемый JSON-снимок своего сценария. Одновременно может существовать только один запуск в состоянии `pending`, `running` или `stopping`; это ограничение обеспечивает PostgreSQL. Метод `POST /runs/{id}/stop` идемпотентен и переводит активный запуск в состояние `stopping`.

`POST /runs` возвращает `201` после резервирования агента или `409`, если агент не подключён либо уже есть активный запуск. Статус `running` появляется после подтверждения старта агентом. Остановка завершается в `cancelled`, естественный конец — в `completed`, сбой управления — в `failed`. Вспомогательные маршруты ручной регистрации и завершения больше не используются. См. [сквозную проверку](stage-05-v1.md).
