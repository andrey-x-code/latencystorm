# Контракты MVP v1

- [model.proto](proto/latencystorm/v1/model.proto) — сущности, snapshot и состояния.
- [control.proto](proto/latencystorm/v1/control.proto) — двунаправленный AgentControl и внутренний ResultsIngestion для манифеста/терминальных метаданных.
- [results.proto](proto/latencystorm/v1/results.proto) — Kafka ResultBatch в формате ProtoJSON.
- [Протокол](../docs/protocol-v1.md), [измерения](../docs/measurements-v1.md), [версии](../docs/toolchain.md).
- [Пример Kafka-пакета](events/examples/result-batch.v1.json).

## Проверка и генерация

Из корня репозитория, PowerShell и Docker Desktop с Linux containers:

~~~powershell
./scripts/verify-contracts.ps1
~~~

Скрипт использует точные версии из toolchain.env, собирает C#-контракты и gRPC stubs, генерирует Go-типы и stubs, проверяет форматирование, go vet и go test. C# читает пример пакета, Go проверяет семантическое равенство C#-вывода исходному примеру и пишет свой JSON, затем C# читает его обратно. Дополнительно проверяются бинарная команда Start, snapshot/манифест, большие uint64, enum и строгий JSON-парсинг. Это проверки контрактов, не работающих сервисов или Kafka.

Артефакты выходят в .artifacts/contracts: Go gen, go.mod/go.sum, .NET lockfiles. Сгенерированные Go-файлы хранятся в contracts/go/gen; C# генерируется Grpc.Tools во время сборки в obj и не коммитится. Проверка сравнивает Go-генерацию с сохранённой версией и не меняет исходники.

При изменении .proto или зависимостей:

1. Обновите схему и связанные документы/примеры. Для SDK синхронно измените global.json, go.mod и toolchain.env; в Dockerfile значения ARG служат теми же значениями по умолчанию для прямого запуска.
2. Запустите ./scripts/verify-contracts.ps1 -UpdateGenerated. Скрипт разрешает обновить lockfiles, пересоздаёт Go-типы и копирует результаты в исходные каталоги.
3. Проверьте diff, затем выполните ./scripts/verify-contracts.ps1 без флага. В этом режиме .NET restore работает с --locked-mode, Go — с -mod=readonly, и проверяется отсутствие расхождений в генерации.

Локальная сборка C# при наличии SDK 10.0.403:

~~~powershell
dotnet restore tests/contracts/csharp/ContractCheck.csproj --locked-mode
dotnet build tests/contracts/csharp/ContractCheck.csproj --no-restore
dotnet run --no-build --project tests/contracts/csharp/ContractCheck.csproj -- contracts/events/examples/result-batch.v1.json .artifacts/csharp.json
~~~

Перед локальным запуском создайте .artifacts. Go-модуль содержит только типы; кросс-языковые тесты находятся в tests/contracts/go и временно копируются в модуль контейнерной проверкой. Версии протокола нельзя менять только в одном языке. Бизнес-валидация полей, идемпотентность, state machines и хранение реализуются на соответствующих следующих этапах.
