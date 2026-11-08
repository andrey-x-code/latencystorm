# Зафиксированные версии

Версии выбраны для локального MVP. .NET 10 — LTS; Go 1.26 — поддерживаемая ветка. Версии сервисов обновляются отдельно после проверки совместимости, без плавающих тегов.

| Компонент | Версия | Где закреплена |
| --- | --- | --- |
| .NET SDK | 10.0.403 | global.json; contracts/toolchain.env |
| ASP.NET Core runtime | 10.0.14 | contracts/toolchain.env |
| Go | 1.26.10 | contracts/go/go.mod (toolchain); contracts/toolchain.env |
| PostgreSQL | 18.1 | postgres:18.1-bookworm |
| Kafka (KRaft) | 4.1.0 | apache/kafka:4.1.0 |
| Prometheus | 3.16.0 | prom/prometheus:v3.16.0 |
| Google.Protobuf / protoc-gen-go | 3.31.1 / 1.36.6 | csproj / toolchain.env |
| Grpc.Tools (включая protoc и C# plugin) | 2.71.0 | csproj |
| Grpc.Core.Api | 2.71.0 | csproj |
| Go gRPC runtime / generator | 1.72.1 / 1.5.1 | go.mod / toolchain.env |

Полные имена образов находятся в [toolchain.env](../contracts/toolchain.env); этап 2 должен использовать те же версии. Собственные образы приложений на этом этапе не создаются.

.NET запрещает автоматический переход на другой SDK через rollForward=disable. Go module задаёт минимальную языковую версию 1.26.0 и рекомендуемый toolchain 1.26.10; проверка в Docker выполняется ровно на 1.26.10 с GOTOOLCHAIN=local. При локальной разработке проверяйте go version: директива toolchain сама по себе не запрещает более новый установленный Go.

Прямые зависимости имеют точные версии. Транзитивные зависимости .NET закреплены packages.lock.json, Go — go.mod/go.sum. Не редактируйте lockfiles вручную. При обновлении восстановите зависимости, проверьте diff и запустите scripts/verify-contracts.ps1. Grpc.Tools поставляет protoc и standard includes, поэтому отдельная установка компилятора protobuf не нужна.

Проверка контрактов требует Docker с Linux containers и доступа к NuGet, Go modules и registry при первом запуске. SDK на Windows-хосте не требуются. PostgreSQL, Kafka и Prometheus проверяются как часть окружения на этапе 2; проверка контрактов их не запускает.
