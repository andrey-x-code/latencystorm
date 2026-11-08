# Инфраструктура MVP v1

Этап 2 добавляет четыре приложения и локальное окружение в [Compose](../deploy/compose/compose.yaml): PostgreSQL 18 с отдельными базами control/results, Kafka 4.1 KRaft, Prometheus 3.16, Control API, Results Service, Load Agent и Demo Target.

## Запуск

Для проверочных скриптов требуется Python 3.9+ без сторонних пакетов.

~~~powershell
Copy-Item deploy/compose/.env.example deploy/compose/.env
# Set three independent local passwords in .env before the first start.
docker compose --env-file deploy/compose/.env -f deploy/compose/compose.yaml up --build -d --wait --wait-timeout 180
python tests/infrastructure/smoke.py --env-file deploy/compose/.env
~~~

Приложения публикуют JSON-логи, /health, /ready и /metrics. C# readiness означает успешные миграции и probes PostgreSQL/Kafka; Load Agent дополнительно проверяет Control API. Проверки повторяются каждые 3 секунды и не завершают процесс при временном отказе. SIGTERM запускает graceful shutdown до 10 секунд.

Миграции выполняются транзакционно и записываются в schema_migrations с SHA-256. Изменение уже применённого SQL блокирует readiness. Control API и Results Service используют отдельные роли и базы; доменные таблицы появятся на этапе 3.

Demo Target принимает DEMO_DELAY_MS (0..300000), DEMO_STATUS_CODE (200..599) и DEMO_ERROR_RATE (0..1). Параметры передаются через окружение и проверяются при старте.

Адреса: Control API — http://localhost:8080, Results Service — http://localhost:8081, Load Agent — http://localhost:8082, Demo Target — http://localhost:8083, Prometheus — http://localhost:9090. Порты опубликованы только на loopback; PostgreSQL и Kafka доступны внутри Compose-сети.

Пароли в примере — заглушки для локального окружения. Используйте независимые случайные буквенно-цифровые значения: пароль включается в строку подключения Npgsql. Инициализация ролей и баз выполняется только при первом запуске с пустым томом PostgreSQL. Изменение пароля в .env не обновляет существующие роли.

PostgreSQL и Prometheus используют именованные тома. Kafka хранит журнал в файловой системе контейнера: stop/start сохраняет его, пересоздание контейнера удаляет. Это ограничение локального каркаса этапа 2; отчёты и история должны храниться в PostgreSQL. Для остановки без удаления данных выполните:

~~~powershell
docker compose --env-file deploy/compose/.env -f deploy/compose/compose.yaml down
~~~

Миграции добавляйте новым SQL-файлом с возрастающим номером в каталоге migrations соответствующего сервиса. Не изменяйте применённый файл. Перед повторной сборкой выполняйте проверку:

~~~powershell
docker build -f tests/infrastructure/Dockerfile.verify --target export --output type=local,dest=.artifacts/infrastructure .
~~~

Проверка собирает C#-проекты с зафиксированными зависимостями, проверяет форматирование C# и Go без изменения исходников, выполняет go mod tidy -diff, go vet и go test -race. Проверенные исходники и результаты сборки экспортируются в .artifacts. Smoke-test временно останавливает Kafka, проверяет 503 и восстановление readiness; выполняйте его в локальном тестовом окружении.
