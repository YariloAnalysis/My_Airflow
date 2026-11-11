# Airflow: официальный Compose без custom build

Для локального обучения. Не публиковать в Интернет: локальные пароли airflow/airflow.
Нет Dockerfile, uv или установки пакетов при старте. uv можно использовать отдельно для IDE.

## PowerShell: из новой папки проекта

```powershell
New-Item -ItemType Directory -Force dags,logs,plugins,config | Out-Null
Copy-Item .env.example .env
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$key = New-Object byte[] 32
$rng.GetBytes($key)
$fernet = [Convert]::ToBase64String($key).Replace('+','-').Replace('/','_')
$rng.GetBytes($key)
$jwt = [Convert]::ToBase64String($key)
$rng.Dispose()
(Get-Content .env) -replace '^FERNET_KEY=.*', "FERNET_KEY=$fernet" -replace '^AIRFLOW__API_AUTH__JWT_SECRET=.*', "AIRFLOW__API_AUTH__JWT_SECRET=$jwt" | Set-Content -Encoding ascii .env
docker compose config --quiet
docker compose pull
docker compose up airflow-init
docker compose up -d
```

Генерируйте ключи только при первом создании .env, не при каждом запуске.
UI: http://localhost:8080, airflow / airflow.
Успешный init завершается с кодом 0.

## Проверка интеграций с PostgreSQL / MinIO

```powershell
docker compose run --rm --no-deps airflow-worker python -c "from airflow.providers.postgres.hooks.postgres import PostgresHook; from airflow.providers.amazon.aws.hooks.s3 import S3Hook; print('OK')"
```

Это проверка импортов, не соединения с вашими сервисами. Удаленные креды добавляйте
отдельно в Airflow Connections. SSH не нужен. Если импорта нет, не запускайте
повторную установку пакетов на каждом старте: понадобится отдельно согласованный образ.

## Очистка СТАРОГО проекта

До замены старого compose из его папки:

```powershell
docker system df
docker compose down --volumes --remove-orphans
docker image rm airflow-celery-uv:3.4.0
```

ВНИМАНИЕ: down --volumes удаляет служебную БД старого проекта и ее данные.
Удаленная учебная БД/MinIO этими командами не удаляются.
Если образ не найден, он, вероятно, не был собран.
Не применяйте system prune --all --volumes: это затронет другие проекты.

Опционально docker builder prune удаляет неиспользуемый кеш сборки во всем Docker,
не только этого проекта; просмотрите предупреждение перед подтверждением.
Образы нового Airflow сами занимают значительное место: Compose не делает их меньше.
На Docker Desktop удаление данных внутри VM не гарантирует немедленное уменьшение
файла виртуального диска на Windows.

Не удаляйте DAG или креды без резервной копии. Старые Dockerfile и SSH DAG здесь
не используются. Служебный Postgres в Compose не заменяйте учебной базой.

Основа: официальный docker-compose.yaml Airflow 3.4.0, сохранена Apache license header.
Изменения: отключены example DAGs, UI привязан к loopback.
Проверен YAML и отсутствие активных build/runtime pip options. Docker end-to-end не тестировался.
