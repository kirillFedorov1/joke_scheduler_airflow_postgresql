# Airflow с PostgreSQL

Этот репозиторий предоставляет инфраструктуру на базе Docker для создания DAG в Apache Airflow и хранения их в PostgreSQL.

## Структура Контейнеров

- **Docker Compose**: Файл `docker-compose.yaml` для развертывания всех нужных контейнеров.
- **PostgreSQL**: Отдельный контейнер `data_postgres-1` для базы данных.

## Конфигурация

- Кастомный файл конфигурации `airflow.cfg` для настройки Apache Airflow.

## 1. joke_json_to_postgresql.py

Парсинг шутки.

### Характеристики

- **Расписание**: Каждый час
- **Источник данных**: `https://official-joke-api.appspot.com/jokes/random`
- **Таблица с результатами**: `airflow_studies.jokes`
- **Обработка Конфликтов**: Учет возможных конфликтов по ключу `id` из api

## 2. randomuser_to_postgresql.py

Генерация личности.

#### Характеристики
- **Расписание**: Каждые 5 минут
- **Источник данных**: `https://randomuser.me/api/`
- **Таблицы с результатами**: `randomuser.user`, `randomuser.location`, `randomuser.login`
- **Обработка Конфликтов**: Учет возможных конфликтов по ключу `uuid` из api
