# Airflow Joke Scheduler с PostgreSQL

Этот репозиторий предоставляет инфраструктуру на базе Docker для автоматического сбора шуток с использованием Apache Airflow и хранения их в PostgreSQL.

## Структура Контейнеров

- **Docker Compose**: Файл `docker-compose.yaml` для развертывания всех нужных контейнеров.
- **PostgreSQL**: Отдельный контейнер `data_postgres-1` для базы данных.

## Конфигурация

- Кастомный файл конфигурации `airflow.cfg` для настройки Apache Airflow.

## 1. joke_json_to_postgresql.py

Генерация шутки.

### Характеристики

- **Расписание**: Каждый час
- **Источник данных**: `https://official-joke-api.appspot.com/jokes/random`
- **Таблица с результатами**: `airflow_studies.jokes`
- **Обработка Конфликтов**: Учет возможных конфликтов по ключу в PostgreSQL.
