import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json


class DatabaseConnector:
    """
    Класс для работы с базой данных проекта
    Поддерживает SQLite, с возможностью расширения до PostgreSQL
    """

    def __init__(self, db_path='server_metrics.db'):
        self.db_path = db_path
        self.connection = None
        self.connect()
        self.create_tables()

    def connect(self):
        """Установка соединения с базой данных"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
            print(f"Успешное подключение к базе данных: {self.db_path}")
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {e}")

    def create_tables(self):
        """Создание таблиц для метрик серверов и логов"""
        try:
            cursor = self.connection.cursor()

            # Таблица для метрик серверов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS server_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    server_id TEXT NOT NULL,
                    cpu_usage REAL,
                    memory_usage REAL,
                    disk_io REAL,
                    network_traffic REAL,
                    request_count INTEGER,
                    error_rate REAL,
                    response_time_ms REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица для логов доступа
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS access_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    client_ip TEXT,
                    request_method TEXT,
                    endpoint TEXT,
                    response_code INTEGER,
                    response_size INTEGER,
                    response_time_ms REAL,
                    referer TEXT,
                    user_agent TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Индексы для ускорения запросов
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_server_metrics_timestamp ON server_metrics(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_server_metrics_server_id ON server_metrics(server_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_access_logs_timestamp ON access_logs(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_access_logs_endpoint ON access_logs(endpoint)')

            self.connection.commit()
            print("Таблицы успешно созданы")

        except Exception as e:
            print(f"Ошибка создания таблиц: {e}")

    def insert_server_metrics(self, csv_file_path):
        """Импорт метрик серверов из CSV в базу данных"""
        try:
            df = pd.read_csv(csv_file_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Вставка данных
            df.to_sql('server_metrics', self.connection, if_exists='replace', index=False)
            print(f"Данные метрик серверов импортированы: {len(df)} записей")

        except Exception as e:
            print(f"Ошибка импорта метрик серверов: {e}")

    def insert_access_logs(self, csv_file_path):
        """Импорт логов доступа из CSV в базу данных"""
        try:
            df = pd.read_csv(csv_file_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Вставка данных
            df.to_sql('access_logs', self.connection, if_exists='replace', index=False)
            print(f"Данные логов доступа импортированы: {len(df)} записей")

        except Exception as e:
            print(f"Ошибка импорта логов доступа: {e}")

    def get_server_metrics(self, server_id=None, start_date=None, end_date=None):
        """Получение метрик серверов с фильтрацией"""
        try:
            query = "SELECT * FROM server_metrics WHERE 1=1"
            params = []

            if server_id:
                query += " AND server_id = ?"
                params.append(server_id)

            if start_date:
                query += " AND timestamp >= ?"
                params.append(start_date)

            if end_date:
                query += " AND timestamp <= ?"
                params.append(end_date)

            query += " ORDER BY timestamp"

            df = pd.read_sql_query(query, self.connection, params=params)
            return df

        except Exception as e:
            print(f"Ошибка получения метрик серверов: {e}")
            return pd.DataFrame()

    def get_access_logs_stats(self, hours=24):
        """Статистика по логам доступа за указанный период"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)

            query = """
            SELECT 
                COUNT(*) as total_requests,
                AVG(response_time_ms) as avg_response_time,
                COUNT(CASE WHEN response_code >= 400 THEN 1 END) as error_count,
                COUNT(DISTINCT client_ip) as unique_ips,
                COUNT(DISTINCT endpoint) as unique_endpoints
            FROM access_logs 
            WHERE timestamp BETWEEN ? AND ?
            """

            cursor = self.connection.cursor()
            cursor.execute(query, (start_time, end_time))
            result = cursor.fetchone()

            stats = {
                'total_requests': result['total_requests'],
                'avg_response_time': round(result['avg_response_time'], 2),
                'error_count': result['error_count'],
                'error_rate': round((result['error_count'] / result['total_requests']) * 100, 2) if result[
                                                                                                        'total_requests'] > 0 else 0,
                'unique_ips': result['unique_ips'],
                'unique_endpoints': result['unique_endpoints']
            }

            return stats

        except Exception as e:
            print(f"Ошибка получения статистики логов: {e}")
            return {}

    def get_top_endpoints(self, limit=10):
        """Топ самых популярных эндпоинтов"""
        try:
            query = """
            SELECT 
                endpoint,
                COUNT(*) as request_count,
                AVG(response_time_ms) as avg_response_time,
                COUNT(CASE WHEN response_code >= 400 THEN 1 END) as error_count
            FROM access_logs 
            GROUP BY endpoint 
            ORDER BY request_count DESC 
            LIMIT ?
            """

            df = pd.read_sql_query(query, self.connection, params=[limit])
            return df

        except Exception as e:
            print(f"Ошибка получения топ эндпоинтов: {e}")
            return pd.DataFrame()

    def close(self):
        """Закрытие соединения с базой данных"""
        if self.connection:
            self.connection.close()
            print("Соединение с базой данных закрыто")


# Демонстрация работы
if __name__ == "__main__":
    # Создаем экземпляр коннектора
    db = DatabaseConnector()

    # Импортируем данные если они существуют
    try:
        db.insert_server_metrics('../../data/synthetic_metrics.csv')
        db.insert_access_logs('../../data/parsed_access_logs.csv')
    except FileNotFoundError as e:
        print(f"Файлы данных не найдены: {e}")
        print("Сначала запустите скрипты генерации данных")

    # Демонстрация запросов
    print("\n=== Демонстрация работы с БД ===")

    # Получаем метрики для web сервера
    web_metrics = db.get_server_metrics(server_id='web_server_1')
    if not web_metrics.empty:
        print(f"Метрики web_server_1: {len(web_metrics)} записей")

    # Получаем статистику логов
    logs_stats = db.get_access_logs_stats(hours=24)
    print("Статистика логов:", json.dumps(logs_stats, indent=2, ensure_ascii=False))

    # Получаем топ эндпоинтов
    top_endpoints = db.get_top_endpoints(5)
    if not top_endpoints.empty:
        print("\nТоп 5 эндпоинтов:")
        print(top_endpoints.to_string(index=False))

    # Закрываем соединение
    db.close()