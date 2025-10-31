import pandas as pd
import random
from datetime import datetime, timedelta
import os


def generate_realistic_logs():
    """Генерация реалистичных логов на основе реальных паттернов"""

    print("Генерация реалистичных веб-логов...")

    logs = []
    base_time = datetime.now() - timedelta(hours=24)

    # Эндпоинты с разными характеристиками: (endpoint, method, error_rate, slow_rate, traffic_weight)
    endpoints = [
        ("/api/users", "GET", 0.1, 0.05, 15),
        ("/api/products", "GET", 0.05, 0.02, 20),
        ("/api/orders", "POST", 0.15, 0.1, 10),
        ("/api/auth/login", "POST", 0.2, 0.08, 8),
        ("/static/css/style.css", "GET", 0.01, 0.001, 25),
        ("/static/js/app.js", "GET", 0.01, 0.001, 25),
        ("/static/images/logo.png", "GET", 0.01, 0.001, 30),
        ("/", "GET", 0.02, 0.005, 40),
        ("/admin", "GET", 0.3, 0.2, 3),
        ("/api/health", "GET", 0.1, 0.01, 5),
        ("/api/metrics", "GET", 0.08, 0.03, 4),
        ("/api/search", "GET", 0.12, 0.15, 12)
    ]

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "curl/7.68.0",
        "PostmanRuntime/7.26.5",
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)"
    ]

    http_methods = ["GET", "POST", "PUT", "DELETE", "PATCH"]

    # Создаем взвешенный список эндпоинтов на основе traffic_weight
    weighted_endpoints = []
    for endpoint, method, error_rate, slow_rate, weight in endpoints:
        weighted_endpoints.extend([(endpoint, method, error_rate, slow_rate)] * weight)

    # Генерация 5000 записей логов
    for i in range(5000):
        endpoint, method, error_rate, slow_rate = random.choice(weighted_endpoints)

        # Временная метка с реалистичным распределением (больше днем, меньше ночью)
        hour = random.randint(0, 23)
        # Увеличиваем вероятность запросов в рабочее время
        if 9 <= hour <= 18:
            time_offset = random.randint(0, 3600)  # 1 час в рабочее время
        else:
            time_offset = random.randint(0, 7200)  # 2 часа в нерабочее время

        timestamp = base_time + timedelta(hours=hour, seconds=time_offset)

        # Определяем статус код на основе error_rate
        if random.random() < error_rate:
            status_code = random.choice([400, 401, 403, 404, 500, 503])
        else:
            status_code = 200

        # Время ответа в зависимости от эндпоинта, статуса и slow_rate
        base_response_time = 0.05  # базовое время

        # Учитываем тип эндпоинта
        if endpoint.startswith("/api/"):
            base_response_time += random.uniform(0.1, 0.5)
        if endpoint.startswith("/admin"):
            base_response_time += random.uniform(0.3, 1.0)

        # Учитываем ошибки
        if status_code >= 400:
            base_response_time += random.uniform(0.5, 2.0)

        # Учитываем медленные запросы
        if random.random() < slow_rate:
            base_response_time += random.uniform(1.0, 3.0)

        # Случайные вариации
        response_time = max(0.01, base_response_time + random.uniform(-0.02, 0.02))

        # Размер ответа в зависимости от типа контента
        if endpoint.startswith("/static/"):
            response_size = random.randint(5000, 50000)  # статические файлы больше
        elif endpoint.startswith("/api/"):
            response_size = random.randint(200, 2000)  # API ответы меньше
        else:
            response_size = random.randint(1000, 10000)  # HTML страницы

        log_entry = {
            'timestamp': timestamp,
            'client_ip': f"192.168.1.{random.randint(1, 255)}",
            'request_method': method,
            'endpoint': endpoint,
            'response_code': status_code,
            'response_size': response_size,
            'response_time_ms': round(response_time * 1000, 2),
            'user_agent': random.choice(user_agents),
            'referer': random.choice(["https://example.com", "https://google.com", "https://direct.com", ""])
        }

        logs.append(log_entry)

    # Сортируем по времени
    logs.sort(key=lambda x: x['timestamp'])

    # Сохраняем в CSV
    df = pd.DataFrame(logs)
    os.makedirs('data', exist_ok=True)
    df.to_csv('data/realistic_web_logs.csv', index=False)

    print(f"Сгенерировано реалистичных логов: {len(df)} записей")
    print(f"Статистика по кодам ответов:")
    print(df['response_code'].value_counts().sort_index())
    print(f"Топ-5 эндпоинтов:")
    print(df['endpoint'].value_counts().head())
    print(f"Среднее время ответа: {df['response_time_ms'].mean():.2f} ms")

    return df


def analyze_generated_logs():
    """Анализ сгенерированных логов"""
    try:
        df = pd.read_csv('data/realistic_web_logs.csv')
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        print("\n Анализ сгенерированных логов:")
        print(f"Период данных: от {df['timestamp'].min()} до {df['timestamp'].max()}")
        print(f"Уникальных клиентов: {df['client_ip'].nunique()}")
        print(f"Уникальных эндпоинтов: {df['endpoint'].nunique()}")

        # Анализ по часам
        df['hour'] = df['timestamp'].dt.hour
        hourly_stats = df.groupby('hour').agg({
            'response_time_ms': 'mean',
            'response_code': 'count'
        }).round(2)

        print("\n Нагрузка по часам:")
        print(hourly_stats)

    except FileNotFoundError:
        print("Файл логов не найден. Сначала запустите генерацию.")


if __name__ == "__main__":
    generate_realistic_logs()
    analyze_generated_logs()