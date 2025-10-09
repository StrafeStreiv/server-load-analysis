import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os


def generate_synthetic_metrics():
    """
    Генерация синтетических метрик серверов для тестирования системы мониторинга
    """
    # Параметры генерации
    num_servers = 3
    hours = 24  # Данные за 24 часа
    data_points = hours * 60  # По одной записи в минуту

    # Список для хранения данных
    data = []

    # Базовые параметры серверов
    servers = {
        'web_server_1': {'base_cpu': 30, 'base_memory': 50, 'traffic_factor': 1.2},
        'api_server_1': {'base_cpu': 40, 'base_memory': 35, 'traffic_factor': 1.5},
        'db_server_1': {'base_cpu': 25, 'base_memory': 70, 'traffic_factor': 0.8}
    }

    start_time = datetime(2024, 1, 1, 0, 0, 0)

    for server_id, params in servers.items():
        for i in range(data_points):
            current_time = start_time + timedelta(minutes=i)

            # Базовые значения с суточными колебаниями
            hour = current_time.hour
            time_factor = 0.5 + 0.5 * np.sin(2 * np.pi * (hour - 9) / 24)  # Пик в 9 утра

            # Генерация метрик с добавлением шума и аномалий
            cpu_usage = params['base_cpu'] * time_factor + np.random.normal(0, 5)
            cpu_usage = max(0, min(100, cpu_usage))

            memory_usage = params['base_memory'] + np.random.normal(0, 3)
            memory_usage = max(0, min(100, memory_usage))

            disk_io = 50 + 50 * time_factor + np.random.normal(0, 10)
            disk_io = max(0, disk_io)

            network_traffic = 30 * params['traffic_factor'] * time_factor + np.random.normal(0, 5)
            network_traffic = max(0, network_traffic)

            request_count = int(100 + 200 * time_factor + np.random.normal(0, 20))
            request_count = max(0, request_count)

            # Время ответа зависит от нагрузки
            response_time = 50 + (cpu_usage / 2) + np.random.normal(0, 10)
            response_time = max(10, response_time)

            # Процент ошибок
            error_rate = 0.5 + (cpu_usage / 100) * 2 + np.random.normal(0, 0.2)
            error_rate = max(0, min(10, error_rate))

            # Добавление аномалий
            if i == 300:  # В 5:00 - аномалия на web сервере
                if server_id == 'web_server_1':
                    cpu_usage = 95
                    error_rate = 8.0
                    response_time = 500

            data.append({
                'timestamp': current_time,
                'server_id': server_id,
                'cpu_usage': round(cpu_usage, 2),
                'memory_usage': round(memory_usage, 2),
                'disk_io': round(disk_io, 2),
                'network_traffic': round(network_traffic, 2),
                'request_count': request_count,
                'error_rate': round(error_rate, 2),
                'response_time_ms': round(response_time, 2)
            })

    # Создание DataFrame
    df = pd.DataFrame(data)

    # Сохранение в CSV
    os.makedirs('../../data', exist_ok=True)
    df.to_csv('../../data/synthetic_metrics.csv', index=False)
    print(f"Сгенерировано {len(df)} записей для {num_servers} серверов")
    print("Файл сохранен: data/synthetic_metrics.csv")

    # Вывод статистики
    print("\nСтатистика по серверам:")
    for server in servers.keys():
        server_data = df[df['server_id'] == server]
        print(f"\n{server}:")
        print(f"  CPU: {server_data['cpu_usage'].mean():.1f}% ± {server_data['cpu_usage'].std():.1f}%")
        print(f"  Время ответа: {server_data['response_time_ms'].mean():.1f}ms")


if __name__ == "__main__":
    generate_synthetic_metrics()