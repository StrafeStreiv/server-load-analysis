import pandas as pd
import re
from datetime import datetime
import os


def parse_nginx_log(log_file_path, output_file_path):
    """
    Парсинг Nginx access.log в структурированный CSV формат
    """
    # Регулярное выражение для парсинга стандартного формата Nginx
    log_pattern = r'(\S+) - - \[(.*?)\] "(\S+) (\S+) (\S+)" (\d+) (\d+) "([^"]*)" "([^"]*)" (\d+\.\d+)'

    parsed_data = []

    try:
        with open(log_file_path, 'r', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                try:
                    match = re.match(log_pattern, line.strip())
                    if match:
                        groups = match.groups()

                        # Парсинг timestamp
                        timestamp_str = groups[1].split()[0]  # Берем только дату, игнорируем часовой пояс
                        timestamp = datetime.strptime(timestamp_str, '%d/%b/%Y:%H:%M:%S')

                        # Извлечение данных
                        client_ip = groups[0]
                        request_method = groups[2]
                        endpoint = groups[3]
                        response_code = int(groups[5])
                        response_size = int(groups[6]) if groups[6] != '-' else 0
                        referer = groups[7]
                        user_agent = groups[8]
                        response_time = float(groups[9]) if groups[9] != '-' else 0.0

                        parsed_data.append({
                            'timestamp': timestamp,
                            'client_ip': client_ip,
                            'request_method': request_method,
                            'endpoint': endpoint,
                            'response_code': response_code,
                            'response_size': response_size,
                            'response_time_ms': round(response_time * 1000, 2),  # Конвертируем в миллисекунды
                            'referer': referer,
                            'user_agent': user_agent
                        })

                    elif line.strip():  # Пропускаем пустые строки
                        print(f"Warning: Не удалось распарсить строку {line_num}: {line.strip()}")

                except Exception as e:
                    print(f"Error processing line {line_num}: {e}")
                    continue

    except FileNotFoundError:
        print(f"Error: Файл {log_file_path} не найден")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

    if not parsed_data:
        print("No valid log entries found")
        return None

    # Создание DataFrame
    df = pd.DataFrame(parsed_data)

    # Сохранение в CSV
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    df.to_csv(output_file_path, index=False)

    print(f"Парсинг завершен! Обработано {len(df)} записей")
    print(f"Файл сохранен: {output_file_path}")

    # Статистика
    print("\nСтатистика логов:")
    print(f"Уникальных IP: {df['client_ip'].nunique()}")
    print(f"Методы запросов: {dict(df['request_method'].value_counts())}")
    print(f"Коды ответов: {dict(df['response_code'].value_counts().head())}")
    print(f"Среднее время ответа: {df['response_time_ms'].mean():.2f}ms")

    return df


def generate_sample_logs():
    """
    Генерация тестовых логов для демонстрации
    """
    sample_logs = [
        '192.168.1.1 - - [09/Oct/2024:10:30:00 +0300] "GET /api/users HTTP/1.1" 200 1543 "https://example.com" "Mozilla/5.0" 0.045',
        '192.168.1.2 - - [09/Oct/2024:10:30:01 +0300] "POST /api/login HTTP/1.1" 201 234 "https://example.com" "Mozilla/5.0" 0.123',
        '192.168.1.3 - - [09/Oct/2024:10:30:02 +0300] "GET /static/css/style.css HTTP/1.1" 200 5432 "https://example.com" "Mozilla/5.0" 0.012',
        '192.168.1.1 - - [09/Oct/2024:10:30:03 +0300] "GET /api/products HTTP/1.1" 200 8765 "https://example.com" "Mozilla/5.0" 0.067',
        '192.168.1.4 - - [09/Oct/2024:10:30:04 +0300] "GET /admin HTTP/1.1" 403 123 "https://example.com" "Mozilla/5.0" 0.034',
        '192.168.1.2 - - [09/Oct/2024:10:30:05 +0300] "POST /api/orders HTTP/1.1" 500 0 "https://example.com" "Mozilla/5.0" 1.234',
        '192.168.1.5 - - [09/Oct/2024:10:30:06 +0300] "GET /api/health HTTP/1.1" 200 89 "https://example.com" "Mozilla/5.0" 0.008',
        '192.168.1.1 - - [09/Oct/2024:10:30:07 +0300] "GET /images/logo.png HTTP/1.1" 200 15432 "https://example.com" "Mozilla/5.0" 0.023',
        '192.168.1.3 - - [09/Oct/2024:10:30:08 +0300] "PUT /api/users/123 HTTP/1.1" 404 267 "https://example.com" "Mozilla/5.0" 0.089',
        '192.168.1.6 - - [09/Oct/2024:10:30:09 +0300] "GET /api/metrics HTTP/1.1" 200 3456 "https://example.com" "Mozilla/5.0" 0.156'
    ]

    os.makedirs('../../data', exist_ok=True)
    with open('../../data/sample_access.log', 'w') as f:
        for log_entry in sample_logs:
            f.write(log_entry + '\n')

    print("Создан тестовый файл логов: data/sample_access.log")


if __name__ == "__main__":
    # Создаем тестовые логи
    generate_sample_logs()

    # Парсим созданные логи
    parse_nginx_log(
        log_file_path='../../data/sample_access.log',
        output_file_path='../../data/parsed_access_logs.csv'
    )