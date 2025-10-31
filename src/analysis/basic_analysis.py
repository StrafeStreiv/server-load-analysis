import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np
import os


def load_and_analyze_metrics():
    """Загрузка и базовый анализ метрик серверов"""
    print("=== ПЕРВИЧНЫЙ АНАЛИЗ МЕТРИК СЕРВЕРОВ ===\n")

    try:
        # Загрузка данных
        df = pd.read_csv('data/synthetic_metrics.csv')
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        print(f"Общий объем данных: {len(df)} записей")
        print(f"Период данных: от {df['timestamp'].min()} до {df['timestamp'].max()}")
        print(f"Уникальные серверы: {df['server_id'].unique().tolist()}")

        # 1. Базовая статистика по серверам
        print("\n--- СТАТИСТИКА ПО СЕРВЕРАМ ---")
        server_stats = df.groupby('server_id').agg({
            'cpu_usage': ['mean', 'max', 'std'],
            'memory_usage': ['mean', 'max', 'std'],
            'response_time_ms': ['mean', 'max', 'std'],
            'error_rate': ['mean', 'max']
        }).round(2)

        print(server_stats)

        # 2. Анализ по времени суток
        print("\n--- АНАЛИЗ ПО ЧАСАМ СУТОК ---")
        df['hour'] = df['timestamp'].dt.hour
        hourly_stats = df.groupby('hour').agg({
            'cpu_usage': 'mean',
            'request_count': 'sum',
            'error_rate': 'mean',
            'response_time_ms': 'mean'
        }).round(2)

        print(hourly_stats)

        # 3. Обнаружение аномалий
        print("\n--- ОБНАРУЖЕНИЕ АНОМАЛИЙ ---")
        # Аномалии по CPU (выше 90%)
        cpu_anomalies = df[df['cpu_usage'] > 90]
        print(f"Пики нагрузки CPU (>90%): {len(cpu_anomalies)} случаев")
        if not cpu_anomalies.empty:
            print("Серверы с пиковой нагрузкой:")
            print(cpu_anomalies[['timestamp', 'server_id', 'cpu_usage']].head())

        # Аномалии по ошибкам (выше 5%)
        error_anomalies = df[df['error_rate'] > 5]
        print(f"\nВысокий процент ошибок (>5%): {len(error_anomalies)} случаев")
        if not error_anomalies.empty:
            print("Серверы с высоким error rate:")
            print(error_anomalies[['timestamp', 'server_id', 'error_rate', 'cpu_usage']].head())

        # 4. Корреляционный анализ
        print("\n--- КОРРЕЛЯЦИЯ МЕТРИК ---")
        correlation_matrix = df[['cpu_usage', 'memory_usage', 'response_time_ms', 'error_rate']].corr()
        print(correlation_matrix.round(3))

        # Визуализация
        create_basic_visualizations(df)

        return df

    except FileNotFoundError:
        print("Ошибка: Файл с данными не найден. Сначала запустите генерацию данных.")
        return None


def analyze_access_logs():
    """Анализ логов доступа"""
    print("\n\n=== АНАЛИЗ ЛОГОВ ДОСТУПА ===")

    try:
        logs_df = pd.read_csv('data/parsed_access_logs.csv')
        logs_df['timestamp'] = pd.to_datetime(logs_df['timestamp'])

        print(f"Всего записей в логах: {len(logs_df)}")
        print(f"Уникальных клиентов: {logs_df['client_ip'].nunique()}")
        print(f"Уникальных эндпоинтов: {logs_df['endpoint'].nunique()}")

        # Статистика по кодам ответов
        print("\n--- СТАТИСТИКА HTTP-ОТВЕТОВ ---")
        status_stats = logs_df['response_code'].value_counts()
        print(status_stats)

        # Топ эндпоинтов
        print("\n--- ТОП-5 САМЫХ ЗАПРАШИВАЕМЫХ ЭНДПОИНТОВ ---")
        top_endpoints = logs_df['endpoint'].value_counts().head(5)
        print(top_endpoints)

        # Анализ времени ответа
        print("\n--- АНАЛИЗ ВРЕМЕНИ ОТВЕТА ---")
        response_time_stats = logs_df['response_time_ms'].describe()
        print(response_time_stats)

        # Медленные запросы
        slow_requests = logs_df[logs_df['response_time_ms'] > 500]
        print(f"\nМедленные запросы (>500ms): {len(slow_requests)}")
        if not slow_requests.empty:
            print("Самые медленные эндпоинты:")
            print(slow_requests.groupby('endpoint')['response_time_ms'].mean().sort_values(ascending=False).head(3))

    except FileNotFoundError:
        print("Логи не найдены. Сначала запустите парсинг логов.")


def create_basic_visualizations(df):
    """Создание базовых визуализаций"""
    print("\n--- СОЗДАНИЕ ВИЗУАЛИЗАЦИЙ ---")

    # Создаем папку для графиков
    os.makedirs('../reports', exist_ok=True)

    # 1. График нагрузки CPU по серверам
    plt.figure(figsize=(12, 6))
    for server in df['server_id'].unique():
        server_data = df[df['server_id'] == server]
        plt.plot(server_data['timestamp'], server_data['cpu_usage'], label=server, alpha=0.7)

    plt.title('Нагрузка CPU по серверам')
    plt.xlabel('Время')
    plt.ylabel('CPU Usage (%)')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('../reports/cpu_usage_trends.png', dpi=300, bbox_inches='tight')
    plt.close()

    # 2. Heatmap корреляций
    plt.figure(figsize=(8, 6))
    correlation_matrix = df[['cpu_usage', 'memory_usage', 'response_time_ms', 'error_rate']].corr()
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0)
    plt.title('Корреляция метрик производительности')
    plt.tight_layout()
    plt.savefig('../reports/metrics_correlation.png', dpi=300, bbox_inches='tight')
    plt.close()

    # 3. Распределение времени ответа
    plt.figure(figsize=(10, 6))
    for server in df['server_id'].unique():
        server_data = df[df['server_id'] == server]
        plt.hist(server_data['response_time_ms'], alpha=0.7, label=server, bins=20)

    plt.title('Распределение времени ответа по серверам')
    plt.xlabel('Response Time (ms)')
    plt.ylabel('Frequency')
    plt.legend()
    plt.tight_layout()
    plt.savefig('../reports/response_time_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()

    print("Графики сохранены в папку 'reports/'")


def generate_analysis_report():
    """Генерация итогового отчета"""
    print("\n\n=== ИТОГОВЫЙ ОТЧЕТ ===")

    df = load_and_analyze_metrics()
    if df is not None:
        # Ключевые выводы
        print("\n--- КЛЮЧЕВЫЕ ВЫВОДЫ ---")

        # Самый загруженный сервер
        busiest_server = df.groupby('server_id')['cpu_usage'].mean().idxmax()
        avg_cpu = df.groupby('server_id')['cpu_usage'].mean().max()
        print(f"Самый загруженный сервер: {busiest_server} (средняя загрузка CPU: {avg_cpu:.1f}%)")

        # Пиковые часы
        peak_hour = df.groupby('hour')['request_count'].sum().idxmax()
        print(f"Пиковый час нагрузки: {peak_hour}:00")

        # Корреляция CPU и времени ответа
        cpu_response_corr = df['cpu_usage'].corr(df['response_time_ms'])
        print(f"Корреляция CPU и времени ответа: {cpu_response_corr:.3f}")

        # Общие рекомендации
        print("\n--- ПЕРВОНАЧАЛЬНЫЕ РЕКОМЕНДАЦИИ ---")
        if cpu_response_corr > 0.7:
            print("• Высокая корреляция CPU и времени ответа - оптимизируйте вычислительную нагрузку")

        if len(df[df['error_rate'] > 5]) > 10:
            print("• Обнаружены частые случаи высокого error rate - требуется анализ причин ошибок")

        avg_response_time = df['response_time_ms'].mean()
        if avg_response_time > 200:
            print(f"• Высокое среднее время ответа ({avg_response_time:.1f}ms) - требуется оптимизация")

    analyze_access_logs()


if __name__ == "__main__":
    generate_analysis_report()