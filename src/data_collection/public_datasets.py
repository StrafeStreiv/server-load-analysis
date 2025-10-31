import pandas as pd
import requests
import io


def download_public_datasets():
    """Загрузка публичных датасетов для обогащения"""

    # 1. NASA HTTP logs (публичный датасет)
    nasa_url = "https://raw.githubusercontent.com/logpai/loghub/master/HTTP/HTTP_2k.log"

    # 2. Sample web server logs from Kaggle
    kaggle_datasets = {
        "web_logs": "https://raw.githubusercontent.com/datasets/nginx-log/master/data/access.log"
    }

    datasets = {}

    try:
        # Загрузка NASA логов
        response = requests.get(nasa_url)
        if response.status_code == 200:
            with open("data/nasa_http_logs.log", "w") as f:
                f.write(response.text)
            print("Downloaded NASA HTTP logs")

    except Exception as e:
        print(f"Error downloading public datasets: {e}")

    return datasets


def create_hybrid_dataset():
    """Создание гибридного датасета из реальных и синтетических данных"""

    # Реальные системные метрики
    real_metrics = pd.read_csv("data/real_metrics_production_server_*.csv")

    # Синтетические метрики для масштабирования
    synthetic_metrics = pd.read_csv("data/synthetic_metrics.csv")

    # Реальные логи
    real_logs = pd.read_csv("data/parsed_access_logs.csv")

    # Публичные логи для разнообразия
    public_logs = "data/nasa_http_logs.log"

    print("Hybrid dataset components:")
    print(f"- Real metrics: {len(real_metrics)} records")
    print(f"- Synthetic metrics: {len(synthetic_metrics)} records")
    print(f"- Real logs: {len(real_logs)} records")
    print(f"- Public logs: {public_logs}")

    return {
        'real_metrics': real_metrics,
        'synthetic_metrics': synthetic_metrics,
        'real_logs': real_logs,
        'public_logs': public_logs
    }


if __name__ == "__main__":
    download_public_datasets()
    hybrid_data = create_hybrid_dataset()