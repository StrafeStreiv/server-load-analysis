import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')


class AdvancedAnalyzer:
    def __init__(self, db_connector):
        self.db = db_connector

    def detect_anomalies(self, df, method='zscore', threshold=3):
        """Обнаружение аномалий различными методами"""
        anomalies = pd.DataFrame()

        if method == 'zscore':
            # Z-score метод
            numeric_cols = ['cpu_usage', 'memory_usage', 'response_time_ms', 'error_rate']
            for col in numeric_cols:
                if col in df.columns:
                    z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                    col_anomalies = df[z_scores > threshold]
                    anomalies = pd.concat([anomalies, col_anomalies])

        elif method == 'iqr':
            # IQR метод
            numeric_cols = ['cpu_usage', 'memory_usage', 'response_time_ms', 'error_rate']
            for col in numeric_cols:
                if col in df.columns:
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    col_anomalies = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
                    anomalies = pd.concat([anomalies, col_anomalies])

        return anomalies.drop_duplicates()

    def cluster_servers(self, df):
        """Кластеризация серверов по паттернам нагрузки"""
        features = df.groupby('server_id').agg({
            'cpu_usage': ['mean', 'std', 'max'],
            'memory_usage': ['mean', 'std'],
            'response_time_ms': ['mean', 'max'],
            'error_rate': 'mean'
        }).round(3)

        # Выравниваем колонки
        features.columns = ['_'.join(col).strip() for col in features.columns]

        # Стандартизация
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(features)

        # K-means кластеризация
        kmeans = KMeans(n_clusters=min(3, len(features)), random_state=42)
        clusters = kmeans.fit_predict(scaled_features)

        features['cluster'] = clusters

        print("📊 Server Clustering Results:")
        for cluster_id in range(len(set(clusters))):
            cluster_servers = features[features['cluster'] == cluster_id]
            print(f"Cluster {cluster_id}: {', '.join(cluster_servers.index.tolist())}")

        return features

    def forecast_load(self, df, hours=6):
        """Простое прогнозирование нагрузки"""
        df = df.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

        # Ресемплируем по часам
        hourly_load = df['cpu_usage'].resample('H').mean()

        # Простой прогноз на основе скользящего среднего
        forecast = hourly_load.rolling(window=3).mean().iloc[-hours:]

        print("🔮 Load Forecast (next 6 hours):")
        for time, load in forecast.items():
            print(f"  {time.strftime('%H:%M')}: {load:.1f}% CPU")

        return forecast

    def generate_report(self):
        """Генерация расширенного отчета"""
        print("📈 ADVANCED ANALYSIS REPORT")
        print("=" * 50)

        # Получаем данные
        metrics_df = self.db.get_server_metrics()

        if metrics_df.empty:
            print("No data available for analysis")
            return

        # 1. Обнаружение аномалий
        print("\n1. 🚨 ANOMALY DETECTION")
        anomalies = self.detect_anomalies(metrics_df)
        print(f"Found {len(anomalies)} anomalous records")
        if not anomalies.empty:
            print("Top anomalies:")
            print(anomalies[['timestamp', 'server_id', 'cpu_usage', 'response_time_ms']].head())

        # 2. Кластеризация серверов
        print("\n2. 🎯 SERVER CLUSTERING")
        clusters = self.cluster_servers(metrics_df)

        # 3. Прогнозирование нагрузки
        print("\n3. 🔮 LOAD FORECASTING")
        forecast = self.forecast_load(metrics_df)

        # 4. Рекомендации
        print("\n4. 💡 RECOMMENDATIONS")
        self.generate_recommendations(metrics_df)

    def generate_recommendations(self, df):
        """Генерация рекомендаций по оптимизации"""
        server_stats = df.groupby('server_id').agg({
            'cpu_usage': ['mean', 'max'],
            'memory_usage': ['mean', 'max'],
            'response_time_ms': 'mean',
            'error_rate': 'mean'
        }).round(2)

        print("Optimization recommendations:")

        for server in server_stats.index:
            avg_cpu = server_stats.loc[server, ('cpu_usage', 'mean')]
            max_cpu = server_stats.loc[server, ('cpu_usage', 'max')]
            avg_response = server_stats.loc[server, ('response_time_ms', 'mean')]

            if avg_cpu > 70:
                print(f"  ⚠️  {server}: High average CPU ({avg_cpu}%) - consider scaling")
            elif max_cpu > 90:
                print(f"  🔥 {server}: CPU spikes detected (up to {max_cpu}%) - optimize peak load")

            if avg_response > 200:
                print(f"  🐌 {server}: Slow response time ({avg_response}ms) - investigate bottlenecks")