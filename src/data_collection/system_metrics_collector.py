import psutil
import time
import pandas as pd
from datetime import datetime
import requests
import json


class SystemMetricsCollector:
    def __init__(self, server_id="local_server"):
        self.server_id = server_id
        self.metrics_data = []

    def collect_metrics(self):
        """Сбор системных метрик"""
        try:
            # CPU метрики
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_times = psutil.cpu_times()

            # Memory метрики
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()

            # Disk метрики
            disk_io = psutil.disk_io_counters()
            disk_usage = psutil.disk_usage('/')

            # Network метрики
            net_io = psutil.net_io_counters()

            metrics = {
                'timestamp': datetime.now(),
                'server_id': self.server_id,
                'cpu_usage': cpu_percent,
                'cpu_user': cpu_times.user,
                'cpu_system': cpu_times.system,
                'memory_usage': memory.percent,
                'memory_used_gb': round(memory.used / (1024 ** 3), 2),
                'memory_available_gb': round(memory.available / (1024 ** 3), 2),
                'swap_usage': swap.percent,
                'disk_usage': disk_usage.percent,
                'disk_read_mb': round(disk_io.read_bytes / (1024 ** 2), 2) if disk_io else 0,
                'disk_write_mb': round(disk_io.write_bytes / (1024 ** 2), 2) if disk_io else 0,
                'network_sent_mb': round(net_io.bytes_sent / (1024 ** 2), 2),
                'network_recv_mb': round(net_io.bytes_recv / (1024 ** 2), 2),
                'load_average': psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0
            }

            self.metrics_data.append(metrics)
            return metrics

        except Exception as e:
            print(f"Error collecting metrics: {e}")
            return None

    def collect_for_duration(self, duration_minutes=60, interval_seconds=30):
        """Сбор метрик в течение указанного времени"""
        print(f"Starting metrics collection for {duration_minutes} minutes...")

        end_time = time.time() + duration_minutes * 60
        collection_count = 0

        while time.time() < end_time:
            metrics = self.collect_metrics()
            if metrics:
                collection_count += 1
                print(f"Collected {collection_count} samples...")

            time.sleep(interval_seconds)

        self.save_to_csv()
        return self.metrics_data

    def save_to_csv(self):
        """Сохранение собранных метрик в CSV"""
        if self.metrics_data:
            df = pd.DataFrame(self.metrics_data)
            filename = f"data/real_metrics_{self.server_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            df.to_csv(filename, index=False)
            print(f"Metrics saved to {filename}")
            return filename
        return None


# Сбор метрик в фоне
if __name__ == "__main__":
    collector = SystemMetricsCollector("production_server")
    collector.collect_for_duration(duration_minutes=5, interval_seconds=30)