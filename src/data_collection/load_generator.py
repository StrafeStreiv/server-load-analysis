import requests
import time
import random
import threading
from datetime import datetime


class LoadGenerator:
    def __init__(self, base_url="http://localhost:80"):
        self.base_url = base_url
        self.endpoints = [
            "/", "/api/users", "/api/products", "/static/style.css",
            "/api/orders", "/admin", "/api/health", "/images/logo.png"
        ]
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "PostmanRuntime/7.26.5"
        ]

    def generate_request(self):
        """Генерация одного HTTP запроса"""
        try:
            endpoint = random.choice(self.endpoints)
            headers = {
                'User-Agent': random.choice(self.user_agents)
            }

            response = requests.get(
                f"{self.base_url}{endpoint}",
                headers=headers,
                timeout=10
            )

            return {
                'timestamp': datetime.now(),
                'endpoint': endpoint,
                'status_code': response.status_code,
                'response_time': response.elapsed.total_seconds(),
                'success': response.status_code < 400
            }

        except Exception as e:
            return {
                'timestamp': datetime.now(),
                'endpoint': endpoint,
                'status_code': 0,
                'response_time': 0,
                'success': False,
                'error': str(e)
            }

    def generate_load(self, duration_minutes=30, requests_per_minute=100):
        """Генерация нагрузки в течение указанного времени"""
        print(f"Generating load: {requests_per_minute} RPM for {duration_minutes} minutes")

        end_time = time.time() + duration_minutes * 60
        request_interval = 60.0 / requests_per_minute

        while time.time() < end_time:
            thread = threading.Thread(target=self.generate_request)
            thread.start()
            time.sleep(request_interval)


if __name__ == "__main__":
    generator = LoadGenerator()
    generator.generate_load(duration_minutes=60, requests_per_minute=50)