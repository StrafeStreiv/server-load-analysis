import os
import json
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class DatabaseConfig:
    db_path: str = "server_metrics.db"
    host: str = "localhost"
    port: int = 5432
    username: str = "user"
    password: str = "password"


@dataclass
class AnalysisConfig:
    anomaly_threshold: float = 90.0  # CPU usage %
    error_threshold: float = 5.0  # Error rate %
    slow_request_threshold: float = 500.0  # Response time ms


@dataclass
class DataGenerationConfig:
    num_servers: int = 3
    hours_of_data: int = 24
    data_points_per_hour: int = 60


class Config:
    def __init__(self, config_file="config.json"):
        self.config_file = config_file
        self.db = DatabaseConfig()
        self.analysis = AnalysisConfig()
        self.data_gen = DataGenerationConfig()
        self.load_config()

    def load_config(self):
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                config_data = json.load(f)
                self._update_from_dict(config_data)
        else:
            self.create_default_config()

    def _update_from_dict(self, config_data: Dict[str, Any]):
        if 'database' in config_data:
            db_data = config_data['database']
            self.db = DatabaseConfig(**db_data)

        if 'analysis' in config_data:
            analysis_data = config_data['analysis']
            self.analysis = AnalysisConfig(**analysis_data)

        if 'data_generation' in config_data:
            data_gen_data = config_data['data_generation']
            self.data_gen = DataGenerationConfig(**data_gen_data)

    def create_default_config(self):
        default_config = {
            "database": {
                "db_path": "server_metrics.db",
                "host": "localhost",
                "port": 5432,
                "username": "user",
                "password": "password"
            },
            "analysis": {
                "anomaly_threshold": 90.0,
                "error_threshold": 5.0,
                "slow_request_threshold": 500.0
            },
            "data_generation": {
                "num_servers": 3,
                "hours_of_data": 24,
                "data_points_per_hour": 60
            }
        }

        with open(self.config_file, 'w') as f:
            json.dump(default_config, f, indent=2)

        print(f"Created default config file: {self.config_file}")

    def save_config(self):
        config_data = {
            "database": self.db.__dict__,
            "analysis": self.analysis.__dict__,
            "data_generation": self.data_gen.__dict__
        }

        with open(self.config_file, 'w') as f:
            json.dump(config_data, f, indent=2)


# Global config instance
config = Config()