"""
Главный модуль приложения - точка входа
"""

import argparse
import sys
import os
import yaml
from typing import Dict, Any

# Добавляем пути для импортов
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.error_handler import error_handler
from connector.db_connector import DatabaseConnector
from scripts.generate_synthetic_metrics import generate_synthetic_metrics
from scripts.parse_access_log import parse_nginx_log, generate_sample_logs
from data_collection.real_logs_generator import generate_realistic_logs
from analysis.basic_analysis import load_and_analyze_metrics, analyze_access_logs
from analysis.statistical_evaluation import StatisticalEvaluator

from data_collection.bigdata_processor import BigDataProcessor


class ServerLoadAnalyzer:
    """Основной класс приложения"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.config = self.load_config()
        self.setup_environment()

    def load_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации из YAML файла"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            error_handler.handle_error(e, "Loading configuration")
            # Конфигурация по умолчанию
            return {
                'app': {'name': 'Server Load Analysis', 'debug': True},
                'paths': {
                    'data_directory': 'data',
                    'reports_directory': 'reports',
                    'logs_directory': 'logs'
                }
            }

    def setup_environment(self):
        """Настройка окружения"""
        # Создаем необходимые папки
        paths = self.config.get('paths', {})
        for path_key in ['data_directory', 'reports_directory', 'logs_directory']:
            if path_key in paths:
                os.makedirs(paths[path_key], exist_ok=True)

    def run_full_pipeline(self):
        """Запуск полного пайплайна анализа"""
        print("🚀 Starting full analysis pipeline...")

        try:
            # 1. Генерация данных
            print("\n📊 Phase 1: Data Generation")
            generate_synthetic_metrics()
            generate_realistic_logs()

            # 2. Парсинг и подготовка
            print("\n🔧 Phase 2: Data Preparation")
            generate_sample_logs()
            parse_nginx_log('data/sample_access.log', 'data/parsed_access_logs.csv')

            # 3. Загрузка в БД
            print("\n💾 Phase 3: Database Operations")
            db = DatabaseConnector()
            db.insert_server_metrics('data/synthetic_metrics.csv')
            db.insert_access_logs('data/parsed_access_logs.csv')

            # 4. Базовый анализ
            print("\n📈 Phase 4: Basic Analysis")
            load_and_analyze_metrics()
            analyze_access_logs()

            # 5. Статистический анализ
            print("\n🎯 Phase 5: Statistical Evaluation")
            evaluator = StatisticalEvaluator()
            evaluator.evaluate_data_completeness(
                db.get_server_metrics()
            )
            evaluator.generate_statistical_report()

            db.close()

            print("\n✅ Pipeline completed successfully!")

        except Exception as e:
            error_handler.handle_error(e, "Full pipeline execution", raise_again=True)

    def run_benchmarks(self):
        """Запуск бенчмарков производительности"""
        print("⚡ Running performance benchmarks...")

        try:
            benchmark = PerformanceBenchmark()
            benchmark.benchmark_data_loading()
            benchmark.benchmark_analysis_tasks()
            benchmark.benchmark_scalability(max_records=100000)
            benchmark.generate_benchmark_report()

            print("✅ Benchmarks completed!")

        except Exception as e:
            error_handler.handle_error(e, "Benchmark execution")

    def demonstrate_bigdata(self):
        """Демонстрация BigData возможностей"""
        print("💾 Demonstrating BigData capabilities...")

        try:
            processor = BigDataProcessor(
                chunk_size=self.config.get('bigdata', {}).get('chunk_size', 100000)
            )
            processor.demonstrate_2gb_capability()

            print("✅ BigData demonstration completed!")

        except Exception as e:
            error_handler.handle_error(e, "BigData demonstration")

    def show_system_info(self):
        """Показать информацию о системе"""
        print("\n" + "=" * 60)
        print("SERVER LOAD ANALYSIS SYSTEM")
        print("=" * 60)
        print(f"Version: {self.config.get('app', {}).get('version', '1.0.0')}")
        print(f"Configuration: {self.config_path}")

        # Информация о путях
        paths = self.config.get('paths', {})
        print("\nPaths:")
        for key, path in paths.items():
            exists = "✓" if os.path.exists(path) else "✗"
            print(f"  {exists} {key}: {path}")

        print("\nAvailable commands:")
        print("  --full-pipeline    Run complete analysis pipeline")
        print("  --benchmarks       Run performance benchmarks")
        print("  --bigdata          Demonstrate BigData capabilities")
        print("  --analyze          Run data analysis only")
        print("  --generate-data    Generate synthetic data")
        print("  --help             Show this help message")
        print("=" * 60)


def main():
    """Точка входа приложения"""
    parser = argparse.ArgumentParser(
        description='Server Load Analysis System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --full-pipeline    # Run complete analysis
  python main.py --benchmarks       # Performance testing
  python main.py --bigdata          # BigData capabilities demo
  python main.py --analyze          # Data analysis only
        """
    )

    parser.add_argument('--full-pipeline', action='store_true',
                        help='Run complete analysis pipeline')
    parser.add_argument('--benchmarks', action='store_true',
                        help='Run performance benchmarks')
    parser.add_argument('--bigdata', action='store_true',
                        help='Demonstrate BigData capabilities')
    parser.add_argument('--analyze', action='store_true',
                        help='Run data analysis only')
    parser.add_argument('--generate-data', action='store_true',
                        help='Generate synthetic data')
    parser.add_argument('--config', type=str, default='config/config.yaml',
                        help='Path to configuration file')
    parser.add_argument('--version', action='store_true',
                        help='Show version information')

    args = parser.parse_args()

    # Создаем экземпляр приложения
    analyzer = ServerLoadAnalyzer(args.config)

    # Показать информацию о системе если нет аргументов
    if not any(vars(args).values()):
        analyzer.show_system_info()
        return

    # Обработка аргументов
    if args.version:
        print(f"Server Load Analysis System v{analyzer.config.get('app', {}).get('version', '1.0.0')}")
        return

    if args.full_pipeline:
        analyzer.run_full_pipeline()

    if args.benchmarks:
        analyzer.run_benchmarks()

    if args.bigdata:
        analyzer.demonstrate_bigdata()

    if args.analyze:
        print("📊 Running data analysis...")
        load_and_analyze_metrics()
        analyze_access_logs()

    if args.generate_data:
        print("📈 Generating synthetic data...")
        generate_synthetic_metrics()
        generate_realistic_logs()


if __name__ == "__main__":
    main()