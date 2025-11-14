import argparse
import sys
import os

# Добавляем корневую папку в путь Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Теперь импортируем наши модули
from connector.db_connector import DatabaseConnector
from scripts.generate_synthetic_metrics import generate_synthetic_metrics
from scripts.parse_access_log import parse_nginx_log, generate_sample_logs
from data_collection.real_logs_generator import generate_realistic_logs
from analysis.basic_analysis import load_and_analyze_metrics, analyze_access_logs


def main():
    parser = argparse.ArgumentParser(description='Server Load Analysis System')
    parser.add_argument('--generate-data', action='store_true', help='Generate synthetic data')
    parser.add_argument('--parse-logs', action='store_true', help='Parse access logs')
    parser.add_argument('--analyze', action='store_true', help='Run data analysis')
    parser.add_argument('--db-stats', action='store_true', help='Show database statistics')
    parser.add_argument('--realistic-logs', action='store_true', help='Generate realistic logs')
    parser.add_argument('--full-pipeline', action='store_true', help='Run full data pipeline')

    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        return

    print("🚀 Server Load Analysis System")
    print("=" * 50)

    # Полный пайплайн
    if args.full_pipeline:
        print("📊 Running full data pipeline...")
        generate_synthetic_metrics()
        generate_sample_logs()
        parse_nginx_log('data/sample_access.log', 'data/parsed_access_logs.csv')
        generate_realistic_logs()

        db = DatabaseConnector()
        db.insert_server_metrics('data/synthetic_metrics.csv')
        db.insert_access_logs('data/parsed_access_logs.csv')
        db.close()

        load_and_analyze_metrics()
        analyze_access_logs()
        return

    # Отдельные команды
    if args.generate_data:
        print("📈 Generating synthetic metrics...")
        generate_synthetic_metrics()

    if args.realistic_logs:
        print("🌐 Generating realistic web logs...")
        generate_realistic_logs()

    if args.parse_logs:
        print("📝 Parsing access logs...")
        generate_sample_logs()
        parse_nginx_log('data/sample_access.log', 'data/parsed_access_logs.csv')

    if args.analyze:
        print("🔍 Running data analysis...")
        load_and_analyze_metrics()
        analyze_access_logs()

    if args.db_stats:
        print("🗃️ Database statistics:")
        db = DatabaseConnector()
        stats = db.get_access_logs_stats()
        print(f"Total requests: {stats.get('total_requests', 0)}")
        print(f"Average response time: {stats.get('avg_response_time', 0)}ms")
        print(f"Error rate: {stats.get('error_rate', 0)}%")
        db.close()


if __name__ == "__main__":
    main()