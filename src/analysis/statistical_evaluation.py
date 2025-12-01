import pandas as pd
import numpy as np
import scipy.stats as stats
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple, Optional
import warnings

warnings.filterwarnings('ignore')


class StatisticalEvaluator:
    """
    Класс для статистической оценки качества данных и результатов анализа
    Включает оценку полноты, однородности, погрешности и доверительных интервалов
    """

    def __init__(self, confidence_level: float = 0.95):
        self.confidence_level = confidence_level
        self.results = {}

    def evaluate_data_completeness(self, df: pd.DataFrame, time_column: str = 'timestamp') -> Dict:
        """
        Оценка полноты временных рядов

        Параметры:
        df - DataFrame с данными
        time_column - колонка с временными метками

        Возвращает:
        Словарь с метриками полноты
        """
        print("📊 Evaluating data completeness...")

        # Конвертируем временные метки
        df = df.copy()
        df[time_column] = pd.to_datetime(df[time_column])
        df.set_index(time_column, inplace=True)

        completeness_metrics = {}

        # 1. Общая полнота по записям
        total_expected = self._calculate_expected_records(df)
        total_actual = len(df)
        completeness_pct = (total_actual / total_expected) * 100 if total_expected > 0 else 0

        completeness_metrics['total_expected'] = total_expected
        completeness_metrics['total_actual'] = total_actual
        completeness_metrics['completeness_percentage'] = round(completeness_pct, 2)

        # 2. Полнота по серверам
        if 'server_id' in df.columns:
            server_completeness = {}
            for server in df['server_id'].unique():
                server_data = df[df['server_id'] == server]
                server_expected = self._calculate_expected_records(server_data, is_subset=True)
                server_actual = len(server_data)
                server_pct = (server_actual / server_expected) * 100 if server_expected > 0 else 0
                server_completeness[server] = {
                    'expected': server_expected,
                    'actual': server_actual,
                    'completeness': round(server_pct, 2)
                }

            completeness_metrics['server_completeness'] = server_completeness

        # 3. Визуализация пропусков
        self._visualize_missing_data(df)

        print(f"✅ Data completeness: {completeness_pct:.1f}%")

        self.results['completeness'] = completeness_metrics
        return completeness_metrics

    def _calculate_expected_records(self, df: pd.DataFrame, is_subset: bool = False) -> int:
        """Расчет ожидаемого количества записей"""
        if len(df) < 2:
            return len(df)

        # Определяем временной интервал между записями
        time_diffs = df.index.to_series().diff().dropna()
        if len(time_diffs) == 0:
            return len(df)

        # Наиболее частый интервал (мода)
        mode_interval = time_diffs.mode()[0] if not time_diffs.mode().empty else time_diffs.iloc[0]

        # Общий временной промежуток
        time_span = df.index.max() - df.index.min()

        # Ожидаемое количество записей
        if mode_interval.total_seconds() > 0:
            expected = (time_span.total_seconds() / mode_interval.total_seconds()) + 1
        else:
            expected = len(df)

        return int(expected)

    def evaluate_sample_homogeneity(self, df: pd.DataFrame, metric_column: str = 'cpu_usage') -> Dict:
        """
        Оценка однородности выборок с использованием статистических тестов

        Параметры:
        df - DataFrame с данными
        metric_column - колонка для анализа

        Возвращает:
        Словарь с результатами тестов на однородность
        """
        print("🔍 Evaluating sample homogeneity...")

        homogeneity_metrics = {}

        if 'server_id' not in df.columns or metric_column not in df.columns:
            print("❌ Required columns missing for homogeneity test")
            return homogeneity_metrics

        servers = df['server_id'].unique()
        if len(servers) < 2:
            print("❌ Need at least 2 servers for homogeneity test")
            return homogeneity_metrics

        # 1. Тест ANOVA для сравнения средних
        groups = [df[df['server_id'] == server][metric_column].dropna().values
                  for server in servers]

        try:
            # Проверка на нормальность (тест Шапиро-Уилка)
            normality_results = {}
            for i, server in enumerate(servers):
                if len(groups[i]) >= 3 and len(groups[i]) <= 5000:
                    stat, p_value = stats.shapiro(groups[i])
                    normality_results[server] = {
                        'statistic': round(stat, 4),
                        'p_value': round(p_value, 4),
                        'is_normal': p_value > 0.05
                    }

            homogeneity_metrics['normality_test'] = normality_results

            # ANOVA если данные нормальные
            normal_groups = [normality_results.get(s, {}).get('is_normal', False)
                             for s in servers]

            if all(normal_groups) and len(groups) >= 2:
                f_stat, p_value = stats.f_oneway(*groups)
                homogeneity_metrics['anova'] = {
                    'f_statistic': round(f_stat, 4),
                    'p_value': round(p_value, 4),
                    'homogeneous': p_value > 0.05
                }
                print(f"   ANOVA: F={f_stat:.2f}, p={p_value:.4f}, "
                      f"Homogeneous: {p_value > 0.05}")

            # Непараметрический тест Крускала-Уоллиса
            h_stat, p_value = stats.kruskal(*[g for g in groups if len(g) > 0])
            homogeneity_metrics['kruskal_wallis'] = {
                'h_statistic': round(h_stat, 4),
                'p_value': round(p_value, 4),
                'homogeneous': p_value > 0.05
            }
            print(f"   Kruskal-Wallis: H={h_stat:.2f}, p={p_value:.4f}, "
                  f"Homogeneous: {p_value > 0.05}")

            # 2. Визуализация распределений
            self._visualize_distributions(df, metric_column)

        except Exception as e:
            print(f"❌ Error in homogeneity tests: {e}")

        self.results['homogeneity'] = homogeneity_metrics
        return homogeneity_metrics

    def calculate_confidence_intervals(self, df: pd.DataFrame,
                                       metric_columns: List[str] = None) -> Dict:
        """
        Расчет доверительных интервалов для метрик

        Параметры:
        df - DataFrame с данными
        metric_columns - список колонок для анализа

        Возвращает:
        Словарь с доверительными интервалами
        """
        print("🎯 Calculating confidence intervals...")

        if metric_columns is None:
            metric_columns = ['cpu_usage', 'memory_usage', 'response_time_ms', 'error_rate']

        confidence_intervals = {}

        for column in metric_columns:
            if column not in df.columns:
                continue

            data = df[column].dropna()
            if len(data) < 2:
                continue

            # Основные статистики
            mean = np.mean(data)
            std = np.std(data, ddof=1)  # Выборочное стандартное отклонение
            n = len(data)

            # Критическое значение t-распределения
            alpha = 1 - self.confidence_level
            t_critical = stats.t.ppf(1 - alpha / 2, df=n - 1)

            # Стандартная ошибка
            se = std / np.sqrt(n)

            # Доверительный интервал
            margin_of_error = t_critical * se
            ci_lower = mean - margin_of_error
            ci_upper = mean + margin_of_error

            # Относительная погрешность (%)
            relative_error = (margin_of_error / mean) * 100 if mean != 0 else 0

            confidence_intervals[column] = {
                'mean': round(mean, 4),
                'std_dev': round(std, 4),
                'sample_size': n,
                'confidence_level': self.confidence_level,
                't_critical': round(t_critical, 4),
                'standard_error': round(se, 4),
                'margin_of_error': round(margin_of_error, 4),
                'ci_lower': round(ci_lower, 4),
                'ci_upper': round(ci_upper, 4),
                'relative_error_percent': round(relative_error, 2)
            }

            print(f"   {column}: {mean:.2f} ± {margin_of_error:.2f} "
                  f"({ci_lower:.2f} - {ci_upper:.2f}) "
                  f"Error: {relative_error:.1f}%")

        # Визуализация доверительных интервалов
        self._visualize_confidence_intervals(confidence_intervals)

        self.results['confidence_intervals'] = confidence_intervals
        return confidence_intervals

    def calculate_error_metrics(self, predictions: np.ndarray,
                                actuals: np.ndarray) -> Dict:
        """
        Расчет метрик погрешности для моделей прогнозирования

        Параметры:
        predictions - предсказанные значения
        actuals - фактические значения

        Возвращает:
        Словарь с метриками погрешности
        """
        print("📈 Calculating error metrics...")

        if len(predictions) != len(actuals):
            raise ValueError("Predictions and actuals must have same length")

        # Убираем NaN значения
        mask = ~np.isnan(predictions) & ~np.isnan(actuals)
        predictions = predictions[mask]
        actuals = actuals[mask]

        if len(predictions) == 0:
            return {}

        error_metrics = {}

        # Абсолютные ошибки
        errors = predictions - actuals
        absolute_errors = np.abs(errors)

        # Основные метрики
        error_metrics['mae'] = np.mean(absolute_errors)  # MAE
        error_metrics['mse'] = np.mean(errors ** 2)  # MSE
        error_metrics['rmse'] = np.sqrt(error_metrics['mse'])  # RMSE

        # Средняя абсолютная процентная ошибка (MAPE)
        mape_mask = actuals != 0
        if np.any(mape_mask):
            mape = np.mean(np.abs(errors[mape_mask] / actuals[mape_mask])) * 100
            error_metrics['mape'] = mape

        # R-квадрат (коэффициент детерминации)
        ss_res = np.sum(errors ** 2)
        ss_tot = np.sum((actuals - np.mean(actuals)) ** 2)
        if ss_tot != 0:
            error_metrics['r_squared'] = 1 - (ss_res / ss_tot)

        # Статистика Дарбина-Ватсона (автокорреляция ошибок)
        if len(errors) > 1:
            dw = np.sum(np.diff(errors) ** 2) / np.sum(errors ** 2)
            error_metrics['durbin_watson'] = dw

        # Округление
        for key in error_metrics:
            error_metrics[key] = round(error_metrics[key], 4)

        print(f"   MAE: {error_metrics.get('mae', 0):.4f}, "
              f"RMSE: {error_metrics.get('rmse', 0):.4f}, "
              f"R²: {error_metrics.get('r_squared', 0):.4f}")

        self.results['error_metrics'] = error_metrics
        return error_metrics

    def _visualize_missing_data(self, df: pd.DataFrame):
        """Визуализация пропущенных данных"""
        plt.figure(figsize=(12, 6))

        # Heatmap пропусков
        if len(df) > 0:
            missing_data = df.isnull()
            sns.heatmap(missing_data, cbar=False, cmap='viridis')
            plt.title('Missing Data Pattern (Yellow = Missing)')
            plt.tight_layout()
            plt.savefig('reports/missing_data_heatmap.png', dpi=150, bbox_inches='tight')
            plt.close()

    def _visualize_distributions(self, df: pd.DataFrame, metric_column: str):
        """Визуализация распределений по серверам"""
        if 'server_id' not in df.columns:
            return

        plt.figure(figsize=(10, 6))

        servers = df['server_id'].unique()
        colors = plt.cm.Set3(np.linspace(0, 1, len(servers)))

        for i, server in enumerate(servers):
            server_data = df[df['server_id'] == server][metric_column].dropna()
            if len(server_data) > 0:
                # Гистограмма с ядерной оценкой плотности
                sns.histplot(server_data, kde=True, alpha=0.5,
                             label=server, color=colors[i], bins=20)

        plt.title(f'Distribution of {metric_column} by Server')
        plt.xlabel(metric_column)
        plt.ylabel('Frequency')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('reports/distribution_comparison.png', dpi=150, bbox_inches='tight')
        plt.close()

    def _visualize_confidence_intervals(self, confidence_intervals: Dict):
        """Визуализация доверительных интервалов"""
        if not confidence_intervals:
            return

        metrics = list(confidence_intervals.keys())
        means = [ci['mean'] for ci in confidence_intervals.values()]
        lowers = [ci['ci_lower'] for ci in confidence_intervals.values()]
        uppers = [ci['ci_upper'] for ci in confidence_intervals.values()]
        errors = [ci['margin_of_error'] for ci in confidence_intervals.values()]

        plt.figure(figsize=(10, 6))

        # Столбцы со средними значениями
        bars = plt.bar(metrics, means, yerr=errors, capsize=10,
                       alpha=0.7, color='skyblue', edgecolor='black')

        # Добавляем значения на столбцы
        for bar, mean_val in zip(bars, means):
            plt.text(bar.get_x() + bar.get_width() / 2., bar.get_height() + 0.1,
                     f'{mean_val:.2f}', ha='center', va='bottom', fontsize=9)

        plt.title(f'Confidence Intervals ({self.confidence_level * 100:.0f}% Confidence Level)')
        plt.ylabel('Metric Value')
        plt.grid(True, alpha=0.3, axis='y')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('reports/confidence_intervals.png', dpi=150, bbox_inches='tight')
        plt.close()

    def generate_statistical_report(self) -> str:
        """Генерация текстового отчета со статистическими выводами"""
        report_lines = []

        report_lines.append("=" * 60)
        report_lines.append("STATISTICAL EVALUATION REPORT")
        report_lines.append("=" * 60)

        # 1. Полнота данных
        if 'completeness' in self.results:
            comp = self.results['completeness']
            report_lines.append("\n1. DATA COMPLETNESS")
            report_lines.append("-" * 40)
            report_lines.append(f"Total expected records: {comp.get('total_expected', 0):,}")
            report_lines.append(f"Total actual records: {comp.get('total_actual', 0):,}")
            report_lines.append(f"Completeness: {comp.get('completeness_percentage', 0):.1f}%")

            if 'server_completeness' in comp:
                report_lines.append("\nCompleteness by server:")
                for server, stats in comp['server_completeness'].items():
                    report_lines.append(f"  {server}: {stats['completeness']}% "
                                        f"({stats['actual']}/{stats['expected']} records)")

        # 2. Однородность
        if 'homogeneity' in self.results:
            homo = self.results['homogeneity']
            report_lines.append("\n2. SAMPLE HOMOGENEITY")
            report_lines.append("-" * 40)

            if 'kruskal_wallis' in homo:
                kw = homo['kruskal_wallis']
                report_lines.append(f"Kruskal-Wallis Test:")
                report_lines.append(f"  H-statistic: {kw['h_statistic']}")
                report_lines.append(f"  p-value: {kw['p_value']}")
                report_lines.append(f"  Samples are homogeneous: {kw['homogeneous']}")

        # 3. Доверительные интервалы
        if 'confidence_intervals' in self.results:
            cis = self.results['confidence_intervals']
            report_lines.append("\n3. CONFIDENCE INTERVALS")
            report_lines.append("-" * 40)

            for metric, ci in cis.items():
                report_lines.append(f"{metric.upper()}:")
                report_lines.append(f"  Mean: {ci['mean']:.4f} ± {ci['margin_of_error']:.4f}")
                report_lines.append(f"  95% CI: [{ci['ci_lower']:.4f}, {ci['ci_upper']:.4f}]")
                report_lines.append(f"  Relative error: {ci['relative_error_percent']}%")
                report_lines.append(f"  Sample size: {ci['sample_size']}")

        # 4. Метрики погрешности
        if 'error_metrics' in self.results:
            errors = self.results['error_metrics']
            report_lines.append("\n4. ERROR METRICS")
            report_lines.append("-" * 40)

            for metric, value in errors.items():
                report_lines.append(f"  {metric.upper()}: {value}")

        report_lines.append("\n" + "=" * 60)
        report_lines.append("Formulas used:")
        report_lines.append("- Confidence Interval: x̄ ± t*(s/√n)")
        report_lines.append("- Standard Error: s/√n")
        report_lines.append("- Relative Error: (Margin of Error / Mean) * 100%")
        report_lines.append("- Kruskal-Wallis: Non-parametric test for homogeneity")
        report_lines.append("=" * 60)

        report = '\n'.join(report_lines)

        # Сохраняем отчет в файл
        with open('reports/statistical_evaluation.txt', 'w', encoding='utf-8') as f:
            f.write(report)

        print("✅ Statistical report generated: reports/statistical_evaluation.txt")

        return report


# Пример использования
def demonstrate_statistical_evaluation():
    """Демонстрация работы статистического оценщика"""
    print("🧪 Demonstrating statistical evaluation...")

    # Создаем тестовые данные
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', '2024-01-10', freq='1H')

    data = []
    for i, date in enumerate(dates):
        for server in ['web01', 'api01', 'db01']:
            # Добавляем немного пропусков для реалистичности
            if np.random.random() > 0.05:  # 5% пропусков
                data.append({
                    'timestamp': date,
                    'server_id': server,
                    'cpu_usage': np.random.normal(50, 15),
                    'memory_usage': np.random.normal(60, 10),
                    'response_time_ms': np.random.normal(100, 30),
                    'error_rate': np.random.exponential(1)
                })

    df = pd.DataFrame(data)

    # Создаем оценщик и запускаем анализ
    evaluator = StatisticalEvaluator(confidence_level=0.95)

    # 1. Оценка полноты
    completeness = evaluator.evaluate_data_completeness(df)

    # 2. Оценка однородности
    homogeneity = evaluator.evaluate_sample_homogeneity(df, 'cpu_usage')

    # 3. Доверительные интервалы
    confidence_intervals = evaluator.calculate_confidence_intervals(df)

    # 4. Генерация отчета
    report = evaluator.generate_statistical_report()

    print("\n📊 Key statistical insights for presentation:")
    print("1. Data completeness metrics and missing patterns")
    print("2. Statistical tests for sample homogeneity")
    print("3. Confidence intervals with error margins")
    print("4. Formulas: CI = x̄ ± t*(s/√n), R², MAE, RMSE")

    return evaluator


if __name__ == "__main__":
    evaluator = demonstrate_statistical_evaluation()