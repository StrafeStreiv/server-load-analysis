"""
Универсальный обработчик ошибок и валидатор данных
"""

import logging
from typing import Any, Dict, Optional
import traceback
from datetime import datetime


class ErrorHandler:
    """Класс для централизованной обработки ошибок"""

    def __init__(self, log_file: str = 'logs/app.log'):
        self.log_file = log_file
        self.setup_logging()

    def setup_logging(self):
        """Настройка системы логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def handle_error(self, error: Exception, context: str = "",
                     raise_again: bool = False) -> Dict[str, Any]:
        """
        Обработка ошибки с логированием

        Параметры:
        error - исключение
        context - контекст где произошла ошибка
        raise_again - поднимать ли исключение снова

        Возвращает:
        Словарь с информацией об ошибке
        """
        error_info = {
            'timestamp': datetime.now().isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context,
            'traceback': traceback.format_exc()
        }

        # Логирование
        self.logger.error(f"Error in {context}: {error}")
        self.logger.debug(f"Traceback: {traceback.format_exc()}")

        # Можно добавить отправку в Sentry/Telegram и т.д.

        if raise_again:
            raise error

        return error_info

    def validate_dataframe(self, df, required_columns: list = None,
                           check_null: bool = True) -> Dict[str, Any]:
        """
        Валидация DataFrame

        Параметры:
        df - DataFrame для валидации
        required_columns - обязательные колонки
        check_null - проверять на NaN значения

        Возвращает:
        Словарь с результатами валидации
        """
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }

        if df is None:
            validation_result['is_valid'] = False
            validation_result['errors'].append("DataFrame is None")
            return validation_result

        # Проверка обязательных колонок
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                validation_result['is_valid'] = False
                validation_result['errors'].append(f"Missing columns: {missing_columns}")

        # Проверка на пустоту
        if len(df) == 0:
            validation_result['warnings'].append("DataFrame is empty")

        # Проверка на NaN
        if check_null and len(df) > 0:
            null_counts = df.isnull().sum()
            if null_counts.any():
                null_info = null_counts[null_counts > 0].to_dict()
                validation_result['warnings'].append(f"Null values found: {null_info}")

        return validation_result


# Глобальный экземпляр для использования во всем приложении
error_handler = ErrorHandler()