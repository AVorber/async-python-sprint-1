# import logging
# import threading
# import subprocess
import multiprocessing

from api_client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    # city_name = "MOSCOW"
    # ywAPI = YandexWeatherAPI()
    # resp = ywAPI.get_forecasting(city_name)
    initial_data_queue = multiprocessing.Queue()
    producer = DataFetchingTask(queue=initial_data_queue)
    consumer = DataCalculationTask(initial_data_queue=initial_data_queue)
    producer.start()
    consumer.start()
    producer.join()
    print('Process - Calculation Data completed')
    consumer.join()
    print('Process - Agregation Data completed')


if __name__ == "__main__":
    forecast_weather()
