# import logging
# import threading
# import subprocess
# import multiprocessing

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
    forcast_data = DataFetchingTask().get_forcast_data()


if __name__ == "__main__":
    forecast_weather()
