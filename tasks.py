from concurrent.futures import ThreadPoolExecutor

from api_client import YandexWeatherAPI
from utils import CITIES


class DataFetchingTask:
    def get_forcast_data(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            return list(pool.map(YandexWeatherAPI().get_forecasting, CITIES.keys()))


class DataCalculationTask:
    pass


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass
