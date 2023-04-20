from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue, cpu_count

from api_client import YandexWeatherAPI
from entities import DailyTemp, CityTemp, InitialForecast
from utils import CITIES, DRY_WEATHER


class DataFetchingTask(Process):
    def __init__(self, queue: Queue):
        super().__init__()
        self.queue = queue

    def get_city_forecasts(self, city_name):
        return city_name, YandexWeatherAPI().get_forecasting(city_name=city_name)

    def run(self):
        with ThreadPoolExecutor(max_workers=cpu_count()) as pool:
            for city_forecast in pool.map(self.get_city_forecasts, CITIES.keys()):
                self.queue.put(InitialForecast(city=city_forecast[0], forecasts=city_forecast[1]['forecasts']))
            self.queue.put(None)


class DataCalculationTask(Process):
    def __init__(self, initial_data_queue: Queue):
        super().__init__()
        self.initial_data_queue = initial_data_queue

    @staticmethod
    def in_include_hours(hour):
        return bool(int(hour) >= 9 and int(hour) <= 19)

    def get_daily_avg_temp(self, daily_forecast):
        hours = daily_forecast['hours']
        temps = [hour['temp'] for hour in hours if self.in_include_hours(hour=hour['hour'])]
        return round((sum(temps) / len(temps)), 1) if temps else None

    def get_total_dry_hours(self, daily_forecast):
        hours = daily_forecast['hours']
        return sum(
            [
                1 for hour in hours
                if hour['condition'] in DRY_WEATHER and self.in_include_hours(hour=hour['hour'])
            ]
        )

    def calc_city_temp(self, city_forcecast_data: InitialForecast) -> CityTemp:
        daily_avg_temps = []
        with ThreadPoolExecutor(max_workers=cpu_count()) as pool:
            for daily_forecast in city_forcecast_data.forecasts:
                daily_avg_temp = pool.submit(self.get_daily_avg_temp, daily_forecast)
                total_dry_hours = pool.submit(self.get_total_dry_hours, daily_forecast)
                daily_avg_temps.append(
                    DailyTemp(
                        date=daily_forecast['date'],
                        avg_temp=daily_avg_temp.result(),
                        total_dry_hours=total_dry_hours.result(),
                    )
                )

        return CityTemp(
            city=city_forcecast_data.city,
            daily_avg_temps=daily_avg_temps,
        )

    def run(self):
        while city_forcecast_data := self.initial_data_queue.get():
            self.calc_city_temp(city_forcecast_data=city_forcecast_data)


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass
