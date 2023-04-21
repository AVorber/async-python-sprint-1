import csv
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import reduce
from multiprocessing import Process, Queue
from typing import Any

from api_client import YandexWeatherAPI
from entities import DailyTemp, CityTemp, InitialForecast
from utils import DRY_WEATHER, FILE_NAME


logger = logging.getLogger(__name__)


class DataFetchingTask(Process):
    def __init__(self, *, api: YandexWeatherAPI, fetch_data_queue: Queue, cities: dict[str, Any]):
        super().__init__()
        self.api = api
        self.fetch_data_queue = fetch_data_queue
        self.cities = cities

    def get_city_forecasts(self, city_name: str) -> tuple[str, dict[str, Any]]:
        try:
            result = self.api.get_forecasting(city_name=city_name)
        except Exception as error:
            logging.exception(f'Failed to fetch data from api: {error}')
            raise error

        return city_name, result

    def run(self) -> None:
        with ThreadPoolExecutor() as pool:
            for city_forecast in pool.map(self.get_city_forecasts, self.cities.keys()):
                self.fetch_data_queue.put(
                    InitialForecast(city=city_forecast[0], forecasts=city_forecast[1]['forecasts'])
                )
            self.fetch_data_queue.put(None)


class DataCalculationTask(Process):
    def __init__(self, *, fetch_data_queue: Queue, aggregate_data_queue: Queue):
        super().__init__()
        self.fetch_data_queue = fetch_data_queue
        self.aggregate_data_queue = aggregate_data_queue

    @staticmethod
    def in_include_hours(hour) -> bool:
        return bool(int(hour) >= 9 and int(hour) <= 19)

    def get_daily_avg_temp(self, daily_forecast: dict[str, Any]) -> float | None:
        hours = daily_forecast['hours']
        temps = [hour['temp'] for hour in hours if self.in_include_hours(hour=hour['hour'])]

        return round((sum(temps) / len(temps))) if temps else None

    def get_total_dry_hours(self, daily_forecast: dict[str, Any]) -> int:
        hours = daily_forecast['hours']
        dry_hours = 0
        for hour in hours:
            if hour['condition'] in DRY_WEATHER and self.in_include_hours(hour=hour['hour']):
                dry_hours += 1

        return dry_hours

    def calc_city_temp(self, city_forcecast_data: InitialForecast) -> CityTemp:
        daily_avg_temps: list[DailyTemp] = []
        with ThreadPoolExecutor() as pool:
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

    def run(self) -> None:
        while city_forcecast_data := self.fetch_data_queue.get():
            self.aggregate_data_queue.put(self.calc_city_temp(city_forcecast_data=city_forcecast_data))
        self.aggregate_data_queue.put(None)


class DataAggregationTask(Process):
    def __init__(self, aggregate_data_queue: Queue, analyz_data_queue: Queue):
        super().__init__()
        self.aggregate_data_queue = aggregate_data_queue
        self.analyz_data_queue = analyz_data_queue

    def agregate_forcecast(self, forcecast_data: CityTemp) -> dict[str, Any]:
        df = {}
        avg_temp_list = []
        avg_dry_hours_list = []
        df['Город'] = forcecast_data.city
        df[''] = 'Температура, среднее / Без осадков, часов'
        for item in forcecast_data.daily_avg_temps:
            date = item.date
            date_avg_temp = item.avg_temp
            date_total_dry_hours = item.total_dry_hours
            df[date] = f'{date_avg_temp}/{date_total_dry_hours}'

            if date_avg_temp is not None:
                avg_temp_list.append(date_avg_temp)
                avg_dry_hours_list.append(date_total_dry_hours)

        avg_temp = round(reduce(lambda a, b: a + b, avg_temp_list) / len(avg_temp_list), 1)
        avg_dry_hours = round(reduce(lambda a, b: a + b, avg_dry_hours_list) / len(avg_dry_hours_list))
        df['Среднее'] = f'{avg_temp}/{avg_dry_hours}'

        return df

    def run(self) -> None:
        df_list = []
        while city_forcecast_calc_data := self.aggregate_data_queue.get():
            df_list.append(self.agregate_forcecast(forcecast_data=city_forcecast_calc_data))

        with open(FILE_NAME, 'w') as file:
            writer = csv.DictWriter(file, delimiter=';', fieldnames=[*df_list[0]])
            writer.writeheader()
            writer.writerows(df_list)

        self.analyz_data_queue.put(FILE_NAME)


class DataAnalyzingTask(Process):
    def __init__(self, analyz_data_queue: Queue):
        super().__init__()
        self.analyz_data_queue = analyz_data_queue

    def get_rating(self, row: dict[str, Any]) -> int:
        params = [float(i) for i in row['Среднее'].split('/')]
        return round(params[0] * params[1])

    def run(self) -> None:
        df_list = []
        raitings = []
        if aggregated_data_file_name := self.analyz_data_queue.get():
            with open(aggregated_data_file_name, 'r') as file:
                reader = csv.DictReader(file, delimiter=';')
                for row in reader:
                    raiting = self.get_rating(row)
                    df_list.append(row | {'Рейтинг': raiting})
                    raitings.append(raiting)

            with open(aggregated_data_file_name, 'w') as file:
                writer = csv.DictWriter(file, delimiter=';', fieldnames=[*df_list[0]])
                writer.writeheader()
                writer.writerows(df_list)

            most_comfortable_cities = [df['Город'] for df in df_list if df['Рейтинг'] == max(raitings)]
            if len(most_comfortable_cities) == 1:
                msg = f'Самый комфортный город - {most_comfortable_cities[0]}'
            else:
                msg = f'Самые комфортные города - {", ".join(most_comfortable_cities)}'
            logger.info(msg)
