# import logging
# import threading
# import subprocess
import multiprocessing

from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    initial_data_queue = multiprocessing.Queue()
    producer = DataFetchingTask(queue=initial_data_queue)
    consumer = DataCalculationTask(initial_data_queue=initial_data_queue)
    producer.start()
    consumer.start()
    producer.join()
    print('DataFetchingTask completed')
    consumer.join()
    print('DataCalculationTask completed')


if __name__ == "__main__":
    forecast_weather()
