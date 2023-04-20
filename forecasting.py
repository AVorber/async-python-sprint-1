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
    fetch_data_queue = multiprocessing.Queue()
    aggregate_data_queue = multiprocessing.Queue()
    analyz_data_queue = multiprocessing.Queue()

    data_fetching_producer = DataFetchingTask(fetch_data_queue=fetch_data_queue)
    data_calculation_consumer = DataCalculationTask(
        fetch_data_queue=fetch_data_queue,
        aggregate_data_queue=aggregate_data_queue,
    )
    data_aggregation_consumer = DataAggregationTask(
        aggregate_data_queue=aggregate_data_queue,
        analyz_data_queue=analyz_data_queue,
    )
    data_analyzing_consumer = DataAnalyzingTask(analyz_data_queue=analyz_data_queue)

    data_fetching_producer.start()
    print('DataFetchingTask start')
    data_calculation_consumer.start()
    print('DataCalculationTask start')
    data_aggregation_consumer.start()
    print('DataAggregationTask start')
    data_analyzing_consumer.start()
    print('DataAnalyzingTask start')

    data_fetching_producer.join()
    print('DataFetchingTask completed')
    data_calculation_consumer.join()
    print('DataCalculationTask completed')
    data_aggregation_consumer.join()
    print('DataAggregationTask completed')
    data_analyzing_consumer.join()
    print('DataAnalyzingTask completed')


if __name__ == "__main__":
    forecast_weather()
