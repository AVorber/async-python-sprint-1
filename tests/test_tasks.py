from multiprocessing.queues import Queue

import pytest as pytest

from api_client import YandexWeatherAPI
from tasks import DataFetchingTask, DataCalculationTask


class TestDataFetchingTask:
    def test_get_city_forecasts(self, mocker):
        # arrange
        task = DataFetchingTask(api=YandexWeatherAPI, fetch_data_queue=Queue, cities={'foo': 'bar'})
        get_forecasting_mock = mocker.patch.object(
            YandexWeatherAPI,
            'get_forecasting',
            return_value={'forecast': [{'item': 1}]},
        )
        expected_result = ('foo', {'forecast': [{'item': 1}]})

        # act
        result = task.get_city_forecasts('foo')

        # assert
        assert result == expected_result
        get_forecasting_mock.assert_called_once_with(city_name='foo')

    def test_get_city_forecasts__fetch_failed__raise(self, mocker, caplog):
        # arrange
        task = DataFetchingTask(api=YandexWeatherAPI, fetch_data_queue=Queue, cities={'foo': 'bar'})
        get_forecasting_mock = mocker.patch.object(
            YandexWeatherAPI,
            'get_forecasting',
            side_effect=Exception,
        )

        # act & assert
        with pytest.raises(Exception):
            task.get_city_forecasts('foo')

        # assert
        get_forecasting_mock.assert_called_once_with(city_name='foo')
        assert 'Failed to fetch data from api' in caplog.text


@pytest.mark.parametrize(
    'hour, temp, expected_result',
    [
        (12, 10, 10),
        (1, 10, None),
    ]
)
class TestDataCalculationTask:
    def test_get_daily_avg_temp(self, hour, temp, expected_result):
        # arrange
        task = DataCalculationTask(fetch_data_queue=Queue, aggregate_data_queue=Queue)

        # act
        result = task.get_daily_avg_temp(daily_forecast={'hours': [{'hour': hour, 'temp': temp}]})

        # assert
        assert result == expected_result
