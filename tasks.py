import os
import json
import logging
import statistics
from threading import Lock
from multiprocessing.queues import Queue
from typing import Optional

from external.analyzer import analyze_json
from external.client import YandexWeatherAPI
from utils import get_url_by_city_name
from data import DaysData, CityData
from exceptions import InvalidResponseException, EmptyResponseException


logger = logging.getLogger()


class DataFetchingTask:
    ya_api = YandexWeatherAPI()

    def __init__(self, city: str) -> None:
        self._city: str = city
        self._data: dict = {}

    def run(self) -> tuple[str, dict] | None:
        try:
             fetching_data = self._get_from_api()
             logger.info('Data for %s fetched is OK.', self._city)
             return fetching_data
        except Exception as e:
            logger.error('Invalid fetching data for %s %s', self._city, e)
            pass

    def _get_from_api(self) -> tuple[str, dict] | None:
        url_with_data = get_url_by_city_name(self._city)
        self._data = self.ya_api.get_forecasting(url_with_data)
        if self._data is None:
            logger.error('Fetching data for %s is None',self._city)
            raise EmptyResponseException()
        if 'forecasts' not in self._data:
            logger.error('Invalid Response for %s is None', self._city)
            raise InvalidResponseException()

        return self._city, self._data

    @property
    def get_data(self) -> tuple[str, dict] | None:
        return self._city, self._data



class DataCalculationTask:
    def __init__(self, city_name: str, data: dict, queue: Optional[Queue] = None) -> None:
        self._data = data
        self._city_name = city_name
        self._queue = queue
        self._result_data: dict | None = None

    def run(self) -> None:
        self._analyze_data()
        logger.info('Analyze data for %s is OK.', self._city_name)
        if self._result_data:
            city_data = CityData(self._city_name, self._get_av_temp(), self._get_no_precipitation(), self._get_days())
            logger.info('Calculate av_temp and relevant_cond_hours for %s calculate is OK.', self._city_name)
            if self._queue:
                self._queue.put(city_data)

    def _analyze_data(self) -> None:
        self._result_data = analyze_json(self._data)

    def _get_av_temp(self) -> float | None:
        list_temp = [day.get('temp_avg') for day in self._result_data.get('days') if day.get('temp_avg')]
        return round(statistics.mean(list_temp), 1)

    def _get_days(self) -> list[DaysData] | None:
        return [DaysData(item['date'], item['temp_avg'], item['relevant_cond_hours']) for item in self._result_data['days']]

    def _get_no_precipitation(self) -> int | None:
        list_precip = [day.get('relevant_cond_hours') for day in self._result_data.get('days') if day.get('relevant_cond_hours') is not None]
        return sum(list_precip)


class DataAggregationTask:

    RESULT_PATH = os.path.join(os.path.dirname(os.path.dirname(__name__)), 'output.json')

    def __init__(self, data: CityData, lock: Optional[Lock] = None) -> None:
        self._data = data
        self._lock = lock

    def run(self) -> None:
        self._write_updates_data()
        logger.info('Write updates to json is OK.')

    @classmethod
    def make_empty_json_file(cls) -> None:
        if not os.path.exists(cls.RESULT_PATH):
            with open(cls.RESULT_PATH, 'w+') as file:
                file.write(json.dumps({}))

    def _write_updates_data(self) -> None:
        with self._lock:
            with open(DataAggregationTask.RESULT_PATH, 'r') as file:
                exist_dict = json.load(file)
            with open(DataAggregationTask.RESULT_PATH, 'w') as file:
                result_dict = exist_dict|self._data.get_dict
                file.write(json.dumps(dict(sorted(result_dict.items(), key=lambda item: (-item[1]['temp_avg'], -item[1]['relevant_cond_hours'])))))

    @classmethod
    def get_result(cls) -> dict | None:
        if os.path.exists(cls.RESULT_PATH):
            with open(cls.RESULT_PATH, 'r') as file:
                return json.load(file)


class DataAnalyzingTask:
    def __init__(self, data: dict) -> None:
        self._data = data

    def run(self) -> list[dict]:
        cities = list(self._data.keys())
        list_best_cities = []
        best_city = {cities[0]: self._data.pop(cities[0])}
        list_best_cities.append(best_city)
        for city_name, city_data in self._data.items():
            if city_data['temp_avg'] == best_city[cities[0]]['temp_avg']:
                if city_data['relevant_cond_hours'] == best_city[cities[0]]['relevant_cond_hours']:
                    logger.info('Get best cities %s.', city_name)
                    list_best_cities.append({city_name: city_data})
                else:
                    break
        return list_best_cities