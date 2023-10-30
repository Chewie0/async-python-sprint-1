from logging import config
import multiprocessing
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue

from tasks import DataFetchingTask, DataCalculationTask, DataAggregationTask,DataAnalyzingTask
from utils import CITIES
from logging_conf import LOG_CONFIG



config.dictConfig(LOG_CONFIG)

def run_task(task):
    return task.run()


def analyzing(data):
    analize_task = DataAnalyzingTask(data)
    return analize_task.run()


def aggregation(data):
    lock = Lock()
    DataAggregationTask.make_empty_json_file()
    agg_list = (DataAggregationTask(data=item, lock=lock) for item in data)
    with ThreadPoolExecutor(max_workers=4) as pool:
        pool.map(run_task, agg_list)
    return DataAggregationTask.get_result()


def calculation(data):
    queue = Queue()
    process_list = []
    for city_name, city_data in data:
        calc = DataCalculationTask(city_name, data=city_data, queue=queue)
        process = Process(target=calc.run())
        process_list.append(process)
        process.start()

    [pr.join() for pr in process_list]
    return (queue.get() for _ in range(queue.qsize()))


def fetching():
    cities = CITIES.keys()
    fetch_list = (DataFetchingTask(city) for city in cities)
    with ThreadPoolExecutor(max_workers=4) as pool:
        fetch_tasks = pool.map(run_task, fetch_list)
    return filter(lambda item: item is not None, fetch_tasks)

def forecast_weather():
    fetching_data = fetching()
    calc_data = calculation(fetching_data)
    result_json = aggregation(calc_data)
    results = analyzing(result_json)
    for item in results:
        for key, value in item.items():
            print(f"Для поездки благоприятен город - {key}. Средняя температура: {value.get('temp_avg')}. Времени без осадков: {value.get('relevant_cond_hours')} часов.")


if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    forecast_weather()
   
