import json
import os
import unittest
from threading import Lock

from tasks import DataFetchingTask, DataCalculationTask, DataAggregationTask, DataAnalyzingTask
from data import DaysData, CityData


class FetchingTest(unittest.TestCase):

    def test_normal_city(self):
        normal_city = "MOSCOW"
        sample_task = DataFetchingTask(normal_city)

        self.assertIsNotNone(sample_task.run(), msg="Test value is none.")
        self.assertIsNotNone(sample_task.get_data[1], msg="Should be not None")
        self.assertIsInstance(sample_task.get_data[1], dict, msg="Should be dict")

    def test_bad_city(self):
        bad_city = "12345ghh"
        sample_task = DataFetchingTask(bad_city)

        self.assertIsNone(sample_task.run(), msg="Test value is not none.")
        self.assertEqual(sample_task.get_data[1], {}, msg="")


class CalculationTest(unittest.TestCase):

    def test_calc_empty_data(self):
        city_name = 'MOSCOW'
        city_data = {}
        calc = DataCalculationTask(city_name, data=city_data)

        self.assertIsNone(calc.run(), msg="Test value is not none.")

    def test_calc_with_data(self):
        path = './examples/response.json'
        with open(path, 'r') as f:
            data = json.load(f)
        city_name = 'MOSCOW'
        city_data = data
        calc = DataCalculationTask(city_name, data=city_data)

        self.assertIsNone(calc.run(), msg="Should be not None")
        self.assertIsInstance(calc._get_days(), list, msg="Should be list")
        self.assertTrue(-100 < calc._get_av_temp() < 100)
        self.assertIsInstance(calc._get_no_precipitation(), int)


class AggregationTest(unittest.TestCase):

    def test_calc_make_file_if_not_exist(self):
        result_path = os.path.join(os.path.dirname(os.path.dirname(__name__)), 'output.json')
        city_data = CityData(name='test', temp_avg=20000, relevant_cond_hours=30, days=[])
        agg = DataAggregationTask(data=city_data)

        if not os.path.exists(result_path):
            self.assertNotIsInstance(agg.get_result(), dict)
            agg.make_empty_json_file()
            self.assertEqual(agg.get_result(), {})

    def test_calc_return_data(self):
        lock = Lock()
        city_data = CityData(name='test', temp_avg=20000, relevant_cond_hours=30, days=[DaysData('111', 4.5, 44)])
        agg = DataAggregationTask(data=city_data, lock=lock)
        agg.run()

        self.assertIsInstance(agg._data.days[0], DaysData)
        self.assertTrue(len(agg.get_result()) > 0)

    def test_get_city_name(self):
        city_name = 'MOSCOW'
        temp_avg = 20
        lock = Lock()
        city_data = CityData(name=city_name, temp_avg=temp_avg, relevant_cond_hours=30, days=[DaysData('111', 4.5, 44)])
        agg = DataAggregationTask(data=city_data, lock=lock)
        agg.run()
        result = agg.get_result()

        self.assertTrue(city_name in result.keys())
        self.assertEqual(result[city_name]['temp_avg'], temp_avg)


class AnalyzingTest(unittest.TestCase):

    def test_empty_data(self):
        empty_dict = {}
        analyze_task = DataAnalyzingTask(empty_dict)

        with self.assertRaises(IndexError):
            analyze_task.run()

    def test_not_empty_data(self):
        empty_dict = {"Moscow":
            {
                "temp_avg": 13.091,
                "relevant_cond_hours": 11
            }
        }
        analyze_task = DataAnalyzingTask(empty_dict)
        self.assertIsInstance(analyze_task.run(), list)


if __name__ == '__main__':
    unittest.main()
