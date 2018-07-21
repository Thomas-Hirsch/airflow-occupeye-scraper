# -*- coding: utf-8 -*-

"""
Tests
"""

import unittest
from transform_data import strip_commas_from_api_response, sensor_fact_data_to_long_format

class MyTest(unittest.TestCase):
    """
    Test the add function
    """


    def test_scrape_date_in_surveydays(self):
        """
        Adding two numbers should give the correct answer
        """
        l1 = [{"a": "x,y", "b": "x,,y,z", "c": 1.2}]
        l2 = [{'a': 'x;y', 'b': 'x;;y;z', 'c': 1.2}]
        t1 = strip_commas_from_api_response(l1) == l2

        self.assertTrue(t1)

    def test_sensor_fact_data_to_long_format(self):

        data = {'CountOcc': None, 'CountTotal': 0, 'HardwareID': 890001, 'SensorID': 20890001, 'SurveyDeviceID': 127717, 'TriggerDate': '2018-04-25', 't0000': -1, 't0010': 1, 't0020': -1, 't0030': -1, 't0040': -1, 't0050': -1, 't0100': -1, 't0110': -1, 't0120': -1, 't0130': -1, 't0140': -1, 't0150': -1, 't0200': -1, 't0210': -1, 't0220': -1, 't0230': -1, 't0240': -1, 't0250': -1, 't0300': -1, 't0310': -1, 't0320': -1, 't0330': -1, 't0340': -1, 't0350': -1, 't0400': -1, 't0410': -1, 't0420': -1, 't0430': -1, 't0440': -1, 't0450': -1, 't0500': -1, 't0510': -1, 't0520': -1, 't0530': -1, 't0540': -1, 't0550': -1, 't0600': -1, 't0610': -1, 't0620': -1, 't0630': -1, 't0640': -1, 't0650': -1, 't0700': -1, 't0710': -1, 't0720': -1, 't0730': -1, 't0740': -1, 't0750': -1, 't0800': -1, 't0810': -1, 't0820': -1, 't0830': -1, 't0840': -1, 't0850': -1, 't0900': -1, 't0910': -1, 't0920': -1, 't0930': -1, 't0940': -1, 't0950': -1, 't1000': -1, 't1010': -1, 't1020': -1, 't1030': -1, 't1040': -1, 't1050': -1, 't1100': -1, 't1110': -1, 't1120': -1, 't1130': -1, 't1140': -1, 't1150': -1, 't1200': -1, 't1210': -1, 't1220': -1, 't1230': -1, 't1240': -1, 't1250': -1, 't1300': -1, 't1310': -1, 't1320': -1, 't1330': -1, 't1340': -1, 't1350': -1, 't1400': -1, 't1410': -1, 't1420': -1, 't1430': -1, 't1440': -1, 't1450': -1, 't1500': -1, 't1510': -1, 't1520': -1, 't1530': -1, 't1540': -1, 't1550': -1, 't1600': -1, 't1610': -1, 't1620': -1, 't1630': -1, 't1640': -1, 't1650': -1, 't1700': -1, 't1710': -1, 't1720': -1, 't1730': -1, 't1740': -1, 't1750': -1, 't1800': -1, 't1810': -1, 't1820': -1, 't1830': -1, 't1840': -1, 't1850': -1, 't1900': -1, 't1910': -1, 't1920': -1, 't1930': -1, 't1940': -1, 't1950': -1, 't2000': -1, 't2010': -1, 't2020': -1, 't2030': -1, 't2040': 1, 't2050': 0, 't2100': 0, 't2110': 0, 't2120': 0, 't2130': 0, 't2140': 0, 't2150': 0, 't2200': 0, 't2210': 0, 't2220': 0, 't2230': 0, 't2240': 0, 't2250': 0, 't2300': 0, 't2310': 0, 't2320': 0, 't2330': 0, 't2340': 0, 't2350': 0}
        long_format = sensor_fact_data_to_long_format(data)
        long_format_0 = {'SurveyDeviceID': 127717, 'sensor_value': -1, 'obs_datetime': '2018-04-25 00:00:00'}
        long_format_1 = {'SurveyDeviceID': 127717, 'sensor_value': 1, 'obs_datetime': '2018-04-25 00:10:00'}
        long_format_last = {'SurveyDeviceID': 127717, 'sensor_value': 0, 'obs_datetime': '2018-04-25 23:50:00'}
        self.assertTrue(long_format_0 == long_format[0])
        self.assertTrue(long_format_1 == long_format[1])
        self.assertTrue(long_format_last == long_format[-1])


if __name__ == '__main__':
    unittest.main()