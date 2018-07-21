# -*- coding: utf-8 -*-

"""
Tests
"""

import unittest
from time_utils import next_execution_is_in_future, scrape_date_in_surveydays
from dateutil.parser import parse
import datetime

class AddTest(unittest.TestCase):
    """
    Test the add function
    """

    def test_future_execution(self):
        """
        Adding two numbers should give the correct answer
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        nowplus = now + datetime.timedelta(seconds=10)
        nowminus = now - datetime.timedelta(seconds=10)

        self.assertFalse(next_execution_is_in_future(nowminus))

        self.assertTrue(next_execution_is_in_future(nowplus))

    def test_scrape_date_in_surveydays(self):
        """
        Adding two numbers should give the correct answer
        """
        survey =  {'Description1': 'Corporate Services',
                    'Description2': 'Directorate',
                    'Description3': 'Admin',
                    'Disabled': False,
                    'EndDate': '2017-11-10',
                    'EndTime': '17:00',
                    'Name': 'V | Notts Virtual',
                    'StartDate': '2017-10-30',
                    'StartTime': '09:00',
                    'SurveyID': 312,
                    'WorkWeek': 'Monday,Tuesday,Wednesday,Thursday,Friday',
                    'reportResolution': None,
                    'resolution': 600}

        f1 = scrape_date_in_surveydays("2017-10-29", survey)
        t1 = scrape_date_in_surveydays("2017-10-30", survey)
        t2 = scrape_date_in_surveydays("2017-11-01", survey)
        t3 = scrape_date_in_surveydays("2017-11-10", survey)
        f2 = scrape_date_in_surveydays("2017-11-11", survey)

        self.assertTrue(t1)
        self.assertTrue(t2)
        self.assertTrue(t3)
        self.assertFalse(f1)
        self.assertFalse(f2)

if __name__ == '__main__':
    unittest.main()

