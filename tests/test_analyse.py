import os.path
from unittest import TestCase

import pandas as pd

from analyse import JourneyHistory

TESTDATA_DIR_INPUT = os.path.join('testdata', 'input')
TESTDATA_DIR_OUTPUT = os.path.join('testdata', 'output')


class TestAnalyse(TestCase):
    def setUp(self) -> None:
        self.history_files = ['testdata/input/export1.csv', 'testdata/input/export2.csv']

    def test_initialise_success(self):
        """ Test for initialising an instance of the JourneyHistory class. """
        analyser1 = JourneyHistory(self.history_files)
        analyser2 = JourneyHistory(history_dir=TESTDATA_DIR_INPUT)

        # Initialising from a list of files or a directory should have the same dataframe created
        pd.testing.assert_frame_equal(analyser1.df, analyser2.df)

    def test_initialise_failure_from_too_many_args(self):
        """ Test for incorrectly trying to initialise an instance of the JourneyHistory class by providing both the
        list of history files and the history directory """
        # Providing both args should raise a ValueError (only pass one)
        expected_error_msg = 'Only provide either the list of journey history files or the directory containing the ' \
                             'history files, but not both'
        with self.assertRaisesRegex(ValueError, expected_error_msg):
            JourneyHistory(self.history_files, TESTDATA_DIR_INPUT)
            JourneyHistory(self.history_files, history_dir=TESTDATA_DIR_INPUT)
            JourneyHistory(history_files=self.history_files, history_dir=TESTDATA_DIR_INPUT)


class TestAnalyseStats(TestCase):
    def setUp(self) -> None:
        self.analyser = JourneyHistory(history_dir=TESTDATA_DIR_INPUT)
        self.history_files = ['testdata/input/export1.csv', 'testdata/input/export2.csv']
        self.df = self.analyser.df

    def test_load_history_csvs(self):
        """ Test for the DataFrame produced after processing. """
        # Load the expected df and fix dtypes as the data type info is lost in the writing to and reading from CSV process
        expected_df = pd.read_csv(os.path.join(TESTDATA_DIR_OUTPUT, 'test_output1.csv'), parse_dates=[0, 1, 2],
                                  dayfirst=True)
        expected_df.iloc[:, 2] = pd.to_timedelta(expected_df.iloc[:, 2])

        pd.testing.assert_frame_equal(self.df, expected_df)

    def test_get_summary_stats(self):
        summary_stats = self.analyser.get_summary_stats()

        expected_summary_stats = {'total_journey_time': pd.Timedelta(hours=18, minutes=30),
                                  'total_fare_expense': 99.9}
        assert summary_stats == expected_summary_stats

    def test_get_top_origin_stations(self):
        expected_top_5_origin_stations_dict = {'freq': {'Limehouse DLR': 19,
                                                        'Oxford Circus': 8,
                                                        'Tottenham Court Road': 5,
                                                        'Tower Gateway DLR': 2,
                                                        'Borough': 1,
                                                        'East India DLR': 1,
                                                        'Charing Cross [London Underground]': 1,
                                                        'Camden Town': 1,
                                                        'Surrey Quays [London Overground]': 1,
                                                        'Canary Wharf DLR': 1,
                                                        'Leicester Square': 1,
                                                        'Mornington Crescent': 1,
                                                        'Chalk Farm': 1,
                                                        'Canning Town': 1,
                                                        'Knightsbridge': 1,
                                                        'Liverpool Street [London Underground]': 1}}
        assert self.analyser.get_top_origin_stations().to_dict() == expected_top_5_origin_stations_dict

    def test_get_top_destination_stations(self):
        expected_top_5_destination_stations_dict = {'freq': {'Camden Town': 2,
                                                             'Canning Town': 4,
                                                             'Limehouse DLR': 19,
                                                             'Oxford Circus': 7,
                                                             'Tottenham Court Road': 6}}
        assert self.analyser.get_top_destination_stations().to_dict() == expected_top_5_destination_stations_dict
