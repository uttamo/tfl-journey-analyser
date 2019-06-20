import os.path
from unittest import TestCase

import pandas as pd

from analyse import JourneyHistory

TESTDATA_DIR_INPUT = os.path.join('testdata', 'input')
TESTDATA_DIR_OUTPUT = os.path.join('testdata', 'output')


class TestAnalyse(TestCase):
    def test_load_history_csvs(self):
        analyser = JourneyHistory()
        history_df = analyser.load_history_from_dir(TESTDATA_DIR_INPUT)

        # Load the expected df with the correct dtypes
        expected_df = pd.read_csv(os.path.join(TESTDATA_DIR_OUTPUT, 'test_output1.csv'), parse_dates=[0, 1, 2],
                                  dayfirst=True)
        expected_df.iloc[:, 2] = pd.to_timedelta(expected_df.iloc[:, 2])

        pd.testing.assert_frame_equal(history_df, expected_df)
