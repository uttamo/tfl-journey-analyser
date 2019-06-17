import os.path
from unittest import TestCase

import pandas as pd

from analyse import JourneyHistory

TESTDATA_DIR = 'testdata'


class TestAnalyse(TestCase):
    def test_load_history_csvs(self):
        analyser = JourneyHistory(TESTDATA_DIR)
        history_df = analyser.df

        # Load the expected df with the correct dtypes
        expected_df = pd.read_csv(os.path.join(TESTDATA_DIR, 'test_output1.csv'),
                                  parse_dates=[0, 1], dayfirst=True)

        pd.testing.assert_frame_equal(history_df, expected_df)
