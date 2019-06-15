import os.path
import datetime as dt
from typing import List

import pandas as pd

TESTDATA_DIR = os.path.join('testdata')


class JourneyHistory:
    def __init__(self, history_folder: str):
        assert os.path.exists(history_folder), 'Journey history folder does not exist: {}'.format(history_folder)
        self.history_folder = history_folder

        # List of filepaths for all CSVs in `self.history_folder`
        history_files = [os.path.join(self.history_folder, f) for f in os.listdir(self.history_folder) if f.endswith('.csv')]

        self.df = self.load_history_csvs(history_files)

    def __len__(self):
        """ Number of total rows of the dataframe """
        return len(self.df)

    def __repr__(self):
        return 'JourneyHistory(history_folder={!r})'.format(self.history_folder)

    def __getitem__(self, item):
        if item >= len(self):
            raise IndexError('Index out of range of number of DataFrame rows')
        return self.df.iloc[item]

    @staticmethod
    def load_history_csvs(csv_filepaths: List[str]) -> pd.DataFrame:
        """ For a given list of filename, load the CSVs into one dataframe."""
        individual_history_dfs = []
        # Use to validate CSV file as a journey history file
        expected_columns = ['Date', 'Start Time', 'End Time', 'Journey/Action', 'Charge', 'Credit', 'Balance', 'Note']
        for csv_file in csv_filepaths:
            df = pd.read_csv(csv_file)
            if df.columns.tolist() == expected_columns:
                individual_history_dfs.append(df)

        # Join all the individual dfs into one big df
        df = pd.concat(individual_history_dfs)
        df = df.sort_values('Date')

        # Drop all rows with no end time (bus journeys and top ups)
        # todo - support bus journeys
        df = df[pd.notnull(df['End Time'])]

        df['Start Time'] = pd.to_datetime(df['Date'] + ' ' + df['Start Time'])
        df['End Time'] = pd.to_datetime(df['Date'] + ' ' + df['End Time'])

        # Add 1 day to journeys whose end times go into the next day
        df.loc[df['End Time'] < df['Start Time'], 'End Time'] += dt.timedelta(days=1)

        # Get journey time column
        df['Journey time'] = df['End Time'] - df['Start Time']

        # Get the origin and destination columns
        df['From'] = df['Journey/Action'].str.split(' to ').str[0]
        df['To'] = df['Journey/Action'].str.split(' to ').str[1]
        df = df[pd.notnull(df['From']) & pd.notnull(df['To'])]

        final_columns = ['Start Time', 'End Time', 'From', 'To', 'Journey time', 'Charge', 'Note']
        df = df[final_columns].reset_index().drop('index', axis=1)

        return df
