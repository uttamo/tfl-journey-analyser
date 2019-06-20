import os.path
import datetime as dt

import pandas as pd
import numpy as np

TESTDATA_DIR = os.path.join('testdata')


class Journey:
    def __init__(self, start_time, end_time, origin, destination, charge, note):
        self.start_time = start_time
        self.end_time = end_time
        self.origin = origin
        self.destination = destination
        self.charge = charge
        self.note = note

        if self.end_time:
            self.journey_time = self.end_time - self.start_time
        else:
            self.journey_time = None

    def __repr__(self):
        return 'Journey(start_time={!r}, end_time={!r}, origin={!r}, destination={!r}, journey_time={!r}, ' \
               'charge={!r}, note={!r})'.format(self.start_time, self.end_time, self.origin, self.destination,
                                                self.journey_time, self.charge, self.note)

    def __lt__(self, other):
        if not (self.journey_time or other.journey_time):
            return False
        return self.journey_time < other.journey_time

    def __gt__(self, other):
        if not (self.journey_time or other.journey_time):
            return False
        return self.journey_time > other.journey_time


class JourneyHistory:
    def __init__(self, history_folder: str = None):
        if history_folder is not None:
            assert os.path.exists(history_folder), 'Journey history folder does not exist: {}'.format(history_folder)
            self.history_folder = history_folder
        else:
            self.history_folder = None

        self.raw_dfs = {}
        self.df = None

    def __len__(self):
        """ Number of total rows of the dataframe """
        return len(self.df)

    def __repr__(self):
        return 'JourneyHistory(history_folder={!r}, rows={})'.format(self.history_folder, len(self))

    def __getitem__(self, item):
        if item >= len(self):
            raise IndexError('Index out of range of number of DataFrame rows')
        return self.df.iloc[item]

    def load_history_from_dir(self, history_folder: str) -> pd.DataFrame:
        """ For a given list of filename, load the CSVs into one dataframe."""
        # List of filepaths for all CSVs in `history_folder`
        csv_filepaths = [os.path.join(history_folder, f) for f in os.listdir(history_folder) if f.endswith('.csv')]

        individual_history_dfs = []
        # Use to validate CSV file as a journey history file
        expected_columns = ['Date', 'Start Time', 'End Time', 'Journey/Action', 'Charge', 'Credit', 'Balance', 'Note']
        for csv_file in csv_filepaths:
            df = pd.read_csv(csv_file)
            if df.columns.tolist() == expected_columns:  # having the correct headers is the condition for a valid file
                self.raw_dfs[csv_file] = df
                individual_history_dfs.append(df)

        if len(individual_history_dfs) == 0:
            return pd.DataFrame()

        # Join all the individual dfs into one big df
        df = pd.concat(individual_history_dfs)

        # Initialise empty `Bus Journeys` columns that will be filled
        df['Bus Route'] = np.nan
        df = df.sort_values('Date').reset_index().drop('index', axis=1)

        # Processing of dates and times (mainly combining)
        df['Start Time'] = pd.to_datetime(df['Date'] + ' ' + df['Start Time'])
        df['End Time'] = pd.to_datetime(df['Date'] + ' ' + df['End Time'])

        # Add 1 day to journeys whose end times go into the next day
        df.loc[df['End Time'] < df['Start Time'], 'End Time'] += dt.timedelta(days=1)

        # Get the origin and destination columns
        df['From'] = df['Journey/Action'].str.split(' to ').str[0]
        df['To'] = df['Journey/Action'].str.split(' to ').str[1]

        # Filter out unwanted rows
        # todo - find better way of chaining these ORs
        df = df[~(df['To'].astype(str).str.contains("No touch-out") |
                  df['Journey/Action'].str.contains('Oyster helpline refund') |
                  df['Journey/Action'].str.contains('Auto top-up') |
                  df['Journey/Action'].str.contains('Topped-up on touch in'))]

        # Bus journeys
        bus_journeys = df.loc[df['Journey/Action'].str.contains('Bus journey')]
        bus_journeys['Bus Route'] = bus_journeys['Journey/Action'].str.extract(r'(\w\d+)')
        bus_journeys['Journey/Action'] = np.nan
        bus_journeys['From'] = np.nan

        # Merging the processed dataframe subset for bus journeys back into the main dataframe
        df.loc[bus_journeys.index] = bus_journeys

        final_columns = ['Start Time', 'End Time', 'From', 'To', 'Bus Route','Charge', 'Note']
        df = df[final_columns].reset_index().drop('index', axis=1)

        self.df = df
        return self.df

    @staticmethod
    def _df_row_to_journey(row: pd.Series) -> Journey:
        start_time = row['Start Time'].to_pydatetime() if not pd.isnull(row['Start Time']) else None
        end_time = row['End Time'].to_pydatetime() if not pd.isnull(row['End Time']) else None
        origin = row['From'] if not pd.isnull(row['From']) else None
        destination = row['To'] if not pd.isnull(row['To']) else None
        charge = row['Charge'] if not pd.isnull(row['Charge']) else None
        note = row['Note'] if not pd.isnull(row['Note']) else None
        return Journey(start_time, end_time, origin, destination, charge, note)

    def _create_journeys(self) -> list:
        journeys = [self._df_row_to_journey(row) for _, row in self.df.iterrows()]
        return journeys
