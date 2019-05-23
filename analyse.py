import os.path
import datetime as dt
from typing import List

import pandas as pd

TESTDATA_DIR = os.path.join('testdata')


def load_csvs(csv_filepaths: List[str]) -> pd.DataFrame:
    dfs = []
    for csv_file in csv_filepaths:
        df = pd.read_csv(csv_file)
        dfs.append(df)
    df = pd.concat(dfs)
    df = df.sort_values('Date')

    # Drop all rows with no end time (bus journeys and top ups)
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
    df.drop(columns=['Journey/Action'])
    df = df[pd.notnull(df['From']) & pd.notnull(df['To'])]

    final_columns = ['Start Time', 'End Time', 'From', 'To', 'Journey time', 'Charge', 'Note']
    df = df[final_columns]

    return df
