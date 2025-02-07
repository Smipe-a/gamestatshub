from typing import List
import pandas as pd
from analysis.helper import PLATFORMS, postgres_data, define_game


def get_activity(platform: str, dataframes: List[pd.DataFrame], year: int):
    activities = postgres_data(platform, 'history', year)
    activities['gameid'] = activities.achievementid.apply(define_game)

    agg_df = activities.groupby(['playerid', 'gameid'], as_index=False).agg({'date_acquired': ['min', 'max']})
    agg_df.columns = ['playerid', 'gameid', 'date_min', 'date_max']

    dataframes.append(agg_df)


if __name__ == '__main__':
    for platform in PLATFORMS:
        all_activity = []
        for year in range(2008, 2025):
            get_activity(platform=platform, dataframes=all_activity, year=year)

    activities_df = pd.DataFrame()
    for activity_df in all_activity:
        activities_df = pd.concat([activities_df, activity_df])

    activities_df.to_csv(f'analysis/resources/activity_{platform}.csv', index=False)
