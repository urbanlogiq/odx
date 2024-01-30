# Copyright (c) 2024, CommunityLogiq Software
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pandas
from pathlib import Path
from typing import List
from tqdm import tqdm
import argparse


def consolidate_calendar_dates(path: Path) -> pandas.DataFrame:
    calendar_files = path.glob('**/calendar_dates.txt.gz')
    calendar_date_df = pandas.concat(
        [pandas.read_csv(file) for file in calendar_files], ignore_index=True
    )
    return calendar_date_df


def consolidate_trips(path: Path) -> pandas.DataFrame:
    trips_files = path.glob('**/trips.txt.gz')
    trips_dataframe = pandas.concat(
        [pandas.read_csv(file) for file in trips_files], ignore_index=True
    )
    trips_dataframe['service_id'] = trips_dataframe['service_id'].apply(str)
    trips_dataframe['route_id'] = trips_dataframe['route_id'].apply(str)
    return trips_dataframe


def get_active_services(calendar_dates: pandas.DataFrame) -> List[str]:
    return (
        calendar_dates[calendar_dates['exception_type'] == 1]['service_id']
        .apply(str)
        .unique()
        .tolist()
    )


def get_active_trip_ids(
    active_services: List[str], trips_dataframe: pandas.DataFrame
) -> List[int]:
    return (
        trips_dataframe[trips_dataframe['service_id'].isin(active_services)]['trip_id']
        .apply(int)
        .unique()
        .tolist()
    )


def get_stop_times(path: Path) -> pandas.DataFrame:
    stop_times_files = path.glob('**/stop_times.txt.gz')
    return pandas.concat(
        [pandas.read_csv(file) for file in stop_times_files], ignore_index=True
    )


def add_service_id_and_route_id_to_stop_times(
    stop_times_dataframe: pandas.DataFrame, trips_dataframe: pandas.DataFrame
) -> pandas.DataFrame:
    return stop_times_dataframe.merge(
        trips_dataframe[['route_id', 'service_id', 'trip_id']].drop_duplicates(),
        on='trip_id',
        how='left',
    )


def get_relevant_stop_times(
    active_trips: List[int], stop_times_df: pandas.DataFrame
) -> pandas.DataFrame:
    return stop_times_df[stop_times_df['trip_id'].isin(active_trips)].reset_index(
        drop=True
    )


def create_stop_times_per_date(
    calendar_date_df: pandas.DataFrame,
    stop_times_df: pandas.DataFrame,
    output_directory: Path,
) -> pandas.DataFrame:
    grouped_calendar_dates = (
        calendar_date_df.groupby(['date'])['service_id']
        .apply(list)
        .reset_index(drop=False)
        .values.tolist()
    )
    for date, service_ids in tqdm(grouped_calendar_dates,desc = f'Writing simulated AVL data to {output_directory}'):
        stop_times_for_date = stop_times_df[
            stop_times_df['service_id'].isin(service_ids)
        ].reset_index(drop=True)
        date_str = str(date)[0:4] + '-' + str(date)[4:6] + '-' + str(date)[6:8]
        # stop_times_for_date['service_date'] = pandas.to_datetime(date_str)
        # stop_times_for_date['arrival_date'] = stop_times_for_date['date']
        # stop_times_for_date['departure_date'] = stop_times_for_date['date']
        # stop_times_for_date.loc[
        #     stop_times_for_date['arrival_time'].str[0:2].apply(int) > 23, 'arrival_date'
        # ] = (
        #     pandas.to_datetime(stop_times_for_date['arrival_date'])
        #     + pandas.Timedelta(days=1)
        # )
        # stop_times_for_date.loc[
        #     stop_times_for_date['arrival_time'].str[0:2].apply(int) > 23, 'arrival_time'
        # ] = (stop_times_for_date['arrival_time'].str.split(':').str[0].apply(int) - 24).apply(
        #     str
        # ) + stop_times_for_date[
        #     'arrival_time'
        # ].str[
        #     2:
        # ]
        # stop_times_for_date.loc[
        #     stop_times_for_date['arrival_time'].str.split(':').str[0].apply(len) == 1,
        #     'arrival_time',
        # ] = (
        #     '0' + stop_times_for_date['arrival_time']
        # )
        # stop_times_for_date.loc[
        #     stop_times_for_date['departure_time'].str[0:2].apply(int) > 23,
        #     'departure_date',
        # ] = (
        #     pandas.to_datetime(stop_times_for_date['departure_date'])
        #     + pandas.Timedelta(days=1)
        # )
        # stop_times_for_date['arrival_date'] = stop_times_for_date['departure_date'].dt.date.apply(lambda x: x.isoformat())
        # stop_times_for_date['departure_date'] = stop_times_for_date['departure_date'].dt.date.apply(lambda x: x.isoformat())
        # stop_times_for_date.loc[
        #     stop_times_for_date['departure_time'].str[0:2].apply(int) > 23,
        #     'departure_time',
        # ] = (stop_times_for_date['departure_time'].str.split(':').str[0].apply(int) - 24).apply(
        #     str
        # ) + stop_times_for_date[
        #     'departure_time'
        # ].str[
        #     2:
        # ]
        # stop_times_for_date.loc[
        #     stop_times_for_date['departure_time'].str.split(':').str[0].apply(len) == 1,
        #     'departure_time',
        # ] = (
        #     '0' + stop_times_for_date['departure_time']
        # )
        # reformat to align with avl format
        stop_times_for_date['trip_id'] = stop_times_for_date['trip_id'].apply(int)
        stop_times_for_date['stop_id'] = stop_times_for_date['stop_id'].apply(str)
        stop_times_for_date['scheduled_arrive_time'] = (
            + stop_times_for_date['arrival_time']
        )
        stop_times_for_date['actual_arrive_time'] = stop_times_for_date[
            'arrival_time'
        ]
        stop_times_for_date['scheduled_departure_time'] = (
            + stop_times_for_date['departure_time']
        )
        stop_times_for_date['actual_departure_time'] = stop_times_for_date[
            'departure_time'
        ]
        stop_times_for_date['trip_id'] = stop_times_for_date['trip_id'].astype(int)
        stop_times_for_date['stop_sequence'] = stop_times_for_date['stop_sequence'].astype(int)
        output_location = output_directory/Path(f'service_date={date_str}')
        if not output_location.exists():
            output_location.mkdir(parents=True)
        stop_times_for_date[['trip_id','stop_id','stop_sequence','scheduled_arrive_time','scheduled_departure_time','actual_arrive_time','actual_departure_time']].to_parquet(output_location / Path('stop_times.parquet.gzip'),compression='gzip',use_deprecated_int96_timestamps=True)

def get_relevant_dates(calendar_date_df: pandas.DataFrame,start_date_range: str,end_date_range: str) -> pandas.DataFrame:
    calendar_date_df['pandas_datetime'] = pandas.to_datetime(calendar_date_df['date'].apply(str).str[0:4] + '-' + calendar_date_df['date'].apply(str).str[4:6] + '-' +calendar_date_df['date'].apply(str).str[6:8] )
    return calendar_date_df[(calendar_date_df['pandas_datetime'] >= pandas.to_datetime(start_date_range))&(calendar_date_df['pandas_datetime'] <= pandas.to_datetime(end_date_range))].drop(['pandas_datetime'],axis =1 ).reset_index(drop = True)


def main(start_date: str, end_date: str, gtfs_location: Path, output_location: Path):
    consolidated_calendar_dates_df = (
        consolidate_calendar_dates(gtfs_location)
        .drop_duplicates()
        .reset_index(drop=True)
    )
    consolidated_calendar_dates_df = get_relevant_dates(consolidated_calendar_dates_df,start_date,end_date)
    consolidated_trips_df = (
        consolidate_trips(gtfs_location).drop_duplicates().reset_index(drop=True)
    )
    active_services = get_active_services(consolidated_calendar_dates_df)
    active_trips = get_active_trip_ids(active_services, consolidated_trips_df)
    consolidated_stop_times_df = (
        get_stop_times(gtfs_location).drop_duplicates().reset_index(drop=True)
    )
    consolidated_stop_times_df = get_relevant_stop_times(
        active_trips, consolidated_stop_times_df
    )
    consolidated_stop_times_df = (
        add_service_id_and_route_id_to_stop_times(
            consolidated_stop_times_df, consolidated_trips_df
        )
        .drop_duplicates(
            subset=[
                'trip_id',
                'arrival_time',
                'departure_time',
                'stop_id',
                'stop_sequence',
                'route_id',
                'service_id',
            ]
        )
        .reset_index(drop=True)
    )
    create_stop_times_per_date(
        consolidated_calendar_dates_df, consolidated_stop_times_df, output_location
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simulate AVL from GTFS')
    parser.add_argument('--start_date', type=str, required=True, help='Start date in the format YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, required=True, help='End date in the format YYYY-MM-DD')
    parser.add_argument('--gtfs_location', type=str, required=True, help='Location of the GTFS data')
    parser.add_argument('--output_directory', type=str, required=True, help='Directory to output the simulated AVL data')
    args = parser.parse_args()
    main(args.start_date, args.end_date, Path(args.gtfs_location), Path(args.output_directory))
