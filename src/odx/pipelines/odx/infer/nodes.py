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

"""Nodes for inferring ODX from prepared Hop data."""
from functools import reduce
from typing import List, Tuple, Union

from loguru import logger
from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from .impossible_conditions_and_descriptions import (
    get_impossible_conditions_and_descriptions,
)
from .utils import haversine_meters

INTERLINING_TRIP_TYPE = "INTERLINE"
INTERLINING_STOP_TYPE = "INTERLINE"
SINGLE_LINE_TRIP_TYPE = "SINGLE_LINE"
SINGLE_LINE_STOP_TYPE = "SINGLE_LINE"


def get_year_months(
    spark_df: SparkDF,
    column_to_parse: str,
    year_months: Union[List[List[int]], int] = -1,
) -> SparkDF:
    """Get the rows by filtering 'column_to_parse' according to 'year_months'

    Args:
        spark_df (SparkDF): SparkDF with column_to_parse.
        column_to_parse (str): Date/Datetime column to parse.
        year_months (Union[List[List[int]], int], optional): If list of list of int,
            treat as [[YYYY,MM]]. If -1 treat as latest available month in data.

    Returns:
        SparkDF: SparkDF with selected based on year_months

    Raises:
        ValueError: If an incorrect type is provided for year_months
    """
    if isinstance(year_months, list):
        conditions = (
            (
                (F.year(F.col(column_to_parse)) == F.lit(year))
                & (F.month(F.col(column_to_parse)) == F.lit(month))
            )
            for year, month in year_months
        )
        condition = reduce(lambda x, y: x | y, conditions)
        logger.info(
            f"List of year/months selected: {[f'year={year},month={month}' for year, month in year_months]}"
        )
    elif year_months == -1:
        max_year, max_month = (
            spark_df.select(F.max(column_to_parse).alias("MAX_DATE"))
            .select(
                F.year(F.col("MAX_DATE")).alias("YEAR"),
                F.month(F.col("MAX_DATE")).alias("MONTH"),
            )
            .toPandas()[["YEAR", "MONTH"]]
            .iloc[0]
            .values.tolist()
        )
        condition = (F.year(F.col(column_to_parse)) == F.lit(max_year)) & (
            F.month(F.col(column_to_parse)) == F.lit(max_month)
        )
        logger.info(
            f"Most recent month selected for inference. Inferring for year={max_year} and month={max_month}"
        )
    else:
        raise ValueError(
            f"Expected either a list of years and months or -1, got {year_months}"
        )
    return spark_df.filter(condition)


def get_relevant_stop_times(
    hop_events_spark_df: SparkDF, stop_times_spark_df: SparkDF
) -> SparkDF:
    """Only consider service dates that we have hop data for.

    Args:
        hop_events_spark_df (SparkDF): Hop tap events with service date column.
        stop_times_spark_df (SparkDF): Stop times sourced from CAD avl.

    Returns:
        SparkDF: SparkDF where the stop_times_spark_df have been filtered by the dates contained in the HOP data.
    """
    hop_dates = (
        hop_events_spark_df.select("SERVICE_DATE")
        .dropDuplicates()
        .toPandas()["SERVICE_DATE"]
        .values.tolist()
    )
    return stop_times_spark_df.filter(
        F.col("SERVICE_DATE").isin(hop_dates)
    )  # only keep the stop times that have hop taps associated with them


def merge_stop_locations_onto_taps(
    hop_taps_spark_dataframe: SparkDF, stop_times_spark_df: SparkDF
) -> SparkDF:
    """Merge the locations (lat,lon) of the stop ids onto the taps.
        It seems the HOP taps aren't as consistent as gtfs. Rows with STOP_ID = -1 will be filtered.

    Args:
        hop_taps_spark_dataframe (SparkDF): Hop taps with STOP_ID column
        stop_times_spark_df (SparkDF): Stop times with stop_id, stop_lat, stop_lon columns

    Returns:
        SparkDF: SparkDF with the lats and lons from the stop_times merged onto the hop taps
    """
    stop_locations = stop_times_spark_df.groupby(F.col("STOP_ID")).agg(
        F.mean("STOP_LAT").alias("STOP_LAT"), F.mean("STOP_LON").alias("STOP_LON")
    )
    result = hop_taps_spark_dataframe.join(
        F.broadcast(
            stop_locations.filter(~F.col("STOP_ID").isNull()).select(
                F.col("STOP_ID").alias("STOP_ID"),
                F.col("STOP_LAT").alias("STOP_LAT"),
                F.col("STOP_LON").alias("STOP_LON"),
            )
        ),
        on=["STOP_ID"],
        how="left",
    ).filter(F.col("STOP_ID") != -1)
    return result
    # remove any rows with stop id being equal to -1. Seems redundent given a similar node in prepare. TODO: remove this filter


def build_possible_transfers(
    spark_df: SparkDF, unique_rider_column: str, transfer_table_columns: List[str]
) -> SparkDF:
    """Build a table of possible transfers by windowing over the rider events sorted by datetime
    and append the following row to each row.

    Args:
        spark_df (SparkDF): SparkDF to look for transfers in. Must have unique_rider_column,"DATETIME" and columns in transfer_table_columns
        unique_rider_column (str): Column to identify unique riders
        transfer_table_columns (List[str]): Columns to retain for transfers

    Returns:
        SparkDF: DataFrame containing possible transfers in journeys
    """
    transfers_window = Window.partitionBy(unique_rider_column).orderBy(
        F.col("DATETIME").asc()
    )
    possible_transfers = spark_df.select(
        unique_rider_column,
        *transfer_table_columns,
        *[
            F.lead(F.col(str(column)), 1)
            .over(transfers_window)
            .alias(str(column) + "_NEXT")
            for column in transfer_table_columns
        ],
    )
    return possible_transfers


def get_possible_max_boarding_alternatives(
    stop_times_spark_df: SparkDF,
    threshold: int,
    non_max_allowed_alternatives: List[int],
) -> SparkDF:
    """The stop ids for max lines aren't reliable due to the ability for a rider to tap
    at a certain stop id then board at another. This function builds an alternative boarding
    stop id dataset that can be used to search for more probable boarding locations given a MAX tap in the area.
    It only functions on MAX lines, that is to say lines that have the route_id == 200. See prepare pipeline
    to find out which lines are mapped to 200. Haversine is used to measure distances between stops.

    Args:
        stop_times_spark_df (SparkDF): SparkDF with route_id, stop_id, stop_lat,stop_lon columns
        threshold (int): Max distance that a stop can be in order to be considered an alternative. Units are meters.
        non_max_allowed_alternatives (List[int]): List of allowed alternative lines to search for around MAX stops.

    Returns:
        SparkDF: SparkDF of boarding alternatives for MAX stops.
    """
    max_line_stops = (
        stop_times_spark_df.filter(
            ((F.col("ROUTE_ID") == 200))
            | (F.col("STOP_ID").isin(non_max_allowed_alternatives))
        )
        .select("ROUTE_ID", "STOP_ID", "STOP_LAT", "STOP_LON")
        .dropDuplicates(subset=["STOP_ID"])
    )
    max_line_stops = max_line_stops.join(
        max_line_stops.select(
            F.col("STOP_ID").alias("STOP_ID_ALTERNATIVE"),
            F.col("STOP_LAT").alias("STOP_LAT_ALTERNATIVE"),
            F.col("STOP_LON").alias("STOP_LON_ALTERNATIVE"),
        ),
        haversine_meters(
            F.col("STOP_LAT"),
            F.col("STOP_LON"),
            F.col("STOP_LAT_ALTERNATIVE"),
            F.col("STOP_LON_ALTERNATIVE"),
        )
        < threshold,
        how="left",
    ).filter(
        (F.col("ROUTE_ID") == 200) | (F.col("STOP_ID") == F.col("STOP_ID_ALTERNATIVE"))
    )  # only allow MAX lines or the stop itself as an alternative
    return max_line_stops


def split_bus_from_max_lines(
    possible_transfer_events_spark_df: SparkDF,
) -> Tuple[SparkDF, SparkDF]:
    possible_transfer_events_spark_df = possible_transfer_events_spark_df.withColumn(
        "UNIQUE_ROW_ID",
        F.sha2(
            F.concat(
                F.col("CARD_ID").cast(StringType()),
                F.col("DATETIME").cast(StringType()),
            ),
            256,
        ),
    )
    possible_transfer_events_spark_df = possible_transfer_events_spark_df.withColumn(
        "STOP_ID_OLD", F.col("STOP_ID")
    )  # TODO: Remove afterwards
    max_line_transfer_events_spark_df = possible_transfer_events_spark_df.filter(
        F.col("LINE_ID") == 200
    )
    bus_line_transfer_events_spark_df = possible_transfer_events_spark_df.filter(
        F.col("LINE_ID") != 200
    )
    return (
        max_line_transfer_events_spark_df.cache(),
        bus_line_transfer_events_spark_df.cache(),
    )


def build_and_insert_interlining_trips_and_add_trip_type(
    stop_times_spark_df: SparkDF,
    interlining_trips_spark_df: SparkDF,
    allow_interlining: bool,
) -> SparkDF:
    stop_times_spark_df = stop_times_spark_df.withColumn(
        "TRIP_TYPE", F.lit(SINGLE_LINE_TRIP_TYPE)
    ).withColumn("STOP_TYPE", F.lit(SINGLE_LINE_STOP_TYPE))
    if allow_interlining:
        # merge interlining trip ids
        stop_times_spark_df = stop_times_spark_df.join(
            interlining_trips_spark_df, on=["SERVICE_DATE", "TRIP_ID"], how="left"
        )
        stop_times_spark_df = (
            stop_times_spark_df.withColumn(
                "TRIP_TYPE",
                F.when(
                    F.col("INTERLINING_TRIP_ID") == F.col("TRIP_ID"),
                    F.lit(SINGLE_LINE_TRIP_TYPE),
                ).otherwise(F.lit(INTERLINING_TRIP_TYPE)),
            )
            .withColumn("TRIP_ID", F.col("INTERLINING_TRIP_ID"))
            .drop("INTERLINING_TRIP_ID")
        )
        interlining_trips = stop_times_spark_df.filter(
            F.col("TRIP_TYPE") == INTERLINING_TRIP_TYPE
        )
        # remove duplicated stops from interline trips
        next_stop_window = Window.partitionBy(
            F.col("SERVICE_DATE"), F.col("TRIP_ID")
        ).orderBy(F.col("ARRIVE_DATETIME").asc())
        interlining_trips = (
            interlining_trips.withColumn(
                "NEXT_STOP_ID", F.lead(F.col("STOP_ID"), 1).over(next_stop_window)
            )
            .withColumn(
                "PREV_STOP_ID", F.lag(F.col("STOP_ID"), 1).over(next_stop_window)
            )
            .withColumn(
                "NEXT_DEPARTURE_DATETIME",
                F.lead(F.col("DEPARTURE_DATETIME"), 1).over(next_stop_window),
            )
            .withColumn(
                "NEXT_DEPARTURE_TIME",
                F.lead(F.col("DEPARTURE_TIME"), 1).over(next_stop_window),
            )
            .withColumn(
                "NEXT_SCHEDULED_DEPARTURE_TIME",
                F.lead(F.col("SCHEDULED_DEPARTURE_TIME"), 1).over(next_stop_window),
            )
        )
        # reassign the departure times
        interlining_trips = (
            interlining_trips.withColumn(
                "DEPARTURE_DATETIME",
                F.when(
                    F.col("NEXT_STOP_ID") == F.col("STOP_ID"),
                    F.col("NEXT_DEPARTURE_DATETIME"),
                ).otherwise(F.col("DEPARTURE_DATETIME")),
            )
            .withColumn(
                "STOP_TYPE",
                F.when(
                    F.col("NEXT_STOP_ID") == F.col("STOP_ID"),
                    F.lit(INTERLINING_STOP_TYPE),
                ).otherwise(SINGLE_LINE_STOP_TYPE),
            )
            .withColumn(
                "DEPARTURE_TIME",
                F.when(
                    F.col("NEXT_STOP_ID") == F.col("STOP_ID"),
                    F.col("NEXT_DEPARTURE_TIME"),
                ).otherwise(F.col("DEPARTURE_TIME")),
            )
            .withColumn(
                "SCHEDULED_DEPARTURE_TIME",
                F.when(
                    F.col("NEXT_STOP_ID") == F.col("STOP_ID"),
                    F.col("NEXT_SCHEDULED_DEPARTURE_TIME"),
                ).otherwise(F.col("SCHEDULED_DEPARTURE_TIME")),
            )
            .drop(
                "NEXT_STOP_ID",
                "NEXT_SCHEDULED_DEPARTURE_TIME",
                "NEXT_DEPARTURE_TIME",
                "NEXT_DEPARTURE_DATETIME",
            )
        )
        # remove the duplicate stops want to keep the first time the vehicle arrives as the stop
        interlining_trips = interlining_trips.filter(
            F.col("PREV_STOP_ID") != F.col("STOP_ID")
        ).drop("PREV_STOP_ID")
        # rebuild stop sequence
        interlining_trips = interlining_trips.withColumn(
            "STOP_SEQUENCE",
            F.row_number().over(
                next_stop_window
            ),  # row number starts at 1, same as stop sequence
        )
        # reinsert interline trips
        stop_times_spark_df = stop_times_spark_df.filter(
            F.col("TRIP_TYPE") == SINGLE_LINE_TRIP_TYPE
        ).unionByName(interlining_trips)
    return stop_times_spark_df


def get_max_possible_boarding_trip_ids(
    max_line_boarding_points_spark_df: SparkDF,
    stop_times_spark_df: SparkDF,
    max_line_boarding_alternatives: SparkDF,
) -> SparkDF:
    """Finds possible boarding locations associated with MAX taps. Because riders for MAX tap on
    the platform, it's unclear which line they actually board. So alternatives associated with the
    captured tapping location are inserted. Since we don't actually know the line they boarded,
    we evaluate boarding probability on a per (GTFS) line, per direction basis.
    Sequence of actions expected is:
    Rider taps on platform -> vehicle arrives -> rider boards/vehicle departs.

    Args:
        max_line_boarding_points_spark_df (SparkDF): Max (LINE 200 in HOP) boarding taps
        stop_times_spark_df (SparkDF): AVL stop times
        max_line_boarding_alternatives (SparkDF): Alternative locations associated with each stop that serves Max lines

    Returns:
        SparkDF: Possible boarding trips
    """
    # insert alternatives
    max_line_boarding_points_spark_df = max_line_boarding_points_spark_df.join(
        max_line_boarding_alternatives,
        on="STOP_ID",
        how="left",
    )
    # rename alternatives to actual locations. The original location is always an alternative itself.
    max_line_boarding_points_spark_df = (
        max_line_boarding_points_spark_df.select(
            *[
                col
                for col in max_line_boarding_points_spark_df.columns
                if col not in ["STOP_ID", "STOP_LAT", "STOP_LON", "ROUTE_ID"]
            ]
        )
        .withColumn("STOP_ID", F.col("STOP_ID_ALTERNATIVE"))
        .withColumn("STOP_LAT", F.col("STOP_LAT_ALTERNATIVE"))
        .withColumn("STOP_LON", F.col("STOP_LON_ALTERNATIVE"))
        .drop(
            *[
                col
                for col in max_line_boarding_points_spark_df.columns
                if "ALTERNATIVE" in col
            ]
        )
    )
    # join the arrival datetimes of possible boarding points
    possible_boarding_stop_times = stop_times_spark_df.select(
        F.col("SERVICE_DATE").alias("BOARDING_STOP_SERVICE_DATE"),
        F.col("STOP_ID").alias("BOARDING_STOP_STOP_ID"),
        F.col("ROUTE_ID").alias("BOARDING_STOP_LINE_ID"),
        F.col("ROUTE_ID_OLD").alias("BOARDING_STOP_LINE_ID_OLD"),
        "ARRIVE_DATETIME",
        "DEPARTURE_DATETIME",
        "MEAN_BOARDING_INTERVAL_SECONDS",
        "TRIP_ID",
        "HOUR",
        F.col("DIRECTION_ID").alias("STOP_DIRECTION_ID"),
        F.col("STOP_SEQUENCE").alias("BOARDING_STOP_STOP_SEQUENCE"),
    )
    max_possible_boarding_points = max_line_boarding_points_spark_df.join(
        possible_boarding_stop_times,
        (
            max_line_boarding_points_spark_df["SERVICE_DATE"]
            == possible_boarding_stop_times["BOARDING_STOP_SERVICE_DATE"]
        )
        & (
            max_line_boarding_points_spark_df["STOP_ID"]
            == possible_boarding_stop_times["BOARDING_STOP_STOP_ID"]
        )
        & (
            max_line_boarding_points_spark_df["LINE_ID"]
            == possible_boarding_stop_times["BOARDING_STOP_LINE_ID"]
        )
        & (
            max_line_boarding_points_spark_df["DATETIME"].cast("long")
            <= possible_boarding_stop_times["DEPARTURE_DATETIME"].cast(
                "long"
            )  # for max, you must tap on the platform then board. So you must tap before the vehicle departs
        ),
        how="left",
    ).drop(
        "BOARDING_STOP_STOP_ID", "BOARDING_STOP_LINE_ID", "BOARDING_STOP_SERVICE_DATE"
    )
    max_departure_window = Window.partitionBy(
        F.col("UNIQUE_ROW_ID"),
        F.col(
            "BOARDING_STOP_LINE_ID_OLD"
        ),  # evaluate boarding probability against routes and directions
        F.col("STOP_DIRECTION_ID"),
    ).orderBy(F.col("DEPARTURE_DATETIME").asc())
    max_possible_boarding_points = (
        max_possible_boarding_points.withColumn(
            "ROW_ID", F.row_number().over(max_departure_window)
        )
        .filter(
            F.col("ROW_ID") <= 2
        )  # only select two options per line id and direction id. Either you take the first option or you miss the first and take the second
        .drop("ROW_ID")
    )
    max_possible_boardings = max_possible_boarding_points.withColumn(
        "BOARDING_PROBABILITY",
        (F.lit(1) - F.exp(-F.lit(1) / F.col("MEAN_BOARDING_INTERVAL_SECONDS")))
        * F.exp(
            F.lit(-1)
            * (
                F.col("DEPARTURE_DATETIME").cast("long")
                - F.col("DATETIME").cast("long")
            )
            / F.col("MEAN_BOARDING_INTERVAL_SECONDS")
        ),
    )
    unique_row_window = Window.partitionBy(
        F.col("UNIQUE_ROW_ID"),
        F.col("BOARDING_STOP_LINE_ID_OLD"),
        F.col("STOP_DIRECTION_ID"),
    ).orderBy(
        F.col("BOARDING_PROBABILITY").asc()
    )  # normalize along a line and direction
    max_possible_boardings = max_possible_boardings.withColumn(
        "BOARDING_PROBABILITY",
        F.col("BOARDING_PROBABILITY")
        / F.sum(F.col("BOARDING_PROBABILITY")).over(
            unique_row_window.rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )
        ),
    )
    return max_possible_boardings


def get_bus_possible_boarding_trip_ids(
    bus_line_boarding_points_spark_df: SparkDF,
    stop_times_spark_df: SparkDF,
) -> SparkDF:
    """Finds possible boarding trips for buses by looking for vehicles that are in the area just after the tap time.
    This is because because readers on the vehicle rather than the platform, so the sequence of actions expected is
    vehicle arrives -> rider boards/taps -> vehicle departs.

    Args:
        bus_line_boarding_points_spark_df (SparkDF): Boarding events for bus lines
        stop_times_spark_df (SparkDF): AVL stop times

    Returns:
        SparkDF: Possible boarding trips
    """
    possible_boarding_stop_times = stop_times_spark_df.select(
        F.col("SERVICE_DATE").alias("BOARDING_STOP_SERVICE_DATE"),
        F.col("STOP_ID").alias("BOARDING_STOP_STOP_ID"),
        F.col("ROUTE_ID").alias("BOARDING_STOP_LINE_ID"),
        F.col("ROUTE_ID_OLD").alias("BOARDING_STOP_LINE_ID_OLD"),
        "ARRIVE_DATETIME",
        "DEPARTURE_DATETIME",
        "MEAN_BOARDING_INTERVAL_SECONDS",
        "TRIP_ID",
        "HOUR",
        F.col("DIRECTION_ID").alias("STOP_DIRECTION_ID"),
        F.col("STOP_SEQUENCE").alias("BOARDING_STOP_STOP_SEQUENCE"),
    )
    bus_line_boarding_points_spark_df = bus_line_boarding_points_spark_df.join(
        possible_boarding_stop_times,
        (
            bus_line_boarding_points_spark_df["SERVICE_DATE"]
            == possible_boarding_stop_times["BOARDING_STOP_SERVICE_DATE"]
        )
        & (
            bus_line_boarding_points_spark_df["STOP_ID"]
            == possible_boarding_stop_times["BOARDING_STOP_STOP_ID"]
        )
        & (
            bus_line_boarding_points_spark_df["LINE_ID"]
            == possible_boarding_stop_times["BOARDING_STOP_LINE_ID"]
        )
        & (
            bus_line_boarding_points_spark_df["DATETIME"].cast("long")
            >= possible_boarding_stop_times["ARRIVE_DATETIME"].cast(
                "long"
            )  # only possible if you board then tap.
        ),
        how="left",
    ).drop(
        "BOARDING_STOP_STOP_ID", "BOARDING_STOP_LINE_ID", "BOARDING_STOP_SERVICE_DATE"
    )
    bus_arrival_window = Window.partitionBy(F.col("UNIQUE_ROW_ID")).orderBy(
        F.col("ARRIVE_DATETIME").desc()
    )
    bus_line_boarding_points_spark_df = (
        bus_line_boarding_points_spark_df.withColumn(
            "ROW_ID", F.row_number().over(bus_arrival_window)
        )
        .filter(
            F.col("ROW_ID") <= 3
        )  # only select three options to handle bus bunching
        .drop("ROW_ID")
    )
    bus_line_boarding_points_spark_df = bus_line_boarding_points_spark_df.withColumn(
        "BOARDING_PROBABILITY",
        (
            (F.lit(1) - F.exp(-F.lit(1) / F.col("MEAN_BOARDING_INTERVAL_SECONDS")))
            / (
                F.lit(1)
                - F.exp(
                    -(F.col("MEAN_BOARDING_INTERVAL_SECONDS") + 1)
                    / F.col("MEAN_BOARDING_INTERVAL_SECONDS")
                )
            )
        )
        * F.exp(
            -(F.col("DATETIME").cast("long") - F.col("ARRIVE_DATETIME").cast("long"))
            / F.col("MEAN_BOARDING_INTERVAL_SECONDS")
        ),
    )
    unique_row_window = Window.partitionBy(F.col("UNIQUE_ROW_ID")).orderBy(
        F.col("BOARDING_PROBABILITY").asc()
    )  # normalize along a line and direction
    bus_line_boarding_points_spark_df = bus_line_boarding_points_spark_df.withColumn(
        "BOARDING_PROBABILITY",
        F.col("BOARDING_PROBABILITY")
        / F.sum(F.col("BOARDING_PROBABILITY")).over(
            unique_row_window.rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )
        ),
    )
    return bus_line_boarding_points_spark_df


def get_possible_alighting_points(
    max_transfer_events_spark_df: SparkDF,
    bus_transfer_events_spark_df: SparkDF,
    stop_times_spark_df: SparkDF,
) -> SparkDF:
    """Get the possible alighting points based on the boarding trip id, the next tap location and the next tap time.

    Args:
        max_transfer_events_spark_df (SparkDF): Max Hop transfer events
        bus_transfer_events_spark_df (SparkDF): Bus Hop transfer events
        stop_times_spark_df (SparkDF): AVL Stop times.

    Returns:
        SparkDF: SparkDF with candidate alighting locations inserted as new rows
    """
    transfer_events_with_trip_id_spark_df = max_transfer_events_spark_df.unionByName(
        bus_transfer_events_spark_df
    )
    stop_times_spark_df = stop_times_spark_df.select(
        F.col("SERVICE_DATE").alias("ALIGHTING_STOP_SERVICE_DATE"),
        "TRIP_TYPE",
        F.col("TRIP_ID").alias("ALIGHTING_STOP_TRIP_ID"),
        F.col("ARRIVE_DATETIME").alias("ALIGHTING_STOP_ARRIVE_DATETIME"),
        F.col("STOP_LON").alias("ALIGHTING_STOP_LON"),
        F.col("STOP_LAT").alias("ALIGHTING_STOP_LAT"),
        F.col("DEPARTURE_DATETIME").alias("ALIGHTING_STOP_DEPARTURE_DATETIME"),
        F.col("STOP_SEQUENCE").alias("ALIGHTING_STOP_STOP_SEQUENCE"),
        F.col("ROUTE_ID_OLD").alias("ALIGHTING_STOP_ROUTE_ID"),
        F.col("STOP_ID").alias("ALIGHTING_STOP_ID"),
    )
    possible_transfer_events_spark_df = (
        transfer_events_with_trip_id_spark_df.join(
            stop_times_spark_df,
            (
                (
                    stop_times_spark_df["ALIGHTING_STOP_SERVICE_DATE"]
                    == transfer_events_with_trip_id_spark_df["SERVICE_DATE"]
                )
                & (
                    stop_times_spark_df["ALIGHTING_STOP_TRIP_ID"]
                    == transfer_events_with_trip_id_spark_df["TRIP_ID"]
                )
                & (
                    stop_times_spark_df["ALIGHTING_STOP_ARRIVE_DATETIME"].cast("long")
                    <= (
                        transfer_events_with_trip_id_spark_df["DATETIME_NEXT"].cast(
                            "long"
                        )
                    )
                )
                & (
                    transfer_events_with_trip_id_spark_df["DEPARTURE_DATETIME"].cast(
                        "long"
                    )
                    < stop_times_spark_df["ALIGHTING_STOP_ARRIVE_DATETIME"].cast("long")
                )
                & (
                    transfer_events_with_trip_id_spark_df["DATETIME"].cast("long")
                    < stop_times_spark_df["ALIGHTING_STOP_ARRIVE_DATETIME"].cast("long")
                )
            ),
            how="left",
        )
    ).drop("STOP_TIME_SERVICE_DATE", "STOP_TIME_TRIP_ID")
    return possible_transfer_events_spark_df


def get_transfer_distance_and_minimum_required_transfer_time(
    transfer_events_with_trip_id_spark_df: SparkDF, walking_speed_meters_second: float
) -> SparkDF:
    """Get the transfer distance between possible alighting points and the next boarding location. Using this
    the minimum required transfer time can be calculated. Also get the distance from the boarding point.

    Args:
        transfer_events_with_trip_id_spark_df (SparkDF): transfer events HOP dataframe
        walking_speed_meters_second (float): Walking speed of an individual human being.

    Returns:
        SparkDF: SparkDF with minimum required transfer time and transfer distance columns added
    """
    transfer_events_with_trip_id_spark_df = transfer_events_with_trip_id_spark_df.withColumn(
        "TRANSFER_DISTANCE_METERS",
        (
            2 ** (0.5)
        )  # sqrt(2) to control for grid. This is important because the linear distance is not necessarily the distance walked.
        * haversine_meters(
            F.col("ALIGHTING_STOP_LAT"),
            F.col("ALIGHTING_STOP_LON"),
            F.col("STOP_LAT_NEXT"),
            F.col("STOP_LON_NEXT"),
        ),
    )
    transfer_events_with_trip_id_spark_df = transfer_events_with_trip_id_spark_df.withColumn(
        "MINIMUM_REQUIRED_TRANSFER_TIME_SECONDS",
        F.col("TRANSFER_DISTANCE_METERS") / F.lit(walking_speed_meters_second),
    ).withColumn(
        "TOTAL_WALKING_TRAVEL_TIME_SECONDS",
        (
            (
                2 ** (0.5)
            )  # sqrt(2) to control for grid. This is important because the linear distance is not necessarily the distance walked.
            * haversine_meters(
                F.col("STOP_LAT"),
                F.col("STOP_LON"),
                F.col("STOP_LAT_NEXT"),
                F.col("STOP_LON_NEXT"),
            )
        )
        / F.lit(walking_speed_meters_second),
    )
    return transfer_events_with_trip_id_spark_df


def get_alighting_probability(
    possible_transfer_events_spark_df: SparkDF, allow_no_stops=True
) -> SparkDF:
    """Choose the appropriate alighting point based on the next tap location, the available time and the alighting location.
    Makes use of utility theory to report a confidence score.

    Args:
        possible_transfer_events_spark_df (SparkDF): SparkDF of possible transfer events with possible alightings.

    Returns:
        SparkDF: Returns a SparkDF containing possible alighting locations.
    """
    within_trip_window = Window.partitionBy("UNIQUE_ROW_ID", "TRIP_ID").orderBy(
        F.col("ALIGHTING_STOP_DEPARTURE_DATETIME").asc()
    )
    possible_transfer_events_spark_df = (
        possible_transfer_events_spark_df.withColumn(
            "TRAVEL_TIME_SECONDS",
            (
                F.col("ALIGHTING_STOP_ARRIVE_DATETIME").cast("long")
                - F.col("ARRIVE_DATETIME").cast("long")
                + F.col("MINIMUM_REQUIRED_TRANSFER_TIME_SECONDS")
            ),
        )
        .withColumn(
            "MIN_TRAVEL_TIME_SECONDS",
            F.min(F.col("TRAVEL_TIME_SECONDS")).over(
                within_trip_window.rowsBetween(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        )
        .withColumn(
            "MAX_TRAVEL_TIME_SECONDS",
            F.max(F.col("TRAVEL_TIME_SECONDS")).over(
                within_trip_window.rowsBetween(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        )
    )

    def calculate_stop_alighting_probability() -> Column:
        return (
            F.lit(0.5).cast("double")
            * (
                F.lit(1).cast("double")
                / (
                    F.col("MAX_TRAVEL_TIME_SECONDS").cast("double")
                    - F.col("MIN_TRAVEL_TIME_SECONDS").cast("double")
                )
            )
        ) ** 2 / (
            (
                F.col("TRAVEL_TIME_SECONDS").cast("double")
                - F.col("MIN_TRAVEL_TIME_SECONDS").cast("double")
            )
            ** 2
            + (
                F.lit(0.5).cast("double")
                * (
                    F.lit(1).cast("double")
                    / (
                        F.col("MAX_TRAVEL_TIME_SECONDS").cast("double")
                        - F.col("MIN_TRAVEL_TIME_SECONDS").cast("double")
                    )
                )
            )
            ** 2
        )

    if allow_no_stops:
        possible_transfer_events_spark_df = (
            possible_transfer_events_spark_df.withColumn(
                "STOP_ALIGHTING_PROBABILITY", calculate_stop_alighting_probability()
            )
        )
    else:
        possible_transfer_events_spark_df = (
            possible_transfer_events_spark_df.withColumn(
                "STOP_ALIGHTING_PROBABILITY",
                F.when(
                    F.col("ALIGHTING_STOP_ARRIVE_DATETIME")
                    != F.col("ALIGHTING_STOP_DEPARTURE_DATETIME"),
                    calculate_stop_alighting_probability(),
                ).otherwise(F.lit(0).cast("double")),
            )
        )
    possible_transfer_events_spark_df = possible_transfer_events_spark_df.withColumn(
        "CUMULATIVE_NOT_ALIGHTING_PROBABILITY",
        F.product(
            F.lit(1).cast("double") - F.col("STOP_ALIGHTING_PROBABILITY").cast("double")
        ).over(within_trip_window.rowsBetween(Window.unboundedPreceding, -1)),
    ).withColumn(  # handle nulls due to lag on window
        "CUMULATIVE_NOT_ALIGHTING_PROBABILITY",
        F.when(F.col("CUMULATIVE_NOT_ALIGHTING_PROBABILITY").isNull(), 1).otherwise(
            F.col("CUMULATIVE_NOT_ALIGHTING_PROBABILITY")
        ),
    )
    possible_transfer_events_spark_df = possible_transfer_events_spark_df.withColumn(
        "ALIGHTING_PROBABILITY",
        F.col("STOP_ALIGHTING_PROBABILITY").cast("double")
        * F.col("CUMULATIVE_NOT_ALIGHTING_PROBABILITY"),
    )
    return possible_transfer_events_spark_df


def select_alighting_based_on_overall_probability(
    possible_transfer_events_spark_df: SparkDF,
) -> SparkDF:
    """Combine the boarding probability and the alighting probability to provide an
    overall probability that the rider alighted at a location at a specific time. We handle max and bus lines differently, since
    for max lines we need to evaluate the probability of a rider selecting a line and direction, while
    as with bus, we know because they tapped on the vehicle.

    Args:
        possible_transfer_events_spark_df (SparkDF): SparkDF of all possible boarding and alighting pairs
        per tap event.

    Returns:
        SparkDF: SparkDF with alighting location and time selected based on overall probability.
    """
    unique_row_window = Window.partitionBy(F.col("UNIQUE_ROW_ID")).orderBy(
        F.col("TRAVEL_TIME_SECONDS").asc()
    )
    ##### handle max lines
    max_transfer_events_spark_df = possible_transfer_events_spark_df.filter(
        F.col("LINE_ID") == 200
    )
    within_route_and_direction_window = Window.partitionBy(
        F.col("UNIQUE_ROW_ID"),
        F.col("BOARDING_STOP_LINE_ID_OLD"),
        F.col("STOP_DIRECTION_ID"),
    ).orderBy(F.col("TRANSFER_DISTANCE_METERS").desc())
    ### include route selection
    max_transfer_events_spark_df = max_transfer_events_spark_df.withColumn(
        "MIN_ROUTE_AND_DIRECTION_TRAVEL_TIME",
        F.min(F.col("MIN_TRAVEL_TIME_SECONDS")).over(
            within_route_and_direction_window.rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )
        ),
    )
    max_transfer_events_spark_df = max_transfer_events_spark_df.withColumn(
        "MIN_TRAVEL_TIME_ALL_TRIPS",
        F.min(F.col("MIN_TRAVEL_TIME_SECONDS")).over(
            unique_row_window.rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )
        ),
    )
    max_transfer_events_spark_df = max_transfer_events_spark_df.withColumn(
        "ROUTE_AND_DIRECTION_UTILITY",
        F.exp(
            F.col("MIN_TRAVEL_TIME_ALL_TRIPS")
            / F.col("MIN_ROUTE_AND_DIRECTION_TRAVEL_TIME")
        ),
    )
    sum_trip_utilities = (
        max_transfer_events_spark_df.select(
            "UNIQUE_ROW_ID",
            "STOP_DIRECTION_ID",
            "BOARDING_STOP_LINE_ID_OLD",
            "ROUTE_AND_DIRECTION_UTILITY",
        )
        .dropDuplicates(
            ["UNIQUE_ROW_ID", "BOARDING_STOP_LINE_ID_OLD", "STOP_DIRECTION_ID"]
        )
        .groupBy("UNIQUE_ROW_ID")
        .agg(
            F.sum(F.col("ROUTE_AND_DIRECTION_UTILITY")).alias(
                "SUM_ROUTE_AND_DIRECTION_UTILITIES"
            )
        )
    )
    max_transfer_events_spark_df = max_transfer_events_spark_df.join(
        sum_trip_utilities, on=["UNIQUE_ROW_ID"], how="left"
    )
    max_transfer_events_spark_df = max_transfer_events_spark_df.withColumn(
        "ROUTE_AND_DIRECTION_SELECTION_PROBABILITY",
        F.col("ROUTE_AND_DIRECTION_UTILITY")
        / F.col("SUM_ROUTE_AND_DIRECTION_UTILITIES"),
    )
    max_transfer_events_spark_df = max_transfer_events_spark_df.withColumn(
        "CONFIDENCE",
        F.col("ALIGHTING_PROBABILITY")
        * F.col("BOARDING_PROBABILITY")
        * F.col("ROUTE_AND_DIRECTION_SELECTION_PROBABILITY"),
    )
    max_transfer_events_spark_df = max_transfer_events_spark_df.drop(
        "MIN_TRAVEL_TIME_ALL_TRIPS",
        "MIN_ROUTE_AND_DIRECTION_TRAVEL_TIME",
        "ROUTE_AND_DIRECTION_UTILITY",
        "SUM_ROUTE_AND_DIRECTION_UTILITIES",
        "ROUTE_AND_DIRECTION_SELECTION_PROBABILITY",
    )
    ####handle buses
    bus_transfer_events_spark_df = possible_transfer_events_spark_df.filter(
        F.col("LINE_ID") != 200
    )
    bus_transfer_events_spark_df = bus_transfer_events_spark_df.withColumn(
        "CONFIDENCE", F.col("ALIGHTING_PROBABILITY") * F.col("BOARDING_PROBABILITY")
    )
    # select most confident
    confidence_selection_window = Window.partitionBy("UNIQUE_ROW_ID").orderBy(
        F.col("CONFIDENCE").desc(),
        F.col("ALIGHTING_STOP_ARRIVE_DATETIME").asc(),
    )
    bus_transfer_events_spark_df = (
        bus_transfer_events_spark_df.withColumn(
            "ROW_ID", F.row_number().over(confidence_selection_window)
        )
        .filter(F.col("ROW_ID") == 1)
        .drop("ROW_ID")
    )
    max_transfer_events_spark_df = (
        max_transfer_events_spark_df.withColumn(
            "ROW_ID", F.row_number().over(confidence_selection_window)
        )
        .filter(F.col("ROW_ID") == 1)
        .drop("ROW_ID")
    )
    ###combine results
    selected_alighting_events = max_transfer_events_spark_df.unionByName(
        bus_transfer_events_spark_df
    )
    selected_alighting_events = selected_alighting_events.cache()
    return selected_alighting_events


def create_journey_ids_based_on_headway(
    rider_events_spark_df: SparkDF,
    wait_time_multiple: int,
) -> SparkDF:
    """Split up the journeys based on whether or not the rider missed greater than a variable threshold.
    This threshold takes the form of a sigmoid, centered around a "wait_time_multiple". If the headway
    is less than the wait time multiple and the wait time is greater than (2-3, depending on the headway)
    times the headway, then the trip is split. If, on the other hand the wait time is greater than the wait_time_multiple
    and the wait time is greater than (1-2, depending on the headway) times the headway, then the trip is split. Journey
    IDs are based on a SHA-2 hash of CARD_ID and DATETIME of the initial boarding tap.

    Args:
        rider_events_spark_df (SparkDF): Rider event history with boarding times, alighting times and headways
        wait_time_multiple (int): Multiple of minutes that are considered "painful"

    Returns:
        SparkDF: SparkDF where journeys have been further split up according to variable thresholds.
    """
    next_headway_window = Window.partitionBy("CARD_ID").orderBy(F.col("DATETIME").asc())
    rider_events_spark_df = rider_events_spark_df.withColumn(
        "MEAN_BOARDING_INTERVAL_SECONDS_NEXT",
        F.lead(F.col("MEAN_BOARDING_INTERVAL_SECONDS"), 1).over(next_headway_window),
    )
    rider_events_spark_df = rider_events_spark_df.withColumn(
        "INFERRED_WAIT_TIME_SECONDS",
        (
            (
                F.col("DATETIME_NEXT").cast("long")
                - F.col("ALIGHTING_STOP_ARRIVE_DATETIME").cast("long")
            )
            - F.col("MINIMUM_REQUIRED_TRANSFER_TIME_SECONDS")
        ),
    )
    rider_events_spark_df = rider_events_spark_df.withColumn(
        "IS_JOURNEY_END",
        F.when(
            (
                (
                    F.col("INFERRED_WAIT_TIME_SECONDS")
                    / F.col("MEAN_BOARDING_INTERVAL_SECONDS_NEXT")
                )
                > 3
                - 2
                * (
                    1
                    / (
                        1
                        + F.exp(
                            -1
                            * (
                                F.col("MEAN_BOARDING_INTERVAL_SECONDS_NEXT")
                                - wait_time_multiple
                            )
                        )
                        ** (1 / 2)
                    )
                )
            )
            | F.col("INFERRED_WAIT_TIME_SECONDS").isNull()
            | F.col("MEAN_BOARDING_INTERVAL_SECONDS_NEXT").isNull()
            | (F.col("INFERRED_WAIT_TIME_SECONDS") > 7200),
            True,
        ).otherwise(False),
    )
    rider_events_spark_df = rider_events_spark_df.withColumn(
        "JOURNEY_ID",
        F.when(
            (F.lag(F.col("IS_JOURNEY_END"), 1).over(next_headway_window) == True)
            | F.lag(F.col("IS_JOURNEY_END"), 1).over(next_headway_window).isNull(),
            F.sha2(
                F.concat(
                    F.col("CARD_ID").cast(StringType()),
                    F.col("DATETIME").cast(StringType()),
                ),
                256,
            ),
        ).otherwise(None),
    )
    new_journey_window = Window.partitionBy(F.col("CARD_ID")).orderBy(
        F.col("DATETIME").asc()
    )
    rider_events_spark_df = rider_events_spark_df.withColumn(
        "JOURNEY_ID",
        F.last(F.col("JOURNEY_ID"), ignorenulls=True).over(new_journey_window),
    ).withColumn(
        "JOURNEY_ID",
        F.when(
            F.col("JOURNEY_ID").isNull(),
            F.sha2(
                F.concat(
                    F.col("CARD_ID").cast(StringType()),
                    F.col("DATETIME").cast(StringType()),
                ),
                256,
            ),
        ).otherwise(F.col("JOURNEY_ID")),
    )
    return rider_events_spark_df


def calculate_validity_score(rider_events_spark_df: SparkDF) -> SparkDF:
    window = Window.partitionBy("JOURNEY_ID").orderBy(F.col("DATETIME"))
    rider_events_spark_df = rider_events_spark_df.withColumn(
        "STOP_LAT_NEXT_ORIGIN", F.last(F.col("STOP_LAT_NEXT")).over(window)
    ).withColumn("STOP_LON_NEXT_ORIGIN", F.last(F.col("STOP_LON_NEXT")).over(window))
    rider_events_spark_df = (
        rider_events_spark_df.withColumn(
            "ORIGIN_ORIGIN_DISTANCE_METERS",
            haversine_meters(
                F.col("STOP_LAT"),
                F.col("STOP_LON"),
                F.col("STOP_LAT_NEXT_ORIGIN"),
                F.col("STOP_LON_NEXT_ORIGIN"),
            ),
        )
        .withColumn(
            "MAX_TRANSFER_DISTANCE_METERS",
            F.max(F.col("TRANSFER_DISTANCE_METERS")).over(window),
        )
        .withColumn(
            "VALIDITY_SCORE",
            1
            / (
                1
                + F.col("MAX_TRANSFER_DISTANCE_METERS")
                / F.col("ORIGIN_ORIGIN_DISTANCE_METERS")
            ),
        )
        .withColumn(
            "VALIDITY_SCORE",
            F.when(F.col("ORIGIN_ORIGIN_DISTANCE_METERS") == 0, 0).otherwise(
                F.col("VALIDITY_SCORE")
            ),
        )
    )
    return rider_events_spark_df


def remove_impossible_journeys(
    hop_events_spark_df: SparkDF,
    max_time_to_destination_days: int,
) -> Tuple[SparkDF, SparkDF]:
    """Some journeys may include impossible events given the available time.
        We remove these. Impossible conditions are detailed in impossible_conditions_and_descriptions.py

    Args:
        hop_events_spark_df (SparkDF): Hop events SparkDF.
        max_time_to_destination_days (int): Max time between taps to allow for journey to be considered valid.

    Returns:
        Tuple[SparkDF, SparkDF]: First SparkDF is all possible journeys, second is all impossible journeys
    """
    window = Window.partitionBy(F.col("CARD_ID"), F.col("JOURNEY_ID")).orderBy(
        F.col("DATETIME").asc()
    )
    hop_events_spark_df = hop_events_spark_df.select(
        *hop_events_spark_df.columns,
        F.array_except(
            F.array(
                *[
                    F.when(
                        condition_description[0], condition_description[1]
                    ).otherwise(None)
                    for _, condition_description in enumerate(
                        get_impossible_conditions_and_descriptions(
                            max_time_to_destination_days
                        )
                    )
                ]
            ),
            F.array(F.lit(None)),
        ).alias("REASONS_FOR_IMPOSSIBILITY"),
    )
    hop_events_spark_df = hop_events_spark_df.withColumn(
        "IS_IMPOSSIBLE",
        F.sum(F.size(F.col("REASONS_FOR_IMPOSSIBILITY"))).over(
            window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        ),
    ).cache()
    return hop_events_spark_df.filter(F.col("IS_IMPOSSIBLE") == 0).drop(
        "IS_IMPOSSIBLE", "REASONS_FOR_IMPOSSIBILITY"
    ), hop_events_spark_df.filter(F.col("IS_IMPOSSIBLE") > 0).drop("IS_IMPOSSIBLE")


def get_interlining_events(
    hop_events_spark_df: SparkDF, stop_times_spark_df: SparkDF, allow_interlining: bool
) -> SparkDF:
    """Insert interlining events into the hop events. Interlining trips are then used
        to replace original trips.

    Args:
        hop_events_spark_df (SparkDF): Hop events representing inferred journeys.
        stop_times_spark_df (SparkDF): Stop times trip ids that include interlining events.

    Returns:
        SparkDF: Returns a SparkDF with interlining events inserted into the hop events.
    """
    if allow_interlining:
        interlining_locations_spark_df = stop_times_spark_df.filter(
            F.col("STOP_TYPE") == INTERLINING_STOP_TYPE
        )
        interlining_hop_event_legs = hop_events_spark_df.filter(
            F.col("BOARDING_STOP_LINE_ID_OLD") != F.col("ALIGHTING_STOP_ROUTE_ID")
        )
        interlining_locations_spark_df = interlining_locations_spark_df.select(
            F.col("STOP_SEQUENCE").alias("INTERLINING_STOP_SEQUENCE"),
            F.col("TRIP_ID").alias("INTERLINING_TRIP_ID"),
            F.col("SERVICE_DATE").alias("INTERLINING_SERVICE_DATE"),
            F.col("STOP_ID").alias("INTERLINING_STOP_ID"),
            F.col("STOP_LAT").alias("INTERLINING_STOP_LAT"),
            F.col("STOP_LON").alias("INTERLINING_STOP_LON"),
            F.col("ARRIVE_DATETIME").alias("INTERLINING_ARRIVE_DATETIME"),
            F.col("DEPARTURE_DATETIME").alias("INTERLINING_DEPARTURE_DATETIME"),
            F.col("STOP_TYPE"),
        )
        interlining_hop_event_legs = interlining_hop_event_legs.join(
            interlining_locations_spark_df,
            (
                (
                    interlining_locations_spark_df["INTERLINING_TRIP_ID"]
                    == interlining_hop_event_legs["TRIP_ID"]
                )
                & (
                    interlining_locations_spark_df["INTERLINING_STOP_SEQUENCE"]
                    < interlining_hop_event_legs["ALIGHTING_STOP_STOP_SEQUENCE"]
                )
                & (
                    interlining_locations_spark_df["INTERLINING_STOP_SEQUENCE"]
                    > interlining_hop_event_legs["BOARDING_STOP_STOP_SEQUENCE"]
                )
                & (
                    interlining_locations_spark_df["INTERLINING_SERVICE_DATE"]
                    == interlining_hop_event_legs["SERVICE_DATE"]
                )
            ),
        )

        return interlining_hop_event_legs
    return hop_events_spark_df.filter(F.lit(False))


def reshape_failed_journeys(failed_journeys_spark_df: SparkDF) -> SparkDF:
    """Reshape the failed journeys into the same shape as the inferred rider events.

    Args:
        failed_journeys_spark_df (SparkDF): SparkDF of failed journeys.

    Returns:
        SparkDF: Reshaped failed journeys SparkDF.
    """
    # format non-final boardings.
    boardings = (
        failed_journeys_spark_df.select(
            "DATETIME",
            "STOP_ID",
            "CARD_ID",
            "JOURNEY_ID",
            F.col("BOARDING_STOP_LINE_ID_OLD").alias("LINE_ID"),
            "STOP_LAT",
            "STOP_LON",
            "DIRECTION_ID",
            "SERVICE_DATE",
            "REASONS_FOR_IMPOSSIBILITY",
        )
        .withColumn("EVENT", F.lit("BOARDED"))
        .withColumn("CONFIDENCE", F.lit(1))
    )
    # get alightings
    alightings = (
        failed_journeys_spark_df.select(
            F.col("ALIGHTING_STOP_ARRIVE_DATETIME").alias("DATETIME"),
            F.col("ALIGHTING_STOP_ID").alias("STOP_ID"),
            "CARD_ID",
            "JOURNEY_ID",
            F.col("ALIGHTING_STOP_ROUTE_ID").alias("LINE_ID"),
            F.col("ALIGHTING_STOP_LAT").alias("STOP_LAT"),
            F.col("ALIGHTING_STOP_LON").alias("STOP_LON"),
            "DIRECTION_ID",
            "CONFIDENCE",
            "STOP_ID_NEXT",
            "SERVICE_DATE",
            "REASONS_FOR_IMPOSSIBILITY",
        )
        .withColumn("EVENT", F.lit("ALIGHTED"))
        .filter(~F.col("STOP_ID_NEXT").isNull())
        .drop("STOP_ID_NEXT")
    )
    return boardings.unionByName(alightings, allowMissingColumns=True)


def reshape_journeys(
    inferred_transfers_spark_df: SparkDF, interlining_events: SparkDF
) -> SparkDF:
    """Journeys are reshaped into boarding and alighting events.

    Args:
        inferred_transfers_spark_df (SparkDF): Inferred transfers SparkDF.

    Returns:
        SparkDF: Hop inferred transfers reshaped into an event history.
        Each row contributes a boarding and an alighting (except for the rows from the final boardings)
    """
    # format non-final boardings.
    boardings = (
        inferred_transfers_spark_df.select(
            F.col("FARE_CATEGORY_DESCRIPTION"),
            F.col("ARRIVE_DATETIME").alias("DATETIME"),
            "STOP_ID",
            "CARD_ID",
            "JOURNEY_ID",
            F.col("BOARDING_STOP_LINE_ID_OLD").alias(
                "LINE_ID"
            ),  # use GTFS line ids not HOP
            "STOP_LAT",
            "STOP_LON",
            F.col("STOP_DIRECTION_ID").alias("DIRECTION_ID"),  # use gtfs direction id,
            "VALIDITY_SCORE",
        )
        .withColumn("EVENT", F.lit("BOARDED"))
        .withColumn("CONFIDENCE", F.lit(1))
    )
    # get alightings
    alightings = (
        inferred_transfers_spark_df.filter(~F.col("DATETIME_NEXT").isNull())
        .select(
            "FARE_CATEGORY_DESCRIPTION",
            F.col("ALIGHTING_STOP_ARRIVE_DATETIME").alias("DATETIME"),
            F.col("ALIGHTING_STOP_ID").alias("STOP_ID"),
            "CARD_ID",
            "JOURNEY_ID",
            F.col("ALIGHTING_STOP_ROUTE_ID").alias(
                "LINE_ID"
            ),  # USE GTFS LINE IDS NOT HOP
            F.col("ALIGHTING_STOP_LAT").alias("STOP_LAT"),
            F.col("ALIGHTING_STOP_LON").alias("STOP_LON"),
            F.col("STOP_DIRECTION_ID").alias("DIRECTION_ID"),
            "CONFIDENCE",
            "VALIDITY_SCORE",
        )
        .withColumn("EVENT", F.lit("ALIGHTED"))
    ).filter(
        ~F.col("STOP_ID").isNull()
    )  # remove last unknown alighting
    # get interlining events
    if interlining_events.first() is not None:
        interlining_events = (
            interlining_events.filter(~F.col("INTERLINING_STOP_ID").isNull())
            .filter(~F.col("INTERLINING_STOP_LAT").isNull())
            .filter(~F.col("DATETIME_NEXT").isNull())
            .filter(F.col("INTERLINING_ARRIVE_DATETIME") > F.col("ARRIVE_DATETIME"))
            .filter(
                F.col("INTERLINING_DEPARTURE_DATETIME")
                < F.col("ALIGHTING_STOP_ARRIVE_DATETIME")
            )
        )
        interlinings_starts = interlining_events.select(
            "FARE_CATEGORY_DESCRIPTION",
            F.col("INTERLINING_ARRIVE_DATETIME").alias("DATETIME"),
            F.col("INTERLINING_STOP_ID").alias("STOP_ID"),
            "CARD_ID",
            "JOURNEY_ID",
            F.col("BOARDING_STOP_LINE_ID_OLD").alias("LINE_ID"),
            F.col("INTERLINING_STOP_LAT").alias("STOP_LAT"),
            F.col("INTERLINING_STOP_LON").alias("STOP_LON"),
            F.col("STOP_DIRECTION_ID").alias("DIRECTION_ID"),
            "CONFIDENCE",
            "VALIDITY_SCORE",
        ).withColumn("EVENT", F.lit("INTERLINE_STARTED"))
        interlinings_ends = interlining_events.select(
            "FARE_CATEGORY_DESCRIPTION",
            F.col("INTERLINING_DEPARTURE_DATETIME").alias("DATETIME"),
            F.col("INTERLINING_STOP_ID").alias("STOP_ID"),
            "CARD_ID",
            "JOURNEY_ID",
            F.col("ALIGHTING_STOP_ROUTE_ID").alias("LINE_ID"),
            F.col("INTERLINING_STOP_LAT").alias("STOP_LAT"),
            F.col("INTERLINING_STOP_LON").alias("STOP_LON"),
            F.col("STOP_DIRECTION_ID").alias("DIRECTION_ID"),
            "CONFIDENCE",
            "VALIDITY_SCORE",
        ).withColumn("EVENT", F.lit("INTERLINE_ENDED"))
        interlinings = interlinings_starts.unionByName(interlinings_ends)
        events = boardings.unionByName(alightings).unionByName(interlinings)
    else:
        events = boardings.unionByName(alightings)
    return events


def get_event_type(action_history_spark_df: SparkDF) -> SparkDF:
    """Tag each event with either ORIGIN, MID_JOURNEY or DESTINATION.

    Args:
        action_history_spark_df (SparkDF): Rider action history of boardings and alightings.

    Returns:
        SparkDF: Rider action history where the event type column has been added.
    """
    history_window = Window.partitionBy("CARD_ID").orderBy(F.col("DATETIME").asc())
    action_window = Window.partitionBy("CARD_ID", "JOURNEY_ID").orderBy(
        F.col("DATETIME").asc()
    )
    interlines = action_history_spark_df.filter(F.col("EVENT").contains("INTERLINE"))
    non_interlines = action_history_spark_df.filter(
        ~F.col("EVENT").contains("INTERLINE")
    )
    non_interlines = (
        non_interlines.withColumn("ROW_ID", F.row_number().over(history_window))
        .withColumn(
            "MAX_ROW_ID",
            F.max(F.col("ROW_ID")).over(
                history_window.rowsBetween(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        )
        .withColumn(
            "IS_FINAL_EVENT",
            F.when((F.col("ROW_ID") == F.col("MAX_ROW_ID")), True).otherwise(False),
        )
    ).drop("ROW_ID", "MAX_ROW_ID")
    non_interlines = (
        non_interlines.withColumn("ROW_ID", F.row_number().over(action_window))
        .withColumn(
            "MAX_ROW_ID",
            F.max(F.col("ROW_ID")).over(
                action_window.rowsBetween(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        )
        .withColumn(
            "EVENT_TYPE",
            F.when((F.col("EVENT") == "BOARDED") & (F.col("ROW_ID") == 1), "ORIGIN")
            .when(  # first boarding in a journey
                (F.col("ROW_ID") == F.col("MAX_ROW_ID"))  # final alighting in a journey
                & (F.col("EVENT") == "ALIGHTED"),
                "DESTINATION",
            )
            .when(
                (
                    F.col("ROW_ID") == F.col("MAX_ROW_ID")
                )  # final boardings included in non single-event journeys
                & (F.col("EVENT") == "BOARDED")
                & (F.col("MAX_ROW_ID") > 1)
                & F.col("IS_FINAL_EVENT"),
                "MID_JOURNEY",
            )
            .otherwise("MID_JOURNEY"),
        )
        .drop("ROW_ID", "MAX_ROW_ID", "IS_FINAL_EVENT")
    )
    interlines = interlines.withColumn("EVENT_TYPE", F.lit("MID_JOURNEY"))
    action_history_spark_df = interlines.unionByName(non_interlines)
    return action_history_spark_df


def detect_looped_trips_according_to_threshold(
    action_history_spark_df: SparkDF, loop_threshold: int
) -> SparkDF:
    """Look for and tag loop journeys (journeys where the origin and destination stop are within loop_threshold of each other.)

    Args:
        action_history_spark_df (SparkDF): Rider event action history SparkDF
        loop_threshold (int): Threshold in meters for the origin/destination points to be within for a trip to be considered a loop.

    Returns:
        SparkDF: SparkDF where IS_LOOP has been added.
    """
    action_history_spark_df = action_history_spark_df.cache()
    window = (
        Window.partitionBy("JOURNEY_ID")
        .orderBy(F.col("DATETIME").asc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    action_history_spark_df = action_history_spark_df.withColumn(
        "IS_LOOP",
        F.when(
            haversine_meters(
                F.last(F.col("STOP_LAT")).over(window),
                F.last(F.col("STOP_LON")).over(window),
                F.first(F.col("STOP_LAT")).over(window),
                F.first(F.col("STOP_LON")).over(window),
            )
            < loop_threshold,
            True,
        ).otherwise(False),
    )
    return action_history_spark_df


def get_journey_start_dates(rider_events_spark_df: SparkDF) -> SparkDF:
    """Get the start date per journey.

    Args:
        rider_events_spark_df (SparkDF): SparkDF of rider events to write out

    Returns:
        SparkDF: Rider events with journey start date
    """
    window = Window.partitionBy("JOURNEY_ID").orderBy(F.col("DATETIME").asc())
    return rider_events_spark_df.withColumn(
        "JOURNEY_START_DATE",
        F.to_date(
            F.min(F.col("DATETIME")).over(
                window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            "YYYY-MM-DD",
        ),
    ).repartition(F.col("JOURNEY_START_DATE"))


def remove_unexpected_journey_event_sequences(
    successful_journeys_spark_df: SparkDF,
) -> SparkDF:
    """Validate that only journeys including interlining events have
    changes in line id between a boarding and alighting.

    Args:
        successful_journeys_spark_df (SparkDF): Successfully formatted journeys.

    Returns:
        SparkDF: Original SparkDF with unsuccessful journeys removed.
    """
    null_event_string = "NONE"
    allowed_event_pairs = [
        "BOARDED-ALIGHTED",
        "BOARDED-INTERLINE_STARTED",
        "INTERLINE_STARTED-INTERLINE_ENDED",
        "INTERLINE_ENDED-INTERLINE_STARTED",
        "INTERLINE_ENDED-ALIGHTED",
        "ALIGHTED-BOARDED",
        f"ALIGHTED-{null_event_string}",
    ]
    event_window = Window.partitionBy("JOURNEY_ID").orderBy(F.col("DATETIME").asc())
    successful_journeys_spark_df = successful_journeys_spark_df.withColumn(
        "NEXT_EVENT", F.lead(F.col("EVENT"), 1).over(event_window)
    ).withColumn(
        "NEXT_EVENT",
        F.when(F.col("NEXT_EVENT").isNotNull(), F.col("NEXT_EVENT")).otherwise(
            null_event_string
        ),
    )
    successful_journeys_spark_df = successful_journeys_spark_df.withColumn(
        "EVENT_PAIR", F.concat_ws("-", F.col("EVENT"), F.col("NEXT_EVENT"))
    )
    successful_journeys_spark_df = successful_journeys_spark_df.withColumn(
        "UNEXPECTED_EVENT_PAIR",
        F.sum(
            F.when(F.col("EVENT_PAIR").isin(allowed_event_pairs), 0).otherwise(1)
        ).over(
            event_window.rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )
        ),
    )
    return successful_journeys_spark_df.filter(
        F.col("UNEXPECTED_EVENT_PAIR") == 0
    ).drop("NEXT_EVENT", "UNEXPECTED_EVENT_PAIR", "EVENT_PAIR")


def validate_successful_journeys(successful_journeys_spark_df: SparkDF) -> SparkDF:
    """Validate that the successful journeys conform to expectations. Assert that no nulls
       are in any of the columns.

    Args:
        successful_journeys_spark_df (SparkDF): SparkDF of successful journeys to validate.

    Returns:
        SparkDF: If passes, return the original SparkDF.
    """
    successful_journeys_spark_df = successful_journeys_spark_df.cache()
    # ensure that columns aren't null
    for column in successful_journeys_spark_df.columns:
        contains_no_nulls = (
            successful_journeys_spark_df.filter(F.col(column).isNull()).first() is None
        )
        assert (
            contains_no_nulls
        ), f"Error: Nulls detected in {column} for successful journeys."
    return successful_journeys_spark_df


def remove_journeys_with_no_destination(action_history_spark_df: SparkDF) -> SparkDF:
    """Remove journeys that have no destination. Unclear why this currently happens.

    Args:
        action_history_spark_df (SparkDF): History of rider actions

    Returns:
        SparkDF: SparkDF with destinationless journeys removed.
    """
    window = Window.partitionBy("JOURNEY_ID").orderBy("DATETIME")
    action_history_spark_df = action_history_spark_df.withColumn(
        "HAS_DESTINATION",
        F.sum(F.when(F.col("EVENT_TYPE") == "DESTINATION", 1).otherwise(0)).over(
            window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        ),
    )
    return action_history_spark_df.filter(F.col("HAS_DESTINATION") > 0).drop(
        "HAS_DESTINATION"
    )


def get_run_metrics(
    successful_journeys_spark_df: SparkDF, failed_journeys_spark_df: SparkDF
) -> dict:
    """Build some summary stats for the failures in the runs

    Args:
        successful_journeys_spark_df (SparkDF): SparkDF of successful journey inferences.
        failed_journeys_spark_df (SparkDF): SparkDF of failed journey inferences

    Returns:
        dict: Dictionary of metrics related to failures to infer.
    """
    successful_journeys_count = (
        successful_journeys_spark_df.select("JOURNEY_ID").dropDuplicates().count()
    )
    mean_confidence = (
        successful_journeys_spark_df.filter(F.col("EVENT") == "ALIGHTED")
        .select(F.mean(F.col("CONFIDENCE")))
        .toPandas()
        .values[0][0]  # type: ignore
    )
    failed_journeys_count = (
        failed_journeys_spark_df.select("JOURNEY_ID").dropDuplicates().count()
    )
    failure_reasons_and_dates = (
        failed_journeys_spark_df.select(
            "SERVICE_DATE",
            "JOURNEY_ID",
            F.explode(F.col("REASONS_FOR_IMPOSSIBILITY")).alias("REASON"),
        )
        .dropDuplicates()
        .groupBy("REASON", "SERVICE_DATE")
        .count()
        .toPandas()
    )
    reasons_for_failure = {
        reason: value
        for reason, value in failure_reasons_and_dates.groupby(
            by=["REASON"], as_index=False
        )["count"]
        .sum()
        .values
    }
    # Calculate interline ratio
    interline_ratio = (
        successful_journeys_spark_df.select(
            "JOURNEY_ID",
            F.when(F.col("EVENT").contains("INTERLINE"), 1)
            .otherwise(0)
            .alias("HAS_INTERLINE"),
        )
        .groupBy("JOURNEY_ID")
        .agg(F.max("HAS_INTERLINE").alias("HAS_INTERLINE"))
        .agg(
            F.count("JOURNEY_ID").alias("TOTAL_JOURNEYS"),
            F.sum("HAS_INTERLINE").alias("JOURNEYS_WITH_INTERLINE"),
        )
        .withColumn(
            "INTERLINE_RATIO",
            F.col("JOURNEYS_WITH_INTERLINE") / F.col("TOTAL_JOURNEYS"),
        )
        .select("INTERLINE_RATIO")
        .first()[0]
    )
    metrics = {
        "N_SUCCESSFUL": successful_journeys_count,
        "N_FAILED": failed_journeys_count,
        "PERCENT_FAILED": int(
            100
            * failed_journeys_count
            / (successful_journeys_count + failed_journeys_count)
        ),
        "MEAN_CONFIDENCE": mean_confidence,
        "PERCENT_INTERLINE": int(100 * interline_ratio),
        **reasons_for_failure,
    }
    logger.info(metrics)
    return metrics
