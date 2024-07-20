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

"""Nodes for inferring ODX from prepared Hop data.
"""
from functools import reduce
from typing import List, Tuple, Union

from loguru import logger
from pyspark.sql import DataFrame as SparkDF, functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from .impossible_conditions_and_descriptions import (
    get_impossible_conditions_and_descriptions,
)


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
        for year, month in year_months:
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
        (
            2
            * 6371000
            * F.asin(  # haversine formula
                F.sqrt(
                    F.sin(
                        (
                            F.radians(F.col("STOP_LAT"))
                            - F.radians(F.col("STOP_LAT_ALTERNATIVE"))
                        )
                        / 2
                    )
                    ** 2
                    + (
                        F.cos(F.radians(F.col("STOP_LAT")))
                        * F.cos(F.radians(F.col("STOP_LAT_ALTERNATIVE")))
                        * F.sin(
                            (
                                F.radians(F.col("STOP_LON"))
                                - F.radians("STOP_LON_ALTERNATIVE")
                            )
                            / 2
                        )
                        ** 2
                    )
                )
            )
        )
        < threshold,
        how="left",
    ).filter(
        (F.col("ROUTE_ID") == 200) | (F.col("STOP_ID") == F.col("STOP_ID_ALTERNATIVE"))
    )  # only allow MAX lines or the stop itself as an alternative
    return max_line_stops


def get_possible_boarding_trip_ids(
    possible_transfer_events_spark_df: SparkDF,
    stop_times_spark_df: SparkDF,
    max_line_boarding_alternatives: SparkDF,
) -> SparkDF:
    """This looks for the possible boarding vehicles given the tap time and the stop times (either planned or AVL). The
    likelihood of a rider boarding vehicle is evaluated according to the difference between the boarding time and the arrival
    time of the vehicle at the stop. The likelihood function is a normal distribution centered around 0 with a standard deviation of 0.75
    at that stop around that time evaluated at the boarding delta divided by the headway. Joins
    between taps and stop times are based on locality in time. Each tap gets at most 3 options for possible
    associated boarding events.

    Args:
        possible_transfer_events_spark_df (SparkDF): Possible Hop transfer events.
        stop_times_spark_df (SparkDF): Stop times dataframe
        max_line_boarding_alternatives (SparkDF): SparkDF of alternative boarding locations for max lines

    Returns:
        SparkDF: SparkDF where possible boarding trips have been merged onto boarding tap events.
    """
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
    # get boarding taps for max lines
    max_line_boarding_points = possible_transfer_events_spark_df.filter(
        F.col("LINE_ID") == 200
    )
    # insert alternative boarding locations
    max_line_boarding_points = max_line_boarding_points.join(
        max_line_boarding_alternatives,
        on="STOP_ID",
        how="left",
    )
    # rename alternatives to actual locations. The original location is always an alternative itself.
    max_line_boarding_points = (
        max_line_boarding_points.select(
            *[
                col
                for col in max_line_boarding_points.columns
                if col not in ["STOP_ID", "STOP_LAT", "STOP_LON", "ROUTE_ID"]
            ]
        )
        .withColumn("STOP_ID", F.col("STOP_ID_ALTERNATIVE"))
        .withColumn("STOP_LAT", F.col("STOP_LAT_ALTERNATIVE"))
        .withColumn("STOP_LON", F.col("STOP_LON_ALTERNATIVE"))
        .drop(
            *[col for col in max_line_boarding_points.columns if "ALTERNATIVE" in col]
        )
    )
    # handle non max lines
    non_max_line_boarding_points = possible_transfer_events_spark_df.filter(
        F.col("LINE_ID") != 200
    )
    # union the two back together
    possible_boarding_points = non_max_line_boarding_points.unionByName(
        max_line_boarding_points
    )
    # join the arrival datetimes of possible boarding points
    possible_boarding_stop_times = stop_times_spark_df.select(
        F.col("SERVICE_DATE").alias("STOP_SERVICE_DATE"),
        F.col("STOP_ID").alias("STOP_STOP_ID"),
        F.col("ROUTE_ID").alias("STOP_LINE_ID"),
        F.col("ROUTE_ID_OLD").alias("STOP_LINE_ID_OLD"),
        "ARRIVE_DATETIME",
        "DEPARTURE_DATETIME",
        "MEAN_BOARDING_INTERVAL_SECONDS",
        "TRIP_ID",
        "HOUR",
        F.col("DIRECTION_ID").alias("STOP_DIRECTION_ID"),
    )
    max_boardings = possible_boarding_points.filter(F.col("LINE_ID") == 200)
    non_max_boardings = possible_boarding_points.filter(F.col("LINE_ID") != 200)
    max_possible_boarding_points = max_boardings.join(
        possible_boarding_stop_times,
        (
            max_boardings["SERVICE_DATE"]
            == possible_boarding_stop_times["STOP_SERVICE_DATE"]
        )
        & (max_boardings["STOP_ID"] == possible_boarding_stop_times["STOP_STOP_ID"])
        & (max_boardings["LINE_ID"] == possible_boarding_stop_times["STOP_LINE_ID"])
        & (
            max_boardings["DATETIME"].cast("long")
            <= possible_boarding_stop_times["DEPARTURE_DATETIME"].cast("long")
        ),
        how="left",
    ).drop("STOP_STOP_ID", "STOP_LINE_ID", "STOP_SERVICE_DATE")
    non_max_possible_boardings = non_max_boardings.join(
        possible_boarding_stop_times,
        (
            non_max_boardings["SERVICE_DATE"]
            == possible_boarding_stop_times["STOP_SERVICE_DATE"]
        )
        & (non_max_boardings["STOP_ID"] == possible_boarding_stop_times["STOP_STOP_ID"])
        & (non_max_boardings["LINE_ID"] == possible_boarding_stop_times["STOP_LINE_ID"])
        & (
            non_max_boardings["DATETIME"].cast("long")
            >= possible_boarding_stop_times["ARRIVE_DATETIME"].cast("long")
        ),
        how="left",
    ).drop("STOP_STOP_ID", "STOP_LINE_ID", "STOP_SERVICE_DATE")
    max_departure_window = Window.partitionBy(
        F.col("UNIQUE_ROW_ID"),
        F.col("STOP_LINE_ID_OLD"),
        F.col("STOP_DIRECTION_ID"),
    ).orderBy(F.col("DEPARTURE_DATETIME").asc())
    # We differentiate here because we added in alternative boarding locations, which means alternative lines and directions as well
    max_possible_boarding_points = (
        max_possible_boarding_points.withColumn(
            "ROW_ID", F.row_number().over(max_departure_window)
        )
        .filter(
            F.col("ROW_ID") <= 2
        )  # only select two options per stop time line id and direction id. Either you take it (1st option) or you miss the first and take the second
        .drop("ROW_ID")
    )
    non_max_arrival_window = Window.partitionBy(F.col("UNIQUE_ROW_ID")).orderBy(
        F.col("ARRIVE_DATETIME").desc()
    )
    non_max_possible_boardings = (
        non_max_possible_boardings.withColumn(
            "ROW_ID", F.row_number().over(non_max_arrival_window)
        )
        .filter(F.col("ROW_ID") <= 3)  # only select three options to handle bus bunching
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
    non_max_possible_boardings = non_max_possible_boardings.withColumn(
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
    possible_boardings = max_possible_boardings.unionByName(non_max_possible_boardings)
    unique_row_window = Window.partitionBy(
        F.col("UNIQUE_ROW_ID"), F.col("STOP_LINE_ID_OLD"), F.col("STOP_DIRECTION_ID")
    ).orderBy(F.col("BOARDING_PROBABILITY").asc())#normalize along a line and direction
    return possible_boardings.withColumn(
        "BOARDING_PROBABILITY",
        F.col("BOARDING_PROBABILITY")
        / F.sum(F.col("BOARDING_PROBABILITY")).over(
            unique_row_window.rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )
        ),
    )


def get_possible_alighting_points(
    transfer_events_with_trip_id_spark_df: SparkDF,
    stop_times_spark_df: SparkDF,
    interlining_trip_ids_spark_df: SparkDF,
    allow_interlining: bool,
) -> SparkDF:
    """Get the possible alighting points based on the boarding trip id, the next tap location and the next tap time.

    Args:
        transfer_events_with_trip_id_spark_df (SparkDF): Hop transfer events
        stop_times_spark_df (SparkDF): Stop times.
        interlining_trip_ids_spark_df (SparkDF): Maps between original trip ids and interlining trip ids
        allow_interlining (bool): Whether or not to allow interlining

    Returns:
        SparkDF: SparkDF with candidate alighting locations inserted as new rows
    """
    if allow_interlining:
        interlining_trip_ids_spark_df = interlining_trip_ids_spark_df.withColumn(
            "IS_INTERLINING_TRIP",
            F.when(F.col("TRIP_ID") != F.col("INTERLINING_TRIP_ID"), True).otherwise(
                False
            ),
        )
        transfer_events_with_trip_id_spark_df = (
            transfer_events_with_trip_id_spark_df.join(
                F.broadcast(interlining_trip_ids_spark_df),
                on=["SERVICE_DATE", "TRIP_ID"],
                how="left",
            )
            .drop("TRIP_ID", "IS_INTERLINING_TRIP")
            .withColumnRenamed("INTERLINING_TRIP_ID", "TRIP_ID")
        )
        stop_times_spark_df_w_interlining_information_spark_df = (
            stop_times_spark_df.join(
                F.broadcast(interlining_trip_ids_spark_df),
                on=["SERVICE_DATE", "TRIP_ID"],
                how="left",
            )
            .drop("TRIP_ID")
            .withColumnRenamed("INTERLINING_TRIP_ID", "TRIP_ID")
        )
        stop_times_spark_df = (
            stop_times_spark_df_w_interlining_information_spark_df.select(
                F.col("SERVICE_DATE").alias("STOP_TIME_SERVICE_DATE"),
                F.col("ROUTE_ID").alias("ALIGHTING_LINE_ID"),
                F.col("ROUTE_ID_OLD").alias("ALIGHTING_LINE_ID_OLD"),
                F.col("TRIP_ID").alias("STOP_TIME_TRIP_ID"),
                F.col("IS_INTERLINING_TRIP"),
                F.col("STOP_ID").alias("ALIGHTING_STOP_ID"),
                F.col("ARRIVE_DATETIME").alias("ALIGHTING_ARRIVE_DATETIME"),
                F.col("DEPARTURE_DATETIME").alias("ALIGHTING_DEPARTURE_DATETIME"),
                F.col("STOP_LAT").alias("ALIGHTING_STOP_LAT"),
                F.col("STOP_LON").alias("ALIGHTING_STOP_LON"),
            )
        )
    possible_transfer_events_spark_df = (
        transfer_events_with_trip_id_spark_df.join(
            stop_times_spark_df,
            (
                (
                    stop_times_spark_df["STOP_TIME_SERVICE_DATE"]
                    == transfer_events_with_trip_id_spark_df["SERVICE_DATE"]
                )
                & (
                    stop_times_spark_df["STOP_TIME_TRIP_ID"]
                    == transfer_events_with_trip_id_spark_df["TRIP_ID"]
                )
                & (
                    stop_times_spark_df["ALIGHTING_ARRIVE_DATETIME"].cast("long")
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
                    < stop_times_spark_df["ALIGHTING_ARRIVE_DATETIME"].cast("long")
                )
                & (
                    transfer_events_with_trip_id_spark_df["DATETIME"].cast("long")
                    < stop_times_spark_df["ALIGHTING_ARRIVE_DATETIME"].cast("long")
                )
            ),
            how="left",
        )
    ).drop("STOP_TIME_SERVICE_DATE")
    return (
        possible_transfer_events_spark_df,
        stop_times_spark_df_w_interlining_information_spark_df,
    )


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
    transfer_events_with_trip_id_spark_df = (
        (
            transfer_events_with_trip_id_spark_df.withColumn(
                "TRANSFER_DISTANCE_METERS",
                (
                    2 ** (0.5)
                )  # sqrt(2) to control for grid. This is important because the linear distance is not necessarily the distance walked.
                * 2
                * 6371000
                * F.asin(
                    # haversine formula.
                    F.sqrt(
                        F.sin(
                            (
                                F.radians(F.col("ALIGHTING_STOP_LAT"))
                                - F.radians(F.col("STOP_LAT_NEXT"))
                            )
                            / 2
                        )
                        ** 2
                        + (
                            F.cos(F.radians(F.col("ALIGHTING_STOP_LAT")))
                            * F.cos(F.radians(F.col("STOP_LAT_NEXT")))
                            * F.sin(
                                (
                                    F.radians(F.col("STOP_LON_NEXT"))
                                    - F.radians("ALIGHTING_STOP_LON")
                                )
                                / 2
                            )
                            ** 2
                        )
                    )
                ),
            )
        )
        .withColumn(
            "MINIMUM_REQUIRED_TRANSFER_TIME_SECONDS",
            F.col("TRANSFER_DISTANCE_METERS") / F.lit(walking_speed_meters_second),
        )
        .withColumn(
            "TOTAL_WALKING_TRAVEL_TIME_SECONDS",
            (
                (
                    2 ** (0.5)
                )  # sqrt(2) to control for grid. This is important because the linear distance is not necessarily the distance walked.
                * 2
                * 6371000
                * F.asin(
                    # haversine formula. TODO: Write a reusable function as this is useful. Not sure how we would test it though.
                    F.sqrt(
                        F.sin(
                            (
                                F.radians(F.col("STOP_LAT"))
                                - F.radians(F.col("STOP_LAT_next"))
                            )
                            / 2
                        )
                        ** 2
                        + (
                            F.cos(F.radians(F.col("STOP_LAT")))
                            * F.cos(F.radians(F.col("STOP_LAT_next")))
                            * F.sin(
                                (
                                    F.radians(F.col("STOP_LON_next"))
                                    - F.radians("STOP_LON")
                                )
                                / 2
                            )
                            ** 2
                        )
                    )
                )
            )
            / F.lit(walking_speed_meters_second),
        )
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
        F.col("ALIGHTING_DEPARTURE_DATETIME").asc()
    )
    possible_transfer_events_spark_df = (
        possible_transfer_events_spark_df.withColumn(
            "TRAVEL_TIME_SECONDS",
            (
                F.col("ALIGHTING_ARRIVE_DATETIME").cast("long")
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
    if allow_no_stops:
        possible_transfer_events_spark_df = (
            possible_transfer_events_spark_df.withColumn(
                "STOP_ALIGHTING_PROBABILITY",
                (
                    F.lit(0.5)
                    * (
                        F.lit(1)
                        / (
                            F.col("MAX_TRAVEL_TIME_SECONDS")
                            - F.col("MIN_TRAVEL_TIME_SECONDS")
                        )
                    )
                )
                ** 2
                / (
                    (F.col("TRAVEL_TIME_SECONDS") - F.col("MIN_TRAVEL_TIME_SECONDS"))
                    ** 2
                    + (
                        F.lit(0.5)
                        * (
                            F.lit(1)
                            / (
                                F.col("MAX_TRAVEL_TIME_SECONDS")
                                - F.col("MIN_TRAVEL_TIME_SECONDS")
                            )
                        )
                    )
                    ** 2
                ),
            )
        )
    else:
        possible_transfer_events_spark_df = (
            possible_transfer_events_spark_df.withColumn(
                "STOP_ALIGHTING_PROBABILITY",
                F.when(
                    F.col("ALIGHTING_ARRIVE_DATETIME")
                    != F.col("ALIGHTING_DEPARTURE_DATETIME"),
                    (
                        F.lit(0.5)
                        * (
                            F.lit(1)
                            / (
                                F.col("MAX_TRAVEL_TIME_SECONDS")
                                - F.col("MIN_TRAVEL_TIME_SECONDS")
                            )
                        )
                    )
                    ** 2
                    / (
                        (
                            F.col("TRAVEL_TIME_SECONDS")
                            - F.col("MIN_TRAVEL_TIME_SECONDS")
                        )
                        ** 2
                        + (
                            F.lit(0.5)
                            * (
                                F.lit(1)
                                / (
                                    F.col("MAX_TRAVEL_TIME_SECONDS")
                                    - F.col("MIN_TRAVEL_TIME_SECONDS")
                                )
                            )
                        )
                        ** 2
                    ),
                ).otherwise(0),
            )
        )

    possible_transfer_events_spark_df = (
        possible_transfer_events_spark_df.withColumn(
            "ALIGHTING_PROBABILITY",
            F.product(F.lit(1) - F.col("STOP_ALIGHTING_PROBABILITY")).over(
                within_trip_window.rowsBetween(Window.unboundedPreceding, -1)
            )
            * F.col("STOP_ALIGHTING_PROBABILITY"),
        )
        .withColumn(
            "MAX_ALIGHTING_PROBABILITY",
            F.max(F.col("ALIGHTING_PROBABILITY")).over(
                within_trip_window.rowsBetween(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        )
        .withColumn(
            "ALIGHTING_PROBABILITY",
            F.when(
                F.col("MAX_ALIGHTING_PROBABILITY") == 0,
                (
                    F.lit(0.5)
                    * (
                        F.lit(1)
                        / (
                            F.col("MAX_TRAVEL_TIME_SECONDS")
                            - F.col("MIN_TRAVEL_TIME_SECONDS")
                        )
                    )
                )
                ** 2
                / (
                    (F.col("TRAVEL_TIME_SECONDS") - F.col("MIN_TRAVEL_TIME_SECONDS"))
                    ** 2
                    + (
                        F.lit(0.5)
                        * (
                            F.lit(1)
                            / (
                                F.col("MAX_TRAVEL_TIME_SECONDS")
                                - F.col("MIN_TRAVEL_TIME_SECONDS")
                            )
                        )
                    )
                    ** 2
                ),
            ).otherwise(F.col("ALIGHTING_PROBABILITY")),
        )
    )
    return possible_transfer_events_spark_df


def select_alighting_based_on_overall_probability(
    possible_transfer_events_spark_df: SparkDF,
) -> SparkDF:
    """Combine the boarding probability and the alighting probability to provide an
    overall probability that the rider alighted at a location at a specific time.

    Args:
        possible_transfer_events_spark_df (SparkDF): SparkDF of all possible boarding and alighting pairs
        per tap event.

    Returns:
        SparkDF: SparkDF with alighting location and time selected based on overall probability.
    """
    unique_row_window = Window.partitionBy(F.col("UNIQUE_ROW_ID")).orderBy(
        F.col("TRAVEL_TIME_SECONDS").asc()
    )
    within_route_and_direction_window = Window.partitionBy(
        F.col("UNIQUE_ROW_ID"), F.col("STOP_LINE_ID_OLD"), F.col("STOP_DIRECTION_ID")
    ).orderBy(F.col("TRANSFER_DISTANCE_METERS").desc())
    ### include route selection
    possible_transfer_events_spark_df = possible_transfer_events_spark_df.withColumn(
        "MIN_ROUTE_AND_DIRECTION_TRAVEL_TIME",
        F.min(F.col("MIN_TRAVEL_TIME_SECONDS")).over(
            within_route_and_direction_window.rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )
        ),
    )
    possible_transfer_events_spark_df = possible_transfer_events_spark_df.withColumn(
        "MIN_TRAVEL_TIME_ALL_TRIPS",
        F.min(F.col("MIN_TRAVEL_TIME_SECONDS")).over(
            unique_row_window.rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )
        ),
    )
    possible_transfer_events_spark_df = possible_transfer_events_spark_df.withColumn(
        "ROUTE_AND_DIRECTION_UTILITY",
        F.exp(
            F.col("MIN_TRAVEL_TIME_ALL_TRIPS")
            / F.col("MIN_ROUTE_AND_DIRECTION_TRAVEL_TIME")
        ),
    )
    sum_trip_utilities = (
        possible_transfer_events_spark_df.select(
            "UNIQUE_ROW_ID",
            "STOP_DIRECTION_ID",
            "STOP_LINE_ID_OLD",
            "ROUTE_AND_DIRECTION_UTILITY",
        )
        .dropDuplicates(["UNIQUE_ROW_ID", "STOP_LINE_ID_OLD", "STOP_DIRECTION_ID"])
        .groupBy("UNIQUE_ROW_ID")
        .agg(
            F.sum(F.col("ROUTE_AND_DIRECTION_UTILITY")).alias(
                "SUM_ROUTE_AND_DIRECTION_UTILITIES"
            )
        )
    )
    possible_transfer_events_spark_df = possible_transfer_events_spark_df.join(
        sum_trip_utilities, on=["UNIQUE_ROW_ID"], how="left"
    )
    possible_transfer_events_spark_df = possible_transfer_events_spark_df.withColumn(
        "ROUTE_AND_DIRECTION_SELECTION_PROBABILITY",
        F.col("ROUTE_AND_DIRECTION_UTILITY")
        / F.col("SUM_ROUTE_AND_DIRECTION_UTILITIES"),
    )
    ####
    # write out debugging###
    import pdb

    pdb.set_trace()
    debugging_ids = [
        "5ef75b4e-b574-338a-3a4b-66b9bb31fa3a",
        "2aab14db-46fe-797a-fdb3-61ec74610c27",
        "4ff7679a-34c5-b9a3-8266-7ebd8db26b59",
        "9cfcf7ec-1589-d4d8-e179-b8ccb34e3cc2",
        "5ef75b4e-b574-338a-3a4b-66b9bb31fa3a",
        "c95e35d1-1662-0239-7c16-0d44e68dd7e5",
        "43ec4aab-6737-1096-f808-4b466050ab4d",
        "e7854e5b-c2cc-c5c8-96c6-5daed19620bb",
    ]
    debugging_dates = [
        "2024-04-03",
        "2024-04-04",
        "2024-04-05",
        "2024-04-06",
        "2024-04-07",
    ]
    import pdb

    pdb.set_trace()
    possible_transfer_events_spark_df.filter(
        F.col("SERVICE_DATE").isin(debugging_dates)
        & F.col("CARD_ID").isin(debugging_ids)
    ).select(
        "CARD_ID",
        "DATETIME",
        "LINE_ID",
        "STOP_ID",
        "STOP_ID_NEXT",
        "ROUTE_AND_DIRECTION_UTILITY",
        "ROUTE_AND_DIRECTION_SELECTION_PROBABILITY",
        "ALIGHTING_PROBABILITY",
        "BOARDING_PROBABILITY",
        "ALIGHTING_STOP_ID",
        "ALIGHTING_LINE_ID",
        "STOP_ID_OLD",
        "STOP_LINE_ID_OLD",
        "DIRECTION_ID",
    ).coalesce(
        1
    ).write.mode(
        "overwrite"
    ).option(
        "header", True
    ).csv(
        "debugging_data/amended_probabilities"
    )
    import pdb

    pdb.set_trace()
    ####
    confidence_selection_window = Window.partitionBy("UNIQUE_ROW_ID").orderBy(
        F.col("CONFIDENCE").desc()
    )
    possible_transfer_events_spark_df = (
        possible_transfer_events_spark_df.withColumn(
            "CONFIDENCE",
            F.col("ALIGHTING_PROBABILITY")
            * F.col("BOARDING_PROBABILITY")
            * F.col("ROUTE_AND_DIRECTION_SELECTION_PROBABILITY"),
        )
        .withColumn("ROW_ID", F.row_number().over(confidence_selection_window))
        .filter(F.col("ROW_ID") == 1)
        .drop("ROW_ID")
    )
    return possible_transfer_events_spark_df.cache()


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
                - F.col("ALIGHTING_ARRIVE_DATETIME").cast("long")
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


def remove_impossible_journeys(
    hop_events_spark_df: SparkDF,
    max_time_to_destination_days: int,
) -> Tuple[SparkDF, SparkDF]:
    """Some journeys may some journeys that include impossible events given the available time.
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


def insert_interlining_events(
    hop_events_spark_df: SparkDF, stop_times_spark_df: SparkDF
) -> SparkDF:
    """Insert interlining events into the hop events. Interlining trips are then used
        to replace original trips.

    Args:
        hop_events_spark_df (SparkDF): Hop events representing inferred journeys.
        stop_times_spark_df (SparkDF): Stop times trip ids that include interlining events.

    Returns:
        SparkDF: Returns a SparkDF with interlining events inserted into the hop events.
    """
    interlining_locations_window = Window.partitionBy(
        F.col("TRIP_ID"), F.col("SERVICE_DATE")
    ).orderBy(F.col("ARRIVE_DATETIME"))
    interlining_locations_spark_df = (
        stop_times_spark_df.withColumn(
            "ROUTE_ID_OLD_NEXT",
            F.lead(F.col("ROUTE_ID_OLD"), 1).over(interlining_locations_window),
        )
        .withColumn(
            "STOP_ID_NEXT",
            F.lead(F.col("STOP_ID"), 1).over(interlining_locations_window),
        )
        .withColumn(
            "STOP_LAT_NEXT",
            F.lead(F.col("STOP_LAT"), 1).over(interlining_locations_window),
        )
        .withColumn(
            "STOP_LON_NEXT",
            F.lead(F.col("STOP_LON"), 1).over(interlining_locations_window),
        )
        .filter(F.col("ROUTE_ID_OLD_NEXT") != F.col("ROUTE_ID_OLD"))
        .select(
            F.col("TRIP_ID").alias("INTERLINING_TRIP_ID"),
            F.col("ARRIVE_DATETIME").alias("INTERLINING_ARRIVE_DATETIME"),
            F.col("DEPARTURE_DATETIME").alias("INTERLINING_DEPARTURE_DATETIME"),
            F.col("SERVICE_DATE").alias("INTERLINING_SERVICE_DATE"),
            F.col("ROUTE_ID_OLD").alias("INTERLINING_ROUTE_ID"),
            F.col("ROUTE_ID_OLD_NEXT").alias("INTERLINING_ROUTE_ID_NEXT"),
            F.col("STOP_ID").alias("INTERLINING_STOP_ID"),
            F.col("STOP_ID_NEXT").alias("INTERLINING_STOP_ID_NEXT"),
            F.col("STOP_LAT").alias("INTERLINING_STOP_LAT"),
            F.col("STOP_LON").alias("INTERLINING_STOP_LON"),
            F.col("STOP_LAT_NEXT").alias("INTERLINING_STOP_LAT_NEXT"),
            F.col("STOP_LON_NEXT").alias("INTERLINING_STOP_LON_NEXT"),
        )
        .distinct()
    )
    hop_events_spark_df = hop_events_spark_df.join(
        interlining_locations_spark_df,
        (
            hop_events_spark_df["TRIP_ID"]
            == interlining_locations_spark_df["INTERLINING_TRIP_ID"]
        )
        & (
            hop_events_spark_df["SERVICE_DATE"]
            == interlining_locations_spark_df["INTERLINING_SERVICE_DATE"]
        )
        & (
            hop_events_spark_df["DATETIME"]
            <= interlining_locations_spark_df["INTERLINING_ARRIVE_DATETIME"]
        )
        & (
            hop_events_spark_df["ALIGHTING_DEPARTURE_DATETIME"]
            >= interlining_locations_spark_df["INTERLINING_DEPARTURE_DATETIME"]
        )
        & (
            hop_events_spark_df["STOP_LINE_ID_OLD"]
            == interlining_locations_spark_df["INTERLINING_ROUTE_ID"]
        )
        & (
            hop_events_spark_df["ALIGHTING_LINE_ID_OLD"]
            == interlining_locations_spark_df["INTERLINING_ROUTE_ID_NEXT"]
        )
        & (
            hop_events_spark_df["ALIGHTING_STOP_ID"]
            != interlining_locations_spark_df["INTERLINING_STOP_ID_NEXT"]
        )
        & (
            hop_events_spark_df["ALIGHTING_STOP_ID"]
            != interlining_locations_spark_df["INTERLINING_STOP_ID"]
        )
        & (
            hop_events_spark_df["STOP_ID"]
            != interlining_locations_spark_df["INTERLINING_STOP_ID"]
        )
        & (
            hop_events_spark_df["STOP_ID"]
            != interlining_locations_spark_df["INTERLINING_STOP_ID_NEXT"]
        ),
        how="left",
    )
    # ensure that journeys where the rider disembarked at an interlining point retain the original line id
    hop_events_spark_df = hop_events_spark_df.withColumn(
        "ALIGHTING_LINE_ID_OLD",
        F.when(
            (F.col("STOP_LINE_ID_OLD") != F.col("ALIGHTING_LINE_ID_OLD"))
            & F.col("INTERLINING_ROUTE_ID").isNull(),
            F.col("STOP_LINE_ID_OLD"),
        ).otherwise(F.col("ALIGHTING_LINE_ID_OLD")),
    )
    return hop_events_spark_df.dropDuplicates(["DATETIME", "JOURNEY_ID"])


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
            "LINE_ID",
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
            F.col("ALIGHTING_ARRIVE_DATETIME").alias("DATETIME"),
            F.col("ALIGHTING_STOP_ID").alias("STOP_ID"),
            "CARD_ID",
            "JOURNEY_ID",
            F.col("ALIGHTING_LINE_ID").alias("LINE_ID"),
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


def reshape_journeys(inferred_transfers_spark_df: SparkDF) -> SparkDF:
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
            "DATETIME",
            "STOP_ID",
            "CARD_ID",
            "JOURNEY_ID",
            F.col("STOP_LINE_ID_OLD").alias("LINE_ID"),  # use GTFS line ids not HOP
            "STOP_LAT",
            "STOP_LON",
            "DIRECTION_ID",
        )
        .withColumn("EVENT", F.lit("BOARDED"))
        .withColumn("CONFIDENCE", F.lit(1))
    )
    # get alightings
    alightings = (
        inferred_transfers_spark_df.filter(~F.col("DATETIME_NEXT").isNull())
        .select(
            "FARE_CATEGORY_DESCRIPTION",
            F.col("ALIGHTING_ARRIVE_DATETIME").alias("DATETIME"),
            F.col("ALIGHTING_STOP_ID").alias("STOP_ID"),
            "CARD_ID",
            "JOURNEY_ID",
            F.col("ALIGHTING_LINE_ID_OLD").alias(
                "LINE_ID"
            ),  # USE GTFS LINE IDS NOT HOP
            F.col("ALIGHTING_STOP_LAT").alias("STOP_LAT"),
            F.col("ALIGHTING_STOP_LON").alias("STOP_LON"),
            "DIRECTION_ID",
            "CONFIDENCE",
        )
        .withColumn("EVENT", F.lit("ALIGHTED"))
    ).filter(
        ~F.col("STOP_ID").isNull()
    )  # remove last unknown alighting
    # get interlining events
    interlining_events = (
        inferred_transfers_spark_df.filter(~F.col("INTERLINING_ROUTE_ID").isNull())
        .filter(~F.col("INTERLINING_STOP_LAT").isNull())
        .filter(~F.col("INTERLINING_STOP_LAT_NEXT").isNull())
    )
    interlinings_starts = (
        interlining_events.select(
            "FARE_CATEGORY_DESCRIPTION",
            F.col("INTERLINING_ARRIVE_DATETIME").alias("DATETIME"),
            F.col("INTERLINING_STOP_ID").alias("STOP_ID"),
            "CARD_ID",
            "JOURNEY_ID",
            F.col("INTERLINING_ROUTE_ID").alias("LINE_ID"),
            F.col("INTERLINING_STOP_LAT").alias("STOP_LAT"),
            F.col("INTERLINING_STOP_LON").alias("STOP_LON"),
            "DIRECTION_ID",
            "CONFIDENCE",
        )
        .withColumn("EVENT", F.lit("INTERLINE_STARTED"))
        .withColumn("EVENT_TYPE", F.lit("MID_JOURNEY"))
    )
    interlinings_ends = (
        interlining_events.select(
            "FARE_CATEGORY_DESCRIPTION",
            F.col("INTERLINING_DEPARTURE_DATETIME").alias("DATETIME"),
            F.col("INTERLINING_STOP_ID_NEXT").alias("STOP_ID"),
            "CARD_ID",
            "JOURNEY_ID",
            F.col("INTERLINING_ROUTE_ID_NEXT").alias("LINE_ID"),
            F.col("INTERLINING_STOP_LAT_NEXT").alias("STOP_LAT"),
            F.col("INTERLINING_STOP_LON_NEXT").alias("STOP_LON"),
            "DIRECTION_ID",
            "CONFIDENCE",
        )
        .withColumn("EVENT", F.lit("INTERLINE_ENDED"))
        .withColumn("EVENT_TYPE", F.lit("MID_JOURNEY"))
    )
    interlinings = interlinings_starts.unionByName(
        interlinings_ends, allowMissingColumns=True
    )
    return boardings.unionByName(alightings, allowMissingColumns=True).unionByName(
        interlinings, allowMissingColumns=True
    )


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
    action_history_spark_df = (
        action_history_spark_df.withColumn(
            "ROW_ID", F.row_number().over(history_window)
        )
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
    action_history_spark_df = (
        action_history_spark_df.withColumn("ROW_ID", F.row_number().over(action_window))
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
    origins = action_history_spark_df.filter(F.col("EVENT_TYPE") == "ORIGIN")
    destinations = action_history_spark_df.filter(F.col("EVENT_TYPE") == "DESTINATION")
    distances = (
        origins.select(
            "JOURNEY_ID",
            F.col("STOP_LAT").alias("STOP_LAT_ORIGIN"),
            F.col("STOP_LON").alias("STOP_LON_ORIGIN"),
        )
        .join(
            destinations.select(
                "JOURNEY_ID",
                F.col("STOP_LAT").alias("STOP_LAT_DESTINATION"),
                F.col("STOP_LON").alias("STOP_LON_DESTINATION"),
            ),
            on="JOURNEY_ID",
            how="left",
        )
        .withColumn(
            "TRIP_DISTANCE_METERS",
            2
            * 6371000
            * F.asin(  # haversine formula
                F.sqrt(
                    F.sin(
                        (
                            F.radians(F.col("STOP_LAT_ORIGIN"))
                            - F.radians(F.col("STOP_LAT_DESTINATION"))
                        )
                        / 2
                    )
                    ** 2
                    + (
                        F.cos(F.radians(F.col("STOP_LAT_ORIGIN")))
                        * F.cos(F.radians(F.col("STOP_LAT_DESTINATION")))
                        * F.sin(
                            (
                                F.radians(F.col("STOP_LON_DESTINATION"))
                                - F.radians("STOP_LON_ORIGIN")
                            )
                            / 2
                        )
                        ** 2
                    )
                )
            ),
        )
    )
    distances = distances.withColumn(
        "IS_LOOP",
        F.when(F.col("TRIP_DISTANCE_METERS") < loop_threshold, True).otherwise(False),
    )
    action_history_spark_df = action_history_spark_df.join(
        distances.select("JOURNEY_ID", "IS_LOOP"), on="JOURNEY_ID", how="left"
    ).withColumn(
        "IS_LOOP", F.when(F.col("IS_LOOP").isNull(), False).otherwise(F.col("IS_LOOP"))
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


def remove_implicit_interlining_events(
    successful_journeys_spark_df: SparkDF,
) -> SparkDF:
    """Validate that only journeys including interlining events have
    changes in line id between a boarding and alighting.

    Args:
        successful_journeys_spark_df (SparkDF): Description

    Returns:
        SparkDF: Original SparkDF if interlining validated, error otherwise.
    """
    non_interlining_journeys = (
        successful_journeys_spark_df.filter(~F.col("EVENT").contains("INTER"))
        .select("JOURNEY_ID")
        .distinct()
    )
    window = Window.partitionBy("JOURNEY_ID").orderBy(F.col("DATETIME").asc())
    journeys_with_mismatching_lines = (
        successful_journeys_spark_df.join(
            non_interlining_journeys, on=["JOURNEY_ID"], how="right"
        )
        .withColumn("LINE_ID_NEXT", F.lead(F.col("LINE_ID"), 1).over(window))
        .filter(F.col("EVENT") == "BOARDED")
        .filter(F.col("LINE_ID") != F.col("LINE_ID_NEXT"))
        .select("JOURNEY_ID")
        .distinct()
    )
    return successful_journeys_spark_df.join(
        journeys_with_mismatching_lines, on="JOURNEY_ID", how="leftanti"
    )


def validate_successful_journeys(successful_journeys_spark_df: SparkDF) -> SparkDF:
    """Validate that the successful journeys conform to expectations. Assert that no nulls
       are in any of the columns.

    Args:
        successful_journeys_spark_df (SparkDF): SparkDF of successful journeys to validate.

    Returns:
        SparkDF: If passes, return the original SparkDF.
    """
    # ensure that columns aren't null
    for column in successful_journeys_spark_df.columns:
        assert (
            successful_journeys_spark_df.filter(F.col(column).isNull()).count() == 0
        ), f"Error: Nulls detected in {column} for successful journeys."
    return successful_journeys_spark_df


def remove_journeys_with_no_destination(action_history_spark_df: SparkDF) -> SparkDF:
    """Remove journeys that have no destination. Unclear why this currently happens.

    Args:
        action_history_spark_df (SparkDF): Description

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
        .values[0][0]
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
    metrics = {
        "N_SUCCESSFUL": successful_journeys_count,
        "N_FAILED": failed_journeys_count,
        "PERCENT_FAILED": int(
            100
            * failed_journeys_count
            / (successful_journeys_count + failed_journeys_count)
        ),
        "MEAN_CONFIDENCE": mean_confidence,
        **reasons_for_failure,
    }
    logger.info(metrics)
    return metrics
