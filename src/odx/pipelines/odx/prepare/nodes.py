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

"""Nodes for preparing data for the ODX inference pipeline."""
from typing import Dict, List

from loguru import logger
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from .utils import get_arrival_and_departure_datetimes


def validate_no_nulls_on_columns(
    spark_df: SparkDF, no_nulls_columns: List[str]
) -> SparkDF:
    """Validate that the columns in no nulls columns have no nulls.

    Args:
        spark_df (SparkDF): SparkDF with columns in no nulls columns
        no_nulls_columns (List[str]): List of columns to check for nulls

    Returns:
        SparkDF: If passes, spark_df is returned
    """
    for column in no_nulls_columns:
        assert (
            spark_df.filter(F.col(column).isNull()).count() == 0
        ), f"Error: Nulls detected in {column}."
    return spark_df


def map_column_name_to_upper_case(spark_df: SparkDF) -> SparkDF:
    """Maps all the column names in the spark_df to upper case.

    Args:
        spark_df (SparkDF): A generic SparkDF

    Returns:
        SparkDF: SparkDF with column names mapped to upper case.
    """
    return spark_df.select(
        *[F.col(column).alias(column.upper()) for column in spark_df.columns]
    )


def map_line_ids(
    spark_df: SparkDF,
    line_id_maps: Dict[int, int],
    column_name: str,
    retain_old_values: bool = True,
) -> SparkDF:
    """Map the line ids in the spark_df accordingly. This is done because the HOP
    system doesn't have the exact same line ids as in GTFS. E.G. all the MAX lines
    in HOP are marked as 200, while they have distinct line ids in GTFS.

    Args:
        spark_df (SparkDF): SparkDF with LINE_ID column
        line_id_maps (Dict[int, int]): Mapping of {from:to} for line ids.
        column_name (str): column to map
        retain_old_values (bool, optional): Retain the old values for reference. Default True

    Returns:
        SparkDF: SparkDF with line ids mapped
    """
    if retain_old_values:
        spark_df = spark_df.withColumn(column_name + "_OLD", F.col(column_name))
    for key, value in line_id_maps.items():
        spark_df = spark_df.withColumn(
            column_name,
            F.when(F.col(column_name) == key, value).otherwise(F.col(column_name)),
        )
    return spark_df


def get_arrival_datetime_hour(spark_df: SparkDF) -> SparkDF:
    """Get the hour of the arrival_datetime column.

    Args:
        spark_df (SparkDF): SparkDF with datetime column.

    Returns:
        SparkDF: SparkDF with hour column.
    """
    return spark_df.withColumn("HOUR", F.hour(F.col("ARRIVE_DATETIME")))


def repartition_by(spark_df: SparkDF, columns_to_repartition_by: List[str]) -> SparkDF:
    """Repartition spark_df by columns.

    Args:
        spark_df (SparkDF): SparkDF with columns in columns_to_repartition_by
        columns_to_repartition_by (List[str]): List of columns to repartition by

    Returns:
        SparkDF: SparkDF repartitioned by columns_to_repartition_by
    """
    return spark_df.repartition(*columns_to_repartition_by)


def create_datetime_column(spark_df: SparkDF, column_to_parse: str) -> SparkDF:
    """Parse a column into a datetime and name the column datetime.

    Args:
        spark_df (SparkDF): DataFrame to add datetime to column to.
        column_to_parse (str): Column to parse.

    Returns:
        SparkDF: Dataframe with datetime column added
    """
    return spark_df.withColumn("DATETIME", F.to_timestamp(F.col(column_to_parse)))


def create_date_column(
    spark_df: SparkDF, column_to_parse: str = "DATETIME", raw_date_format: str = ""
) -> SparkDF:
    """Create a date column from the column to parse.

    Args:
        spark_df (SparkDF): SparkDF with "column_to_parse" that can be parsed into a date
        column_to_parse (str, optional): Column who's values can be parsed into a date

    Returns:
        SparkDF: SparkDF with date column
    """
    if raw_date_format:
        return spark_df.withColumn(
            "DATE",
            F.to_date(F.col(column_to_parse).cast(StringType()), raw_date_format),
        )
    else:
        return spark_df.withColumn(
            "DATE", F.to_date(F.col(column_to_parse).cast(StringType()))
        )


def remove_unknown_stops(spark_df: SparkDF) -> SparkDF:
    """Some of the STOP_ID values in the Hop data are
    unfilled. In the correct schema node we mapped these to -1. This node removes them.

    Args:
        spark_df (SparkDF): SparkDF with STOP_ID column.

    Returns:
        SparkDF: SparkDF where the STOP_ID column has been filtered to remove values less than 0.
    """
    return spark_df.filter(F.col("STOP_ID") != -1)


def remove_duplicate_taps(spark_df: SparkDF, threshold_seconds: int) -> SparkDF:
    """Removes duplicate taps according to a threshold in time. Retains the latest tap.

    Args:
        spark_df (SparkDF): SparkDF representing taps.

    Returns:
        SparkDF: SparkDF with duplicate taps removed according to the threshold.
    """
    tap_window = Window.partitionBy("CARD_ID").orderBy(F.col("DATETIME").asc())
    spark_df = spark_df.withColumn(
        "DATETIME_NEXT", F.lead(F.col("DATETIME"), 1).over(tap_window)
    )
    # find duplicate taps
    spark_df = spark_df.withColumn(
        "IS_DUPLICATE_TAP",
        F.when(
            F.col("DATETIME_NEXT").isNotNull()
            & (
                (F.col("DATETIME_NEXT").cast("int") - F.col("DATETIME").cast("int"))
                < threshold_seconds
            ),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    return spark_df.filter(F.col("IS_DUPLICATE_TAP") == 0).drop(
        "IS_DUPLICATE_TAP", "DATETIME_NEXT"
    )


def fill_transaction_ticket_description_nulls_with_unknown(
    spark_df: SparkDF,
) -> SparkDF:
    """TRANSACTION_TICKET_DESCRIPTION has nulls, this fills those with an Unknown string

    Args:
        spark_df (SparkDF): SparkDF with TRANSACTION_TICKET_DESCRIPTION

    Returns:
        SparkDF: SparkDF with TRANSACTION_TICKET_DESCRIPTION nulls filled with Unknown
    """
    return spark_df.withColumn(
        "TRANSACTION_TICKET_DESCRIPTION",
        F.when(F.col("TRANSACTION_TICKET_DESCRIPTION").isNull(), "Unknown").otherwise(
            F.col("TRANSACTION_TICKET_DESCRIPTION")
        ),
    )


def identify_unwanted_operators(
    spark_df: SparkDF, unwanted_operators: List[str]
) -> SparkDF:
    """Tag unwanted operators as such. They are removed at a later date.

    Args:
        spark_df (SparkDF): Spark_DF with "OPERATEDBY_DESCRIPTION"
        unwanted_operators (List[str]): List of unwanted operators

    Returns:
        SparkDF: SparkDF with "OPERATEDBY_DESCRIPTION" filtered to remove unwanted operators. Mainly CTRAN
    """
    return spark_df.withColumn(
        "IS_UNWANTED_OPERATOR",
        F.when(
            F.col("OPERATEDBY_DESCRIPTION").isin(unwanted_operators)
            | F.col("OPERATEDBY_DESCRIPTION").isNull(),
            True,
        ).otherwise(False),
    )


def build_stop_times_from_stops_avl(stops_avl_spark_df: SparkDF) -> SparkDF:
    """Build the stop times from the stops avl data. Handles stop times greater than 23:59:59
    by adding to the date and wrapping around midnight.

    Args:
        stops_avl_spark_df (SparkDF): Stop times sourced from the CAD AVL system.

    Returns:
        SparkDF: SparkDF with important information added
    """
    stops_avl_spark_df = (
        stops_avl_spark_df.withColumn("ARRIVE_TIME", F.col("actual_arrive_time"))
        .withColumn("DEPARTURE_TIME", F.col("actual_departure_time"))
        .drop("actual_arrive_time", "actual_departure_time")
    )
    return get_arrival_and_departure_datetimes(stops_avl_spark_df)


def merge_trip_information_onto_avl_stop_times(
    stop_times_avl_spark_df: SparkDF, trips_spark_df: SparkDF
) -> SparkDF:
    """Merge information such as route, direction, block and service id from gtfs
    onto the avl data.

    Args:
        stop_times_avl_spark_df (SparkDF): Stop times sourced from CAD AVL data.
        trips_spark_df (SparkDF): GTFS trip metadata information

    Returns:
        SparkDF: stop_times_avl_spark_df with trip information merged on.
    """
    trips_spark_df = trips_spark_df.select(
        F.col("ROUTE_ID"),
        F.col("SERVICE_ID"),
        F.col("TRIP_ID"),
        F.col("DIRECTION_ID"),
        F.col("BLOCK_ID"),
    ).dropDuplicates(subset=["TRIP_ID"])
    stop_times_avl_spark_df = stop_times_avl_spark_df.join(
        trips_spark_df,
        on=["TRIP_ID"],
        how="left",
    )
    if stop_times_avl_spark_df.filter(F.col("ROUTE_ID").isNull()).count() > 0:
        missing_trip_ids_and_dates = (
            stop_times_avl_spark_df.filter(F.col("ROUTE_ID").isNull())
            .select("SERVICE_DATE")
            .distinct()
            .toPandas()
            .to_markdown(index=False)
        )
        logger.info(
            f"Unable to find GTFS data for following CAD AVL trips in these service dates:\n {missing_trip_ids_and_dates}. Stopping."
        )
        raise Exception("Missing GTFS Data")
    return stop_times_avl_spark_df


def add_source_column(spark_df: SparkDF, source_value: str) -> SparkDF:
    """Add column named "SOURCE" with value equal to source_value to spark_df.

    Args:
        spark_df (SparkDF): SparkDF to add source column to.
        source_value (str): string value to set source column to.

    Returns:
        SparkDF: SparkDF with source column added and value set to source_value.
    """
    assert "SOURCE" not in [
        x.upper() for x in spark_df.columns
    ], "'SOURCE' column already exists"
    return spark_df.withColumn("SOURCE", F.lit(source_value))


def get_relevant_stop_times(
    hop_events_spark_df: SparkDF,
    stop_times_avl_spark_df: SparkDF,
) -> SparkDF:
    """Only consider service dates that we have hop data for.

    Args:
        hop_events_spark_df (SparkDF): Hop tap events with service date column.
        stop_times_avl_spark_df (SparkDF): Stop times sourced from CAD avl.

    Returns:
        SparkDF: SparkDF where the stop_times* SparkDFs have been filtered by the dates contained in the HOP data.
    """
    hop_dates = (
        hop_events_spark_df.select("SERVICE_DATE")
        .dropDuplicates()
        .toPandas()["SERVICE_DATE"]
        .values.tolist()
    )
    stop_times_avl_spark_df = stop_times_avl_spark_df.filter(
        F.col("SERVICE_DATE").isin(hop_dates)
    )  # only keep the stop times that have hop taps associated with them
    return stop_times_avl_spark_df


def union_spark_dataframes_by_name(*args: SparkDF) -> SparkDF:
    """Takes the union of all the spark dfs in args.

    Args:
        *args (SparkDF): SparkDFs to be unioned

    Returns:
        SparkDF: Union by name of all spark dfs

    Raises:
        Exception: If fewer than two spark dfs are passed.
    """
    if len(args) > 1:
        final_frame: SparkDF = args[0]
        for frame in args[1:]:
            final_frame = final_frame.unionByName(frame, allowMissingColumns=True)
        return final_frame
    else:
        raise Exception("Must have at least two frames to union")


def merge_stop_locations_onto_stop_times(
    stop_times_spark_df: SparkDF, stop_locations_spark_df: SparkDF
) -> SparkDF:
    """Take the stop times and the stop locations data and merge them.

    Args:
        stop_times_spark_df (SparkDF): SparkDF of stop times per trip, route, direction, date
        stop_locations_spark_df (SparkDF): SparkDF of stop locations

    Returns:
        SparkDF: SparkDF where stop lat and lon have been tagged on to the stop times
    """
    stop_times_spark_df = stop_times_spark_df.withColumn(
        "UNIQUE_ROW_ID",
        F.sha2(
            F.concat(
                F.col("SERVICE_DATE").cast(StringType()),
                F.col("TRIP_ID").cast(StringType()),
                F.col("STOP_ID").cast(StringType()),
                F.col("ARRIVE_DATETIME").cast(StringType()),
                F.col("DEPARTURE_DATETIME").cast(StringType()),
                F.rand().cast(StringType()),
            ),
            256,
        ),
    ).join(
        stop_locations_spark_df.select(
            "STOP_ID", "STOP_LAT", "STOP_LON", "RELEASE_DATE"
        ).dropDuplicates(),
        on=["STOP_ID"],
        how="left",
    )
    # ensure that the stop times are unique and are merged to locations closest to them in time
    window = Window.partitionBy("UNIQUE_ROW_ID").orderBy(F.col("DATE_DELTA").asc())
    stop_times_spark_df = (
        stop_times_spark_df.withColumn(
            "DATE_DELTA",
            F.abs(F.datediff(F.col("RELEASE_DATE"), F.col("SERVICE_DATE"))),
        )
        .withColumn("RANK", F.row_number().over(window))
        .filter(F.col("RANK") == 1)
        .drop("DATE_DELTA", "RELEASE_DATE", "RANK")
    )
    return stop_times_spark_df


def coalesce_stop_times(stop_times_spark_df: SparkDF) -> SparkDF:
    """Some stop times look like
    | trip_id|direction_id|route_id|service_id|stop_id|arrival_time|
    |11294566|           1|     200|     A.613|   7606|    05:43:27|
    |11294566|           1|     200|     A.613|   7606|    05:43:29|
    i.e. the same stop shows up twice in a row. This node handles by removing duplicate rows and keeping the last row.

    Args:
        stop_times_spark_df (SparkDF): SparkDF of stop times.

    Returns:
        SparkDF: Spark DF with repeated stop times removed.
    """
    window = Window.partitionBy(
        "SERVICE_DATE", "TRIP_ID", "ROUTE_ID", "SERVICE_ID", "DIRECTION_ID"
    ).orderBy(F.col("ARRIVE_DATETIME").asc())
    return (
        stop_times_spark_df.withColumn(
            "NEXT_STOP_ID_IS_SAME",
            F.when(
                F.lead(F.col("STOP_ID"), 1).over(window) == F.col("STOP_ID"), True
            ).otherwise(False),
        )
        .filter(F.col("NEXT_STOP_ID_IS_SAME") == False)
        .drop("NEXT_STOP_ID_IS_SAME")
    )


def convert_string_route_ids_to_integers(stop_times_spark_df: SparkDF) -> SparkDF:
    """Some of the route ids look like 81a, 2a etc. The "a"s aren't represented in the HOP data but the trips are still valid,
    so we need to remove the alphabetical parts of the route id so that everything is an integer. A quick check shows that
    the two routes don't overlap in time.

    Args:
        stop_times_spark_df (SparkDF): SparkDF of stop times with "ROUTE_ID" column

    Returns:
        SparkDF: SparkDF with stop_time route ids converted to their numeric only values.
    """
    stop_times_spark_df = stop_times_spark_df.withColumn(
        "ROUTE_ID", F.regexp_replace(F.col("ROUTE_ID"), "[a-z]", "").cast("int")
    )  # replace alphabetical characters with nothing then cast to int
    assert (
        stop_times_spark_df.filter(F.col("ROUTE_ID").isNull()).count() == 0
    ), "Error: Non-numeric route ids remain in stop times after regex conversion. See: convert_string_route_ids_to_integers."
    return stop_times_spark_df


def build_stop_headways(stop_times_spark_df: SparkDF) -> SparkDF:
    """For each line stop location and stop time group,determine the headway considering
    the previous/next stop_buffer_size stop times.

    Args:
        stop_times_spark_df (SparkDF): SparkDF with "ROUTE_ID","DATE","ARRIVE_DATETIME", "STOP_ID" columns

    Returns:
        SparkDF: stop_times_spark_df with "MEAN_BOARDING_INTERVAL_SECONDS" in seconds added
    """
    mean_interval_window = Window.partitionBy(
        "ROUTE_ID", "STOP_ID", "SERVICE_DATE", "DIRECTION_ID"
    ).orderBy(F.col("ARRIVE_DATETIME").asc())
    # get headways by evaluating arrival times before and after current stop.
    stop_times_spark_df = (
        stop_times_spark_df.withColumn(
            "ARRIVAL_DELTA",
            F.col("ARRIVE_DATETIME").cast("long")
            - F.lag(F.col("ARRIVE_DATETIME").cast("long"), 1).over(
                mean_interval_window
            ),
        ).withColumn(
            "MEAN_BOARDING_INTERVAL_SECONDS",
            F.mean(F.col("ARRIVAL_DELTA")).over(
                mean_interval_window.rowsBetween(-2, 2)
            ),
        )  # we evaluate the mean over a larger window to reduce the impact of extremely high headway (8 hours) routes.
        # If we didn't then a rider might be tagged as having a journey that covers ~ 8 hours of wait time, when it should clearly be split into two journeys.
        .withColumn(
            "MEAN_BOARDING_INTERVAL_SECONDS",
            F.when(F.col("MEAN_BOARDING_INTERVAL_SECONDS") < 60, F.lit(60))
            .when(
                F.col("MEAN_BOARDING_INTERVAL_SECONDS").isNull(),
                F.first(F.col("MEAN_BOARDING_INTERVAL_SECONDS"), ignorenulls=True).over(
                    mean_interval_window.rowsBetween(0, Window.unboundedFollowing)
                ),
            )
            .otherwise(F.col("MEAN_BOARDING_INTERVAL_SECONDS")),  # floor on the headway
        )
    )
    return stop_times_spark_df


def get_trip_id_start_end_stop_ids_and_times(stop_times_spark_df: SparkDF) -> SparkDF:
    """Get the start and end stops/times for each trip per service date

    Args:
        stop_times_spark_df (SparkDF): SparkDF containing all the stop events for the period of interest.

    Returns:
        SparkDF: SparkDF where each trip_id has its start and end stop ids associated with it.
    """
    window = Window.partitionBy("SERVICE_DATE", "TRIP_ID").orderBy(
        F.col("STOP_SEQUENCE").asc()
    )
    stop_times_spark_df = (
        stop_times_spark_df.withColumn(
            "STOP_IDS",
            F.collect_list(F.col("STOP_ID")).over(
                window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
        )
        .withColumn("START_STOP_ID", F.element_at(F.col("STOP_IDS"), 1))
        .withColumn("END_STOP_ID", F.element_at(F.col("STOP_IDS"), -1))
        .withColumn(
            "ARRIVE_DATETIMES",
            F.collect_list(F.col("ARRIVE_DATETIME")).over(
                window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
        )
        .withColumn("START_DATETIME", F.element_at(F.col("ARRIVE_DATETIMES"), 1))
        .withColumn("END_DATETIME", F.element_at(F.col("ARRIVE_DATETIMES"), -1))
    )
    return stop_times_spark_df.drop("STOP_IDS", "ARRIVE_DATETIMES")


def build_blocks(stop_times_spark_df: SparkDF) -> SparkDF:
    """
    Following the code here:
    https://github.com/OpenTransitTools/gtfsdb/blob/master/gtfsdb/model/block.py,
    create blocks of trips and use them to identify interlining trips
    Args:
        stop_times_spark_df (SparkDF): SparkDF of trips

    Returns:
        SparkDF: blocks of trips with interlining identified through an extra column
    """
    unique_trips = stop_times_spark_df.select(
        "SERVICE_DATE",
        "ROUTE_ID",
        "SERVICE_ID",
        "BLOCK_ID",
        "TRIP_ID",
        "START_STOP_ID",
        "END_STOP_ID",
        "START_DATETIME",
        "END_DATETIME",
    ).dropDuplicates()
    window = Window.partitionBy("SERVICE_DATE", "BLOCK_ID", "SERVICE_ID").orderBy(
        F.col("START_DATETIME").asc()
    )
    lead_columns = [
        "START_STOP_ID",
        "END_STOP_ID",
        "START_DATETIME",
        "END_DATETIME",
        "TRIP_ID",
        "ROUTE_ID",
    ]
    blocks = unique_trips.select(
        *unique_trips.columns,
        *[
            F.lead(F.col(column), 1).over(window).alias(column + "_NEXT")
            for column in unique_trips.columns
            if column in lead_columns
        ],
    )
    blocks = blocks.withColumn(
        "INTERLINES_WITH_TRIP_ID",
        F.when(
            (F.col("END_STOP_ID") == F.col("START_STOP_ID_NEXT"))
            & (F.col("ROUTE_ID") != F.col("ROUTE_ID_NEXT")),
            F.col("TRIP_ID_NEXT"),
        ).otherwise(None),
    )
    return blocks.select(
        "SERVICE_DATE",
        "SERVICE_ID",
        "BLOCK_ID",
        "TRIP_ID",
        "INTERLINES_WITH_TRIP_ID",
        "ROUTE_ID",
        "ROUTE_ID_NEXT",
        "END_STOP_ID",
        "START_STOP_ID_NEXT",
    ).cache()


def create_interlining_trips(block_spark_df: SparkDF) -> SparkDF:
    """Build trips from possibly interlining trips, yielding an "INTERLINING_TRIP_ID" that
    describes the entire trip of a vehicle as it interlines across multiple trip ids.

    Args:
        block_spark_df (SparkDF): SparkDF describing which trip ids interline.

    Returns:
        SparkDF: SparkDF with SERVICE_DATE,INTERLINING_TRIP_ID,TRIP_ID
    """

    def create_nested_trips(spark_df: SparkDF) -> SparkDF:
        """Build comprehensive trips with interlining possiblity.

        Args:
            spark_df (SparkDF): SparkDF describing which trips interline

        Returns:
            SparkDF: SparkDF containing the chained interlining
        """
        if "TRIP_ID_LIST" not in spark_df.columns:
            spark_df = spark_df.withColumn("TRIP_ID_LIST", F.array(F.col("TRIP_ID")))
        spark_df = spark_df.select(
            "SERVICE_DATE", "TRIP_ID_LIST", "INTERLINES_WITH_TRIP_ID"
        ).join(
            spark_df.select(
                "SERVICE_DATE",
                F.col("TRIP_ID").alias("INTERLINES_WITH_TRIP_ID"),
                F.col("INTERLINES_WITH_TRIP_ID").alias("NEXT"),
            ),
            on=["SERVICE_DATE", "INTERLINES_WITH_TRIP_ID"],
            how="left",
        )
        spark_df = spark_df.withColumn(
            "TRIP_ID_LIST",
            F.when(
                ~F.col("INTERLINES_WITH_TRIP_ID").isNull(),
                F.concat(
                    F.col("TRIP_ID_LIST"), F.array(F.col("INTERLINES_WITH_TRIP_ID"))
                ),
            ).otherwise(F.col("TRIP_ID_LIST")),
        )
        spark_df = spark_df.select(
            "SERVICE_DATE",
            "TRIP_ID_LIST",
            F.col("INTERLINES_WITH_TRIP_ID").alias("TRIP_ID"),
            F.col("NEXT").alias("INTERLINES_WITH_TRIP_ID"),
        ).cache()
        if spark_df.filter(~F.col("INTERLINES_WITH_TRIP_ID").isNull()).count() > 0:
            spark_df = create_nested_trips(spark_df)
        return spark_df

    trip_lists = create_nested_trips(block_spark_df)
    trip_lists = trip_lists.withColumn("CHAIN_LENGTH", F.size(F.col("TRIP_ID_LIST")))
    window = Window.partitionBy("SERVICE_DATE", "TRIP_ID").orderBy(
        F.col("CHAIN_LENGTH").desc()
    )
    trip_lists = trip_lists.withColumn(
        "INTERLINING_TRIP_ID",
        F.when(
            F.col("CHAIN_LENGTH") > 1,
            F.conv(
                F.substring(F.sha2(F.to_json(F.col("TRIP_ID_LIST")), 256), 1, 16),
                16,
                10,
            ).cast("decimal(38,0)")  # decimal(38,0) allows for very large integers
        ).otherwise(F.element_at(F.col("TRIP_ID_LIST"), 1))
    )
    trip_lists = (
        trip_lists.select(
            "SERVICE_DATE",
            "CHAIN_LENGTH",
            "INTERLINING_TRIP_ID",
            F.explode(F.col("TRIP_ID_LIST")).alias("TRIP_ID"),
        )
        .withColumn("ROW_NUMBER", F.row_number().over(window))
        .filter(F.col("ROW_NUMBER") == 1)
    ).drop("ROW_NUMBER", "CHAIN_LENGTH")
    assert (
        trip_lists.filter(F.col("INTERLINING_TRIP_ID").isNull()).count() == 0
    ), "error: some trips don't have an interlining trip id"
    return trip_lists
