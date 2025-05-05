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

"""Utility functions for the prepare nodes.
"""
import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql.types import IntegerType, StringType


def get_arrival_and_departure_datetimes(stops_times_spark_df: SparkDF) -> SparkDF:
    """Create the arrival and departure datetime columns for stop times spark dataframe. Main
    issue here is that stop times in GTFS can go beyond 23 hours i.e. you could have a stop time that
    is tagged as occuring at 26:15:03, indicating that the stop occured the day after the service date
    but should still be considered part of that service date. This function basically handles that by creating
    stop datetimes rather than stop times and service dates.

    Args:
        stops_avl_spark_df (SparkDF): SparkDF with service_date, arrive/departure time columns.

    Returns:
        SparkDF: SparkDF with arrival and departure datetimes.
    """
    stops_times_spark_df = stops_times_spark_df.withColumn(
        "ARRIVE_TIME_HOUR_GREATER_THAN_23",
        F.when(
            F.split(F.col("ARRIVE_TIME"), ":").getItem(0).cast(IntegerType()) > 23,
            True,
        ).otherwise(False),
    ).withColumn(
        "DEPARTURE_TIME_HOUR_GREATER_THAN_23",
        F.when(
            F.split(F.col("DEPARTURE_TIME"), ":").getItem(0).cast(IntegerType()) > 23,
            True,
        ).otherwise(False),
    )
    ###create actual arrive datetime
    stops_times_spark_df = (
        stops_times_spark_df.withColumn(
            "ARRIVE_HOUR", F.split(F.col("ARRIVE_TIME"), ":").getItem(0)
        )
        .withColumn(
            "ARRIVE_HOUR",
            F.when(
                F.col("ARRIVE_TIME_HOUR_GREATER_THAN_23") == True,
                (F.col("ARRIVE_HOUR").cast(IntegerType()) - 24).cast(StringType()),
            ).otherwise(F.col("ARRIVE_HOUR")),
        )
        .withColumn("ARRIVE_MINUTE", F.split(F.col("ARRIVE_TIME"), ":").getItem(1))
        .withColumn("ARRIVE_SECOND", F.split(F.col("ARRIVE_TIME"), ":").getItem(2))
        .withColumn(
            "ARRIVE_DATE",
            F.when(
                F.col("ARRIVE_TIME_HOUR_GREATER_THAN_23") == True,
                F.date_add(F.col("SERVICE_DATE"), 1),
            ).otherwise(F.col("SERVICE_DATE")),
        )
        .withColumn(
            "ARRIVE_DATETIME",
            F.to_timestamp(
                F.concat(
                    F.col("ARRIVE_DATE").cast(StringType()),
                    F.lit(" "),
                    F.concat_ws(
                        ":",
                        F.col("ARRIVE_HOUR"),
                        F.col("ARRIVE_MINUTE"),
                        F.col("ARRIVE_SECOND"),
                    ),
                )
            ),
        )
    ).drop(
        "ARRIVE_DATE",
        "ARRIVE_HOUR",
        "ARRIVE_MINUTE",
        "ARRIVE_SECOND",
        "ARRIVE_TIME_HOUR_GREATER_THAN_23",
    )
    ###create actual departure datetime
    stops_times_spark_df = (
        stops_times_spark_df.withColumn(
            "DEPARTURE_HOUR",
            F.split(F.col("DEPARTURE_TIME"), ":").getItem(0),
        )
        .withColumn(
            "DEPARTURE_HOUR",
            F.when(
                F.col("DEPARTURE_TIME_HOUR_GREATER_THAN_23") == True,
                (F.col("DEPARTURE_HOUR").cast(IntegerType()) - 24).cast(StringType()),
            ).otherwise(F.col("DEPARTURE_HOUR")),
        )
        .withColumn(
            "DEPARTURE_MINUTE",
            F.split(F.col("DEPARTURE_TIME"), ":").getItem(1),
        )
        .withColumn(
            "DEPARTURE_SECOND",
            F.split(F.col("DEPARTURE_TIME"), ":").getItem(2),
        )
        .withColumn(
            "DEPARTURE_DATE",
            F.when(
                F.col("DEPARTURE_TIME_HOUR_GREATER_THAN_23") == True,
                F.date_add(F.col("SERVICE_DATE"), 1),
            ).otherwise(F.col("SERVICE_DATE")),
        )
        .withColumn(
            "DEPARTURE_DATETIME",
            F.to_timestamp(
                F.concat(
                    F.col("DEPARTURE_DATE").cast(StringType()),
                    F.lit(" "),
                    F.concat_ws(
                        ":",
                        F.col("DEPARTURE_HOUR"),
                        F.col("DEPARTURE_MINUTE"),
                        F.col("DEPARTURE_SECOND"),
                    ),
                )
            ),
        )
    ).drop(
        "DEPARTURE_DATE",
        "DEPARTURE_HOUR",
        "DEPARTURE_MINUTE",
        "DEPARTURE_SECOND",
        "DEPARTURE_TIME_HOUR_GREATER_THAN_23",
    )
    return stops_times_spark_df


def get_valid_dates_from_calendar(calendar_spark_df: SparkDF) -> SparkDF:
    """Using the calendar entries for GTFS data, get the valid dates for each service.

    Args:
        calendar_spark_df (SparkDF): Description
    """
    calendar_spark_df = (
        calendar_spark_df.withColumn(
            "START_DATE", F.to_date(F.col("START_DATE").cast(StringType()), "yyyyMMdd")
        )
        .withColumn(
            "END_DATE", F.to_date(F.col("END_DATE").cast(StringType()), "yyyyMMdd")
        )
        .withColumn("N_DAYS_VALID", F.datediff(F.col("END_DATE"), F.col("START_DATE")))
    )
    calendar_spark_df = calendar_spark_df.withColumn(
        "VALID_DATES",
        F.filter(
            F.array(
                *[
                    F.date_add(F.col("START_DATE"), x)
                    for x in range(
                        0,
                        calendar_spark_df.select(
                            F.max(F.col("N_DAYS_VALID")).alias("MAX")
                        ).collect()[0]["MAX"]
                        + 1,
                    )
                ]
            ),
            lambda x: x <= F.col("END_DATE"),
        ),
    )
    return calendar_spark_df.drop("N_DAYS_VALID")
