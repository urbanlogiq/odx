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

from pyspark.sql import functions as F
from typing import List
from .utils import haversine_meters


def get_impossible_conditions_and_descriptions(
    max_time_to_destination_days: int,
) -> List:
    """Get the impossible conditions and descriptions

    Args:
        max_time_to_destination_days (int): max time to destination cutoff

    Returns:
        List: List of tuples of conditions and descriptions
    """
    return [
        (
            (F.col("ALIGHTING_ARRIVE_DATETIME") == F.col("DATETIME_NEXT")),
            "ALIGHTING DATETIME EQUAL TO NEXT BOARDING DATETIME",
        ),
        (
            (
                (
                    F.col("INTERLINING_STOP_LAT").isNull()
                    & (~F.col("INTERLINING_STOP_ID").isNull())
                )
                | (
                    F.col("INTERLINING_STOP_LAT_NEXT").isNull()
                    & (~F.col("INTERLINING_STOP_ID_NEXT").isNull())
                )
            ),
            "INTERLINING STOP DOESN'T HAVE A LOCATION",
        ),
        (
            (F.col("STOP_ID") == F.col("STOP_ID_NEXT")),
            "NEXT STOP ID IS SAME AS CURRENT",
        ),
        (
            (
                (
                    F.col("ALIGHTING_ARRIVE_DATETIME").cast("long")
                    + F.col("MINIMUM_REQUIRED_TRANSFER_TIME_SECONDS")
                )
                > (F.col("DATETIME_NEXT").cast("long") + 300)
            ),
            "ALIGHTING TIME PLUS TRANSFER TIME IS AFTER NEXT BOARDING TIME",
        ),
        (
            (
                (
                    F.col("ALIGHTING_ARRIVE_DATETIME").cast("long")
                    <= F.col("DATETIME").cast("long")
                )
            ),
            "ALIGHTING TIME IS BEFORE BOARDING TIME",
        ),
        (
            (
                (
                    F.col("ALIGHTING_ARRIVE_DATETIME").cast("long")
                    > F.col("DATETIME_NEXT").cast("long")
                )
            ),
            "ALIGHTING TIME IS BEFORE BOARDING TIME",
        ),
        (
            (
                (((F.col("IS_UNWANTED_OPERATOR") == True)) & F.col("TRIP_ID").isNull())
                | F.col("STOP_ID").isNull()
            ),
            "FAILED TO FIND BOARDING TRIP ID",
        ),
        (
            (F.col("STOP_LAT").isNull() | F.col("STOP_LON").isNull()),
            "STOP_LAT OR STOP_LON ARE NULL",
        ),
        ((F.col("LINE_ID").isNull()), "LINE ID IS NULL"),
        (
            (~F.col("TRIP_ID").isNull())
            & (~F.col("DATETIME_NEXT").isNull())
            & F.col("INFERRED_WAIT_TIME_SECONDS").isNull(),
            "COULD NOT CALCULATE INFERRED WAIT TIME",
        ),
        (
            (~F.col("TRIP_ID").isNull()) & F.col("TRANSFER_DISTANCE_METERS").isNull(),
            "COULD NOT CALCULATE TRANSFER DISTANCE",
        ),
        (
            (~F.col("TRIP_ID").isNull())
            & (F.col("CONFIDENCE").isNull() | F.isnan(F.col("CONFIDENCE"))),
            "COULD NOT CALCULATE CONFIDENCE SCORE",
        ),
        (
            (~F.col("TRIP_ID").isNull() & F.col("BOARDING_PROBABILITY").isNull()),
            "COULD NOT CALCULATE BOARDING PROBABILITY",
        ),
        (
            (
                ~F.col("ALIGHTING_STOP_ID").isNull()
                & (
                    F.col("ALIGHTING_PROBABILITY").isNull()
                    | F.isnan(F.col("ALIGHTING_PROBABILITY"))
                )
            ),
            "COULD NOT CALCULATE ALIGHTING PROBABILITY",
        ),
        ((F.col("IS_UNWANTED_OPERATOR") == True), "OPERATOR IS UNWANTED"),
        (
            (~F.col("TRIP_ID").isNull()) & F.col("STOP_LAT").isNull(),
            "BOARDING STOP HAS NO LOCATION",
        ),
        (F.col("DATETIME_NEXT").isNull(), "NO DESTINATION BOARDING TAP"),
        (
            ~(F.col("DATETIME").isNull() | F.col("DATETIME_NEXT").isNull())
            & (
                (F.col("DATETIME_NEXT").cast("long") - F.col("DATETIME").cast("long"))
                > F.lit(max_time_to_destination_days * 24 * 3600)
            ),
            str(
                f"DESTINATION BOARDING TAP IS MORE THAN {max_time_to_destination_days} DAYS IN THE FUTURE"
            ),
        ),
        (
            (
                (~F.col("DATETIME_NEXT").isNull())
                & F.col("MEAN_BOARDING_INTERVAL_SECONDS_NEXT").isNull()
            ),
            "NO BOARDING INTERVAL AT DESTINATION BOARDING TAP",
        ),
        (
            (~F.col("TRIP_ID").isNull())
            & F.col("MEAN_BOARDING_INTERVAL_SECONDS").isNull(),
            "NO BOARDING INTERVAL BOARDING TAP",
        ),
        (F.col("STOP_LINE_ID_OLD").isNull(), "OLD LINE ID IS NULL"),
        (
            (
                haversine_meters(
                    F.col("STOP_LAT"),
                    F.col("STOP_LON"),
                    F.col("ALIGHTING_STOP_LAT"),
                    F.col("ALIGHTING_STOP_LON"),
                )
                / (
                    F.col("ALIGHTING_ARRIVE_DATETIME").cast("long")
                    - F.col("ARRIVE_DATETIME").cast("long")
                )
            )
            > F.lit(30),
            "VEHICLE ACCOMPLISHED RIDE FASTER THAN 75 MPH",
        ),
    ]
