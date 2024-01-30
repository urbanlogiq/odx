# Copyright 2021 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Project pipelines."""
from typing import Dict
from kedro.pipeline import Pipeline, pipeline
from .pipelines.odx.prepare import pipeline as prepare_odx
from .pipelines.odx.infer import pipeline as infer_odx


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    test_odx_pipeline_prepare = pipeline(
        prepare_odx.create_pipeline(),
        inputs={
            "hop_raw_spark_df": "test_hop_raw_spark_df",
            "stop_times_avl_raw_spark_df": "test_stop_times_avl_raw_spark_df",
            "trips_gtfs_raw_spark_df": "test_trips_gtfs_raw_spark_df",
            "stops_gtfs_raw_spark_df": "test_stops_gtfs_raw_spark_df",
        },
        outputs={
            "hop_spark_df_prepared_spark_df": "test_hop_spark_df_prepared_spark_df",
            "stop_times_prepared_spark_df": "test_stop_times_prepared_spark_df",
            "interlining_trip_ids_prepared_spark_df": "test_interlining_trip_ids_prepared_spark_df",
        },
        parameters={"hop_prepared_year_months": "test_hop_prepared_year_months"},
    )
    test_odx_pipeline_infer = pipeline(
        infer_odx.create_pipeline(),
        inputs={
            "hop_spark_df_prepared_spark_df": "test_hop_spark_df_prepared_spark_df",
            "stop_times_prepared_spark_df": "test_stop_times_prepared_spark_df",
            "interlining_trip_ids_prepared_spark_df": "test_interlining_trip_ids_prepared_spark_df",
        },
        outputs={
            "inferred_rider_events_spark_df": "test_inferred_rider_events_spark_df",
            "impossible_journeys_spark_df": "test_impossible_journeys_spark_df",
            "metrics": "test_metrics",
        },
        parameters={
            "allow_no_stops": "test_allow_no_stops",
            "allow_interlining": "test_allow_interlining",
            "hop_prepared_year_months": "test_hop_prepared_year_months",
        },
    )
    test_odx_pipeline = test_odx_pipeline_prepare + test_odx_pipeline_infer
    return {
        "odx.prepare": prepare_odx.create_pipeline(),
        "odx.infer": infer_odx.create_pipeline(),
        "odx": prepare_odx.create_pipeline() + infer_odx.create_pipeline(),
        "odx.test": test_odx_pipeline,
        "odx.test.prepare": test_odx_pipeline_prepare,
        "odx.test.infer": test_odx_pipeline_infer,
    }
