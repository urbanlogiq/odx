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

"""Pipeline for preparing data for HOP ODX inference
"""
from kedro.pipeline import Pipeline, node
from ..infer.nodes import get_year_months
from .nodes import (
    validate_no_nulls_on_columns,
    fill_transaction_ticket_description_nulls_with_unknown,
    map_line_ids,
    create_datetime_column,
    create_date_column,
    identify_unwanted_operators,
    remove_unknown_stops,
    add_source_column,
    merge_stop_locations_onto_stop_times,
    get_relevant_stop_times,
    merge_trip_information_onto_avl_stop_times,
    convert_string_route_ids_to_integers,
    build_stop_times_from_stops_avl,
    get_trip_id_start_end_stop_ids_and_times,
    build_blocks,
    create_interlining_trips,
    get_arrival_datetime_hour,
    repartition_by,
    coalesce_stop_times,
    build_stop_headways,
    map_column_name_to_upper_case,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Create the pipeline.

    Args:
        **kwargs

    Returns:
        Pipeline: Description
    """
    if "tags" in kwargs.keys():
        tags = kwargs["tags"]
    else:
        tags = ["prepare", "odx"]
    return Pipeline(
        [
            node(
                func=lambda *frames: [map_column_name_to_upper_case(y) for y in frames],
                inputs=[
                    "hop_raw_spark_df",
                    "stop_times_avl_raw_spark_df",
                    "trips_gtfs_raw_spark_df",
                    "stops_gtfs_raw_spark_df",
                ],
                outputs=[
                    "hop_raw_w_upper_case_column_names_spark_df",
                    "stop_times_avl_raw_w_upper_case_column_names_spark_df",
                    "trips_gtfs_raw_w_upper_case_column_names_spark_df",
                    "stops_gtfs_raw_w_upper_case_column_names_spark_df",
                ],
                tags=tags,
                name="map_all_column_names_to_upper_case",
            ),
            node(
                func=validate_no_nulls_on_columns,
                inputs=[
                    "hop_raw_w_upper_case_column_names_spark_df",
                    "params:hop_no_nulls_columns",
                ],
                outputs="hop_raw_w_no_nulls_validated_spark_df",
                tags=tags,
                name="validate_columns_have_no_nulls",
            ),
            node(
                func=fill_transaction_ticket_description_nulls_with_unknown,
                inputs=["hop_raw_w_no_nulls_validated_spark_df"],
                outputs="hop_raw_w_transaction_ticket_description_nulls_filled_spark_df",
                tags=tags,
                name="fill_transaction_ticket_description_nulls_with_unknown",
            ),
            node(
                func=map_line_ids,
                inputs=[
                    "hop_raw_w_transaction_ticket_description_nulls_filled_spark_df",
                    "params:line_id_mappings",
                    "params:hop_line_id_column",
                ],
                outputs="hop_spark_df_with_line_ids_mapped",
                tags=tags,
                name="map_hop_line_ids",
            ),
            node(
                func=remove_unknown_stops,
                inputs=["hop_spark_df_with_line_ids_mapped"],
                outputs="hop_spark_df_without_unknown_stops",
                tags=tags,
                name="remove_unknown_stops",
            ),
            node(
                func=create_datetime_column,
                inputs=[
                    "hop_spark_df_without_unknown_stops",
                    "params:hop_raw_datetime_column",
                ],
                outputs="hop_spark_df_with_datetime",
                tags=tags,
                name="create_datetime_column",
            ),
            node(
                func=create_date_column,
                inputs=["hop_spark_df_with_datetime"],
                outputs="hop_spark_df_with_date",
                tags=tags,
                name="create_date_column_for_hop_taps",
            ),
            node(
                func=get_year_months,
                inputs=[
                    "hop_spark_df_with_date",
                    "params:hop_prepared_service_date_column",
                    "params:hop_prepared_year_months",
                ],
                outputs="raw_hop_spark_df_with_year_month_selected",
                tags=tags,
                name="select_year_months_to_run_prep_for",
            ),
            node(
                func=identify_unwanted_operators,
                inputs=[
                    "raw_hop_spark_df_with_year_month_selected",
                    "params:ignored_service_names",
                ],
                outputs="hop_spark_df_prepared_spark_df",
                tags=tags,
                name="remove_unwanted_operators",
            ),
            node(
                func=build_stop_times_from_stops_avl,
                inputs=["stop_times_avl_raw_w_upper_case_column_names_spark_df"],
                outputs="stop_times_avl_with_arrival_and_departure_datetimes_spark_df",
                tags=tags,
                name="get_arrival_and_departure_datetimes_for_stops_avl",
            ),
            node(
                func=get_relevant_stop_times,
                inputs=[
                    "hop_spark_df_prepared_spark_df",
                    "stop_times_avl_with_arrival_and_departure_datetimes_spark_df",
                ],
                outputs="relevant_stop_times_avl_spark_df",
                tags=tags,
                name="get_relevant_stop_times_from_hop_data",
            ),
            node(
                func=merge_trip_information_onto_avl_stop_times,
                inputs=[
                    "relevant_stop_times_avl_spark_df",
                    "trips_gtfs_raw_w_upper_case_column_names_spark_df",
                ],
                outputs="stop_times_avl_with_trip_information_spark_df",
                tags=tags,
                name="merge_trip_information_onto_avl_stop_times",
            ),
            node(
                func=convert_string_route_ids_to_integers,
                inputs=["stop_times_avl_with_trip_information_spark_df"],
                outputs="stop_times_avl_with_a_routes_modified_spark_df",
                tags=tags,
                name="convert_string_route_ids_to_integers",
            ),
            node(
                func=add_source_column,
                inputs=[
                    "stop_times_avl_with_a_routes_modified_spark_df",
                    "params:avl_source_value",
                ],
                outputs="stop_times_avl_with_with_source_column_spark_df",
                tags=tags,
                name="add_source_column_to_avl_stop_times",
            ),
            node(
                func=merge_stop_locations_onto_stop_times,
                inputs=[
                    "stop_times_avl_with_with_source_column_spark_df",
                    "stops_gtfs_raw_w_upper_case_column_names_spark_df",
                ],
                outputs="stop_times_spark_df",
                tags=tags,
                name="merge_stop_locations_onto_stop_times",
            ),
            node(
                func=coalesce_stop_times,
                inputs=["stop_times_spark_df"],
                outputs="stop_times_prepared_spark_df_w_times_coalesced",
                tags=tags,
                name="coalesce_stop_times",
            ),
            node(
                func=build_stop_headways,
                inputs=["stop_times_prepared_spark_df_w_times_coalesced"],
                outputs="stop_times_prepared_spark_df_w_headways",
                tags=tags,
                name="build_stop_headways",
            ),
            node(
                func=map_line_ids,
                inputs=[
                    "stop_times_prepared_spark_df_w_headways",
                    "params:line_id_mappings",
                    "params:gtfs_line_id_column",
                ],
                outputs="stop_times_with_line_ids_mapped_spark_df",
                tags=tags,
                name="map_line_ids_for_stop_times",
            ),
            node(
                func=get_arrival_datetime_hour,
                inputs=["stop_times_with_line_ids_mapped_spark_df"],
                outputs="stop_times_with_arrival_datetime_hour_spark_df",
                tags=tags,
                name="get_arrival_datetime_hour",
            ),
            node(
                func=repartition_by,
                inputs=[
                    "stop_times_with_arrival_datetime_hour_spark_df",
                    "params:stop_times_repartition_columns",
                ],
                outputs="stop_times_prepared_spark_df",
                tags=tags,
                name="repartition_stop_times_by_service_date_and_hour",
            ),
            node(
                func=get_trip_id_start_end_stop_ids_and_times,
                inputs=["stop_times_spark_df"],
                outputs="stop_times_with_start_end_stop_ids_and_times_spark_df",
                tags=tags,
                name="get_trip_id_start_stop_ends",
            ),
            node(
                func=build_blocks,
                inputs=["stop_times_with_start_end_stop_ids_and_times_spark_df"],
                outputs="blocks_with_interlining_trip_ids_spark_df",
                tags=tags,
                name="build_blocks",
            ),
            node(
                func=create_interlining_trips,
                inputs=["blocks_with_interlining_trip_ids_spark_df"],
                outputs="interlining_trip_ids_prepared_spark_df",
                tags=tags,
                name="create_interlining_trips",
            ),
        ]
    )
