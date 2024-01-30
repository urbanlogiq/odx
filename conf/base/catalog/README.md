# Data Catalog
This `yaml` file contains all the configuration for the data consumed and produced by the ODX inference code. The general taxonomy looks like
```
<in-pipeline-dataset-name>:
  type: "<dataset-type>"
  filepath: "<path-to-data>"
  ...
  <additional-arguments>
```
Only datasets that are in the catalog are read in/written by the pipeline.

## Key Datasets

### Inputs

#### TriMet Rider Transaction Journey (HOP)
__Catalog Entry__
```
hop_raw_spark_df:
  type: "kedro.extras.datasets.spark.SparkDataSet"
  filepath: "data/01_raw/hop/"
  file_format: "parquet"
  generic_load_args:
    pathGlobFilter: "*.parquet.gzip"
```

__Schema__

| COLUMN | DESCRIPTION | TYPE |
|---|---|---|
| CARD_ID | An identifier that links transactions made by the same pass across time.<br>Shuffled every month to preserve privacy. | String |
| TAP_TIME | Datetime string 'YYYY-MM-DD HH:MM:SS' of the transaction datetime. In local (Portland) time (Timezone naive). | String |
| OPERATEDBY_DESCRIPTION | Operator for the transaction. E.G. TriMet, Portland Street Car (PSC) etc. | String |
| LINE_ID | GTFS identifier for the boarded line. Note: All MAX lines are mapped to 200 in the raw transaction data. | Integer |
| STOP_ID | GTFS identifier for the stop at which the transaction occurred. | Integer |
| DIRECTION_ID | Internal transaction journal identifier for the boarded direction. | Integer |

#### TriMet Automated Vehicle Locations

__Catalog Entry__

```
stop_times_avl_raw_spark_df:
  type: "kedro.extras.datasets.spark.SparkDataSet"
  filepath: "data/01_raw/stop_times_avl/"
  file_format: "parquet"
```

 __Schema__

| COLUMN | DESCRIPTION | TYPE |
|---|---|---|
| trip_id | The GTFS trip identifier associated with the vehicle stop event. | Integer |
| stop_id | The GTFS stop identifier associated with the stop at which the vehicle stopped. | String |
| stop_sequence | The number of stops in the trip that have occurred up to and including this event. | Integer |
| scheduled_arrive_time | The time at which the vehicle was scheduled to arrive at the stop. | String |
| scheduled_departure_time | The time at which the vehicle was scheduled to depart from the stop. | String |
| actual_arrive_time | The actual time at which the vehicle arrived at the stop. | String |
| actual_departure_time | The actual time at which the vehicle departed from the stop. | String |
| vehicle_id | The unique identifier for the vehicle in question. | Integer |

#### Partitioned GTFS Files
__Catalog Entries__

```
<gtfs-file-type>_gtfs_raw_spark_df:
  type: "kedro.extras.datasets.spark.SparkDataSet"
  filepath: "data/01_raw/gtfs"
  file_format: "csv"
  load_args:
    header: true
    inferSchema: true
  generic_load_args:
    pathGlobFilter: "*<gtfs-file-type>.txt"
```

__Schema__

See: [GTFS Reference](https://developers.google.com/transit/gtfs/reference)

### Outputs

#### Inferred Rider Events

__Catalog Entry__

```
inferred_rider_events_spark_df:
  type: "kedro.extras.datasets.spark.SparkDataSet"
  filepath: "data/03_primary/rider_events_partitioned/"
  save_args:
    mode: "overwrite"
    partitionBy: "JOURNEY_START_DATE"
```

__Schema__

| COLUMN | DESCRIPTION | TYPE |
|---|---|---|
| JOURNEY_ID | The ID for a specific journey. Used to combine events into a complete journey. | String |
| DATETIME | A timezone naive (Portland Local time) datetime formatted as YYYY-MM-DD HH:MM:SS. | String |
| STOP_ID | The GTFS stop ID for the stop at which the event occurred. | Integer |
| CARD_ID | The CARD ID for the event. | String |
| LINE_ID | The GTFS Line ID for the event. Note: All MAX lines are mapped to 200 in the raw transaction data<br>and show up as such here. | Integer |
| STOP_LAT | The latitude of the stop at which the event occurred. | Float |
| STOP_LON | The longitude of the stop at which the event occurred. | Float |
| DIRECTION_ID | Internal transaction journal identifier for the boarded direction | Integer |
| EVENT | The event that occurred. Either "BOARDED" or "ALIGHTED" | String |
| CONFIDENCE | The confidence that the event occurred at this location, at this time. Between 0 and 1. | Float |
| EVENT_TYPE | The type of the event. One of: "Origin", "MID_JOURNEY","DESTINATION". | String |
| IS_LOOP | Whether or not the journey was identified as a loop (same origin and destination). | Boolean |


