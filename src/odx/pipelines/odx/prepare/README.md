# ODX Prepare Pipeline README

This folder contains the code for the Kedro pipeline responsible for preparing data extracted from TriMet's enterprise databases for the inference pipeline. The primary goal of this pipeline is to ensure that the data is in the correct format and contains all necessary columns for subsequent inference processes.

## Key Operations

- **Formatting GTFS Dates and Times**: The pipeline includes functions to convert the General Transit Feed Specification (GTFS) dates and times into a format that is suitable for the inference tasks. This ensures consistency and accuracy when comparing or processing temporal data.

- **Column Addition**: Essential for the inference pipeline, this step involves adding new columns to the dataset. These columns may include identifiers, flags, or other derived metrics that are crucial for the inference process.

- **Data Validation**: Before passing the data to the inference pipeline, it is validated to ensure there are no nulls in critical columns. This step is vital to maintain the integrity of the data and to avoid errors during inference.

- `nodes.py`: This is the core file where the preparation logic is implemented.

- `utils.py`: This file provides utility functions that support the operations within the pipeline, ensuring code reusability and modularity.

- `pipeline.py`: This file defines the structure of the pipeline by organizing and connecting the nodes created in `nodes.py`. It outlines the flow of data and the dependencies between the processing steps.
