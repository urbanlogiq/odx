# ODX Inference Pipeline

This directory contains the code for the ODX Inference pipeline within the Kedro framework. The primary function of this pipeline is to infer ODX alightings and transfers using the data prepared by the preceding 'prepare' pipeline. At a high level, journey inference is done follow these steps:
1. For a boarding tap, find possible boarding events in the AVL data that correspond to a vehicle the rider may have boarded on.
2. Find the set of given stops where the rider could have gotten off, given their next boarding location.
3. Evaluate each possible alighting location according to how much it minimizes the travel time between the alighting location and the next boarding location.
4. Pick the most efficient alighting location.
5. Decide if the alighting is a transfer or a destination by comparing the headway of the route associated with the next boarding event to the wait time of the rider. If the rider waits much longer than the headway, then we deem it to be a destination (i.e. the rider skipped many boarding opportunities). If not, it is a transfer.
6. Define journeys as sets of boarding/alighting events bookended by destination events.

Key components of this pipeline include:

- `nodes.py`: This is the core file where the inference logic is implemented. It contains functions for validating successful journeys, calculating run metrics, and other processes central to the inference of ODX alightings and transfers.

- `pipeline.py`: This file defines the structure of the pipeline by organizing and connecting the nodes created in `nodes.py`. It outlines the flow of data and the dependencies between the processing steps.

- `impossible_conditions_and_descriptions.py`: This file contains the set of conditions that deem an inferred journey as impossible. Those tagged as impossible are filtered out.

For a comprehensive understanding of the functions and their roles in the pipeline, please refer to the documentation within each file.

