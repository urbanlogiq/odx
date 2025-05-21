# Origin/Destination/Transfer (ODX) Inference
This repository contains code that allows users to infer Origin/Destination/Transfer events for transit riders in the TriMet service area.

## Transit Services Covered
Currently the code only considers trips that are completed in their entirety through the TriMet service network. Not included are trips that use:

- Portland Streetcar
- CTRAN

## Data Requirements
The code requires three key datasets to function:

- Extracts of TriMet's E-Fare Transaction Journal, sourced from the HOP FastPass System;
- Extracts of TriMet's Automated Vehicle Location (AVL) data covering the same date range as the transaction journal;
- Static GTFS data covering the same time range as the AVL data;

Schemas for these are provided in [conf/base/catalog](./conf/base/catalog/README.md).

## Structure
The code is structured according to the [kedro](https://kedro.readthedocs.io/en/stable/introduction/introduction.html) pipeline framework:

- Source code is located in [src](./src). There are two key pipelines:
  - `prepare`: Data is restructured and prepared for inference.
  - `infer`: Journeys are inferred combining the previously prepared data.
- Parameter configuration is located in [conf/base/parameters](./conf/base/parameters).
- Data catalog configuration is located in [conf/base/catalog](./conf/base/catalog).
- Data is located in [data](./data):
  - Raw data is located in [01_raw](./data/01_raw).
  - Intermediate data is located in [02_intermediate](./data/02_intermediate).
  - Outputs are located in [03_primary](./data/03_primary).

## Setup
### Docker (Recommended)
###### Requirements
- Docker
###### Build
```commandline
docker build -t odx .
```
### Manual
###### Requirements
- python3.8 (Recommend [pyenv](https://github.com/pyenv/pyenv) for python version management)
- poetry ([Installation here](https://python-poetry.org/docs/#installation))
- Apache Spark 3.2.1

###### Python
Using pyenv, install python3.8
```commandline
pyenv install 3.8.<version-of-choice>
```
and add it to your path using
```commandline
pyenv shell 3.8.<version-of-choice>
```
Tell poetry to use 3.8
```commandline
poetry env use 3.8
```
Make sure poetry is configured to use virtual environments:
```commandline
poetry config virtualenvs.create true
poetry config virtualenvs.in-project true
```
then spawn a virtualenv using
```commandline
poetry shell
```
and install the python dependencies
```commandline
poetry install --no-root
```

## Data Dependencies
### :bus: Adding in Static GTFS Files
Static GTFS files can be added by downloading and adding them to [data/01_raw/gtfs/](data/01_raw/gtfs). For each new GTFS archive, name the unarchived folder as `RELEASE_DATE=<date-gtfs-was-published>` where `<date-gtfs-was-published>` follows the YYYY-MM-DD format, e.g. `RELEASE_DATE=2020-09-11`. Historical GTFS feeds can be found [here](https://transitfeeds.com/p/trimet/43).

You can use the [scripts/download_and_prep_gtfs.py](./scripts/download_and_prep_gtfs.py) cli to download and prep GTFS data for a date range. E.G.

```bash
python scripts/download_and_prep_gtfs.py --start_date=2022-02-01 --end_date=2022-03-31 --download_directory=data/01_raw/gtfs
```

#### Test GTFS Data
To download the test GTFS data, run
```bash
python scripts/download_and_prep_gtfs.py --start_date=2022-02-01 --end_date=2022-03-31 --download_directory='data/00_test/gtfs/'
```
To generate some synthetic AVL data from downloaded gtfs file, run
```bash
python scripts/simulate_avl_from_gtfs.py --start_date=2022-02-01 --end_date=2022-03-31 --gtfs_location='data/00_test/gtfs/' --output_directory='data/00_test/stop_times_avl/'
```
The dates in the above commands correspond to the date ranges the synthetic HOP data is within.

## Running ODX
### Pipelines
- `odx`
- `odx.infer`
- `odx.prepare`
- `odx.test.prepare`
- `odx.test.infer`

`odx` will run both the `prepare` and `infer` pipelines at once, while `odx.*` will run each respective step. Tests on synthetic data can be run (after having followed the above steps re: GTFS and AVL simulation) using the `odx.text.*` pipelines. `prepare` and `infer` are Spark dependent, so you'll need to pay attention to [spark.yml](./conf/base/spark.yml). Here you can control the driver and executor memory allocations. For large runs (more than a month) you might need to bump those up. Alternatively, the CARD_IDs are cycled every month, so you can run one month at a time (see [parameters](./conf/base/parameters/odx.yml) for month selection).


### :construction: Manual Installation
Listing Pipelines
```commandline
kedro registry list
```
Running Pipelines
```commandline
kedro run --pipeline="<pipeline-name>"
```

### 🐳 Docker (Recommended)
Docker is the recommended approach for a clean installation on most machines.

Listing Pipelines
```commandline
docker run --mount type=bind,src=$(pwd),dst=/root/code odx run registry list #UNIX/Mac
docker run --mount type=bind,src=$(cd),dst=/root/code odx run registry list #Windows
```
Running Pipelines
```commandline
docker run --mount type=bind,src=$(pwd),dst=/root/code odx run --pipeline="<pipeline-name>"
docker run --mount type=bind,src=$(cd),dst=/root/code odx run --pipeline="<pipeline-name>"
```


## :pushpin: Changelog
Below is the release history of all past versions of the ODX model.

#### v2.1 (Interlining Regression Fix)

The current code showed a regression in the inference of interlining journeys. The issues came from logical errors in the insertion of interlining events after the journey inference had been completed. Version V2.1 fixes these issues by:
* Replace component interlining trips with synthesized trips representing the entire journey of the vehicle, with interlining stops/trips marked as such
* Include a check for expected event type pairs and remove journeys that don't conform (approximately 3% of journeys). The primary source of unexpected event type pairs (e.g., a boarding followed by another boarding) seems to stem from riders who tap a while after they've boarded a bus. Producing adequate inferences in these cases would require a change in the overall inference approach
* Set the boarding time to the boarding trip arrival time rather than the observed tap time
* Adjust the impossible journeys conditions to reflect fixes/changes
* Include a metric calculating the number of journeys with an interlining event

#### v2.0 (Short Trips Fix)

In March 2024, it was identified that a significant set of journeys in the ODX data were shown to be 'single stop', i.e., a rider would board, ride one stop, then alight. This issue mainly affected MAX lines. Version V2.0 introduced a new MAX lines probability function, and it also:
* Increase the radius for alternate MAX boarding locations
* Introduce a validity score to help filter out unlikely journeys
* Remove journeys inferred based on identical subsequent boarding locations
* Fix normalization bug on MAX line/direction selection
* Improve code readability

#### v1.5 (Add Interlining)

* Introduce a boarding likelihood per mode (bus or MAX), replacing the previous one-size-fits-all approach
* Remove the assumption that the first tap in a 2.5-hour window was an origin. Origins are now defined based on gaps in tapping activity
* Remove the assumption that all events for a journey must occur within a 2.5-hour time block. Journeys are now delineated according to inferred rider decisions concerning boarding opportunities
* Replace the assumption that three missed boarding opportunities indicate an end to a journey was and use a headway-aware splitting approach instead
* Introduce interlining logic


#### v1.0 (Initial Release)

* Infer journeys such that the first tap in a 2.5-hour period is a trip origin
* Taps that occurred within 2.5 hours of the first tap are grouped into one journey headed towards a final destination
* When the origin and destination locations are within 200 ft of each other, the journey is tagged as a loop
* Every leg of a journey has a confidence score (ranging from 0 to 1) indicating the likelihood of that event