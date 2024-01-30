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
## Docker (Recommended)
### Requirements
- Docker
#### Build
```commandline
docker build -t odx .
```
## Manual
### Requirements
- python3.8 (Recommend [pyenv](https://github.com/pyenv/pyenv) for python version management)
- poetry ([Installation here](https://python-poetry.org/docs/#installation))
- Apache Spark 3.2.1

#### Python
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
### Adding in Static GTFS Files
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


### Manual Installation
#### Listing Pipelines
```commandline
kedro registry list
```
#### Running Pipelines
```commandline
kedro run --pipeline="<pipeline-name>"
```

### Docker (Recommended)
Docker is the recommended approach for a clean installation on most machines.
#### Listing Pipelines
```commandline
docker run --mount type=bind,src=$(pwd),dst=/root/code odx run registry list #UNIX/Mac
docker run --mount type=bind,src=$(cd),dst=/root/code odx run registry list #Windows
```
#### Running Pipelines
```commandline
docker run --mount type=bind,src=$(pwd),dst=/root/code odx run --pipeline="<pipeline-name>"
docker run --mount type=bind,src=$(cd),dst=/root/code odx run --pipeline="<pipeline-name>"
```

