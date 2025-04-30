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

from joblib import Parallel, delayed
from pandas import date_range
import zipfile
import requests
import pandas as pd
import shutil
from io import BytesIO
from pathlib import Path
from typing import Union
import argparse


def download_and_write(link, output_directory: Union[Path, None] = None):
    date = str(link).split("/")[-2]
    u = requests.get(link, stream=True)
    if u.ok:
        z = zipfile.ZipFile(BytesIO(u.content))
        if output_directory:
            directory = Path(output_directory) / Path(
                f"RELEASE_DATE={date[0:4]}-{date[4:6]}-{date[6:]}"
            )
        else:
            directory = Path(f"RELEASE_DATE={date[0:4]}-{date[4:6]}-{date[6:]}")
        directory.mkdir(exist_ok=True)
        z.extractall(directory)
        
        # Convert each txt file to parquet
        for file in directory.glob("*.txt"):
            # Read the CSV file
            df = pd.read_csv(file)
            # Write to parquet
            parquet_file = file.with_suffix('.parquet')
            df.to_parquet(parquet_file, index=False)
            # Remove the original txt file
            file.unlink()
    else:
        pass


def download_and_prep_data(start_date: str, end_date: str, download_directory: Path):
    _dates = [str(_date.date()) for _date in date_range(start_date, end_date)]
    links_template = "https://transitfeeds.com/p/trimet/43/{}/download"
    download_directory.mkdir(exist_ok=True, parents=True)
    links = [links_template.format(_date.replace("-", "")) for _date in _dates]
    results = Parallel(n_jobs=-1, verbose=10)(
        delayed(download_and_write)(link, download_directory) for link in links
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and prepare data.")
    parser.add_argument(
        "--start_date", type=str, required=True, help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--end_date", type=str, required=True, help="End date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--download_directory",
        type=str,
        required=True,
        help="Directory to download the data",
    )

    args = parser.parse_args()

    download_and_prep_data(
        args.start_date, args.end_date, Path(args.download_directory)
    )
