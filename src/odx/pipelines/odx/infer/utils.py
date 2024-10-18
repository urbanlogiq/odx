from pyspark.sql import functions as F,Column

def haversine_meters(
    lat_1_col: Column, lon_1_col: Column, lat_2_col: Column, lon_2_col: Column
) -> Column:
    haversine_column = (
        2
        * 6371000
        * F.asin(  # haversine formula
            F.sqrt(
                F.sin((F.radians(lat_1_col) - F.radians(lat_2_col)) / 2) ** 2
                + (
                    F.cos(F.radians(lat_1_col))
                    * F.cos(F.radians(lat_2_col))
                    * F.sin((F.radians(lon_1_col) - F.radians(lon_2_col)) / 2) ** 2
                )
            )
        )
    )
    return haversine_column