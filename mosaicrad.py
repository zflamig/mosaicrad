import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import timedelta
import datetime
import os
import pyart

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))


def get_nexrad_files_before(
    valid_time, time_window=timedelta(minutes=15), bucket="noaa-nexrad-level2"
):
    """Returns a dictionary with radar site IDs as the keys, and the latest object key before the valid time as the values."""
    radars = {}

    # If your valid_time is at the beginning of the day, you need to search the previous day too.
    prefixes = []
    start_time = valid_time - time_window
    while start_time <= valid_time:
        prefix = start_time.strftime("%Y/%m/%d/")
        if prefix not in prefixes:
            prefixes.append(prefix)
        start_time = start_time + time_window

    for prefix in prefixes:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        while True:
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp["Contents"]:
                key = obj["Key"]

                # We don't care about this file, it is something weird
                if "_MDM" in key:
                    continue

                parts = key.split("/")
                # If this doesn't match what we expect, ignore it
                if len(parts) != 5:
                    continue

                radar = parts[3]
                radar_datetime = parts[4][4:19]
                radar_datetime = datetime.datetime.strptime(
                    radar_datetime, "%Y%m%d_%H%M%S"
                )

                # If this file is from after the valid time, ignore it
                if valid_time < radar_datetime:
                    continue

                # The keys are in order, so just use the last one before the valid time
                # over-writing as we go
                radars[radar] = key

            try:
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            except KeyError:
                break

    return radars


def download_for_radars(radars, path="/tmp/", bucket="noaa-nexrad-level2"):
    for radarid, s3key in radars.items():
        file_path = path + radarid

        # don't redownload files we already have on disk
        if os.path.isfile(file_path):
            continue

        s3.download_file(bucket, s3key, file_path)


def mosaic_radars(radars, path="/tmp/"):
    radars_data = [
        pyart.io.read_nexrad_archive(path + radar) for radar, s3key in radars.items()
    ]

    print("Loaded all radars")

    gridded = pyart.map.map_to_grid(
        radars_data,
        grid_shape=(3, 3500, 7000),
        grid_limits=((0.0, 19000.0), (20.0, 50.0), (-130.0, -60.0)),
        grid_projection="+init=EPSG:4326",
        fields=["reflectivity"],
        min_radius=30,
        bsp=0.,
        h_factor=0.,
        max_refl=100,
    )
    gridded.write(path + "grid")


valid_time = datetime.datetime.utcnow().replace(
    year=2017, month=8, day=25, hour=0, minute=5, second=0, microsecond=0
)
print("Picking appropriate files")
radar_files = get_nexrad_files_before(valid_time)
print("Downloading")
download_for_radars(radar_files)
print("gridding")
mosaic_radars(radar_files)
