import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import timedelta
import datetime
import os
import pyart
import osgeo.osr as osr
import osgeo.gdal as gdal
import numpy as np

LIMITS_Y = (20.0, 55.0)
LIMITS_X = (-130.0, -60.0)
LIMITS_NX = 7000
LIMITS_NY = 3500

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
                if "_MDM" in key or ".tar" in key:
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
        grid_shape=(3, LIMITS_NY, LIMITS_NX),
        grid_limits=((0.0, 3000.0), LIMITS_Y, LIMITS_X),
        grid_origin_alt=0.0,
        grid_projection={"init": "EPSG:4326"},
        fields=["reflectivity"],
        min_radius=1.0,
        bsp=0.,
        h_factor=0.,
        max_refl=100,
        weighting_function="nearest",
    )
    
    return gridded

def write_grid(data, output_file):
    driver = gdal.GetDriverByName("GTiff")
    ny, nx = data.shape
    dst_ds = driver.Create(output_file, nx, ny, 1, gdal.GDT_Float32)

    y_res = (LIMITS_Y[1] - LIMITS_Y[0]) / ny
    x_res = (LIMITS_X[1] - LIMITS_X[0]) / nx

    gt = (LIMITS_X[0], x_res, 0, LIMITS_Y[1], 0, -y_res)
    dst_ds.SetGeoTransform(gt)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    dst_ds.SetProjection(srs.ExportToWkt())
    data = data.filled(-9999.0)

    data = np.flipud(data)
    dst_ds.GetRasterBand(1).WriteArray(data, 0, 0)
    dst_ds.GetRasterBand(1).SetNoDataValue(-9999.0)

    dst_ds = None


valid_time = datetime.datetime.utcnow().replace(
    year=2017, month=8, day=25, hour=0, minute=5, second=0, microsecond=0
)
print("Picking appropriate files")
radar_files = get_nexrad_files_before(valid_time)
print("Downloading")
download_for_radars(radar_files)
print("gridding")
gridded = mosaic_radars(radar_files)
print("saving")
write_grid(gridded["reflectivity"][1], "/tmp/grid.tif")
