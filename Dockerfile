FROM python:3.7

ENV PYTHONUNBUFFERED 1
RUN apt-get update && apt-get install -y gdal-bin libgdal-dev

RUN pip install --upgrade scipy netCDF4 matplotlib numpy boto3 pandas pyproj
RUN pip install --upgrade arm-pyart gdal==2.4.0

COPY mosaicrad.py /mosaicrad.py

CMD python mosaicrad.py
