FROM python:3.7

RUN pip install --upgrade scipy netCDF4 matplotlib numpy boto3 pandas
RUN pip install --upgrade arm-pyart

COPY mosaicrad.py /mosaicrad.py

CMD python mosaicrad.py
