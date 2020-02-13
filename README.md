# Mosaic NEXRAD

Use PyArt to create gridded NEXRAD products over the CONUS

## Running

```
docker build -t rm .
docker run --rm -v `pwd`/mosaicrad.py:/mosaicrad.py -v `pwd`/tmp:/tmp -ti rm
```
