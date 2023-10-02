FROM ghcr.io/osgeo/gdal:ubuntu-full-latest
LABEL authors="malvandi"

RUN apt update
RUN apt install -y python3-pip

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requireements.txt