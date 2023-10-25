FROM ghcr.io/osgeo/gdal:ubuntu-small-3.7.2
LABEL authors="Mohammad Hesam Khorshidi, Morteza Malvandi"

RUN apt update
RUN apt install -y python3-pip

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt