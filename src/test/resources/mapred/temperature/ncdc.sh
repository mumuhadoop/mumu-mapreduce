#!/bin/bash

NCDC_FTP=ftp://ftp.ncdc.noaa.gov/pub/data/gsod

for YEAR in $(seq 1901 2017)
do
  wget $NCDC_FTP/$YEAR/gsod_$YEAR_$YEAR.tar
done