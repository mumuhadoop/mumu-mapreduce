#!/bin/bash

NGINX_ACCESSLOG_URL=http://120.77.68.57/log/access.log-
NGINX_ACCESSLOG_URL2=http://120.77.13.93/log/access.log-


for DATESTRING in $(seq 20170510 20171012)
do
  for NGINX_ACCESSLOG in $NGINX_ACCESSLOG_URL $NGINX_ACCESSLOG_URL2
  do
     wget $NGINX_ACCESSLOG$DATESTRING
     if [ -e access.log-$DATESTRING ] ;then
       cat access.log-$DATESTRING >> $DATESTRING
       rm -rf access.log-$DATESTRING
     fi
  done
done