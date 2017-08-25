#!/bin/bash
/usr/bin/spark2-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 5g \
  --executor-cores 5 \
  --driver-memroy 2g \
  --conf spark.dynamicAllocation.enabled=true \
  --queue etl \
./spark_wc_senseandsensibility.py
