#! /bin/bash


spark-submit \
  --class Main \
  --master local[*] \
  --deploy-mode client \
  ./dataset-example.jar $1
