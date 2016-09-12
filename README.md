

# example code for a friend at deloitte
This is a small example of using dataframes and dataset api in spark scala,
with a little machine learning thrown in.

I've provided a utility script in the bin folder for convienence. To to use;
run:

``` bash
./run.sh '/Users/greg/dev/spark/datasetExample/src/main/resources/data/*' s
```

or

``` bash
spark-submit \ --class Main \ --master local[*] \ --deploy-mode client \ ./dataset-example.jar '/path/to/your/data/*'
```

you can also run via sbt

``` bash
sbt run
```

or recreate the jars by typing

``` bash
sbt assembly
```
then copy the dataset-example.jar to the bin folder so that the run script will
work on the latest binary
