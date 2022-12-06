# parquet-delta-spark

Clone the file to your local repository<br>
`git-clone https://github.com/kartikc13/parquet-delta-spark.git`

Run the spark submit command (ensure to start the Spark master prior to this)<br>
`$SPARK_HOME/bin/spark-submit --packages io.delta:delta-core_2.12:1.2.1 "/Users/I077063/Documents/GitHub/parquet-delta-spark/parquet_to_delta.py"`<br>
**NOTE:** This works only in Linux systems. Also, update the file location in the command as per the directory in your local system.<br>
$SPARK_HOME --> The location of your spark installation until `/libexec`
