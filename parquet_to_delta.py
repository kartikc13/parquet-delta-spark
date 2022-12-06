#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from delta.tables import *
import os


def read_parquet(spark, schema, readpath):
    print("****************Reading parquet file.....****************")
    parquet_data = spark.read.schema(schema).parquet(readpath)

    print("****************Parquet file read complete!****************")
    return parquet_data


def write_delta(spark, data, writepath):
    print("****************Writing data into delta table.....****************")
    parquet_data = data.write.format("delta")\
                                .mode("overwrite")\
                                .save(writepath)
    print("****************Delta write complete!****************")


def print_delta(spark, deltapath):
    delta_exists = DeltaTable.isDeltaTable(spark, deltapath)
    if delta_exists:
        print("****************Printing data from delta table.....****************")
        deltaTable = DeltaTable.forPath(spark, deltapath)
        deltaDF = deltaTable.toDF()
        deltaDF.withColumn("description", f.expr("filter(name, x -> x.lang = 'en-US')")[0]["content"])\
                .drop("name")\
                .show(10, False)



spark = SparkSession.builder \
    .appName("parquet_to_delta") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

readpath = "/Users/I077063/Documents/GitHub/parquet-delta-spark/sample-data/currencycodes.snappy.parquet"
writepath = '/Users/I077063/Documents/GitHub/parquet-delta-spark/sample-data/delta-data'
schema = StructType([ \
    StructField("internalUUID",StringType(),True), \
    StructField("code",StringType(),True), \
    StructField('name', ArrayType(StructType([
       StructField('lang', StringType(), True),\
       StructField('content', StringType(), True)\
       ])), True)\
  ])

# Cleanup the delta table from previous runs
os.system(f"rm -rf {writepath}")

# Read the parquet file
data = read_parquet(spark, schema, readpath)

# Write the parquet contents into delta table
write_delta(spark, data, writepath)

# Read the delta table and print the entries
print_delta(spark, writepath)
