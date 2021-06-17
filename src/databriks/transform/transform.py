""" 
Developed in Databriks. 
High recommended run this script in this platform.
"""


# Importing dependencies

import csv
from getpass import getpass
from mysql.connector import connect
from pyspark.sql.functions import date_format, sum as sum_column

""" 
Getting data from AWS RDS and loading raw data in FS
"""

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

tables = ["Locations", "Measurements"]
for table in tables:
    df = spark.read \
        .format('jdbc') \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("url",  "jdbc:mysql://ornitorrinco.c1ufsvg02g8w.us-east-1.rds.amazonaws.com/ornitorrinco") \
        .option("user",  "admin") \
        .option("password",  "ornitorrinco123")\
        .option("dbtable",  table)\
        .load()

    df.write.json(f'raw/{table}_raw.json')

""" 
Getting raw data to clean and load them into FS in parquet format
(The clean data is partitioned by Year and Month)
"""

locations_df = spark.read.json('dbfs:/raw/Locations_raw.json')
measurements_df = spark.read.json('dbfs:/raw/Measurements_raw.json')

""" 
Mapping measurements to respective location data
and aggregating measurements by Country and Date
"""
data_df = locations_df.join(measurements_df, "Country") \
    .groupBy("Country", "CountryCode", "Date") \
    .agg(
    sum_column("Recovered").alias("Recovered"),
    sum_column("Deaths").alias("Deaths"),
    sum_column("Active").alias("Active"),
    sum_column("Confirmed").alias("Confirmed")
) \
    .withColumn("Year", date_format("Date", "y")) \
    .withColumn("Month", date_format("Date", "MM"))


# Creating partitions by Year and Month and save into DBFS
data_df.write.partitionBy("Year", "Month").saveAsTable(
    'Measurements_cleaned', format='parquet', mode='overwrite', path="dbfs:/cleaned/Measurements_cleaned")

""" 
Reading usefull data
"""

# Reading columns
spark.read.parquet("dbfs:/cleaned/Measurements_cleaned").select("Country",
                                                                "CountryCode", "Deaths", "Confirmed", "Date").orderBy("Date")
