# Databricks notebook source
df = spark.read.option('header',True).csv('dbfs:/FileStore/CSV_1207.csv')
display(df)

# COMMAND ----------

df.schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema = StructType([
    StructField('State/UTs', StringType(), True),
    StructField('TOtal Cases', StringType(), True),
    StructField('Population', StringType(), True),
    StructField('Year', StringType(), True)
])

df_new = spark.read.option('header', True).schema(schema).csv('dbfs:/FileStore/CSV_1207.csv')

