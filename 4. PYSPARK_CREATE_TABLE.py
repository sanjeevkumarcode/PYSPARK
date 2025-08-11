# Databricks notebook source
from pyspark.sql.types import StructType,StructField, IntegralType, StringType, DecimalType, DateType

data=[(1,'sanjeev',30,4000),(2,'kumar',32,5000)]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name",StringType(), True),
    StructField("age",IntegerType(), True),
    StructField("salarry",IntegerType(), True)
])

df = spark.createDataFrame(data,schema)
display(df)