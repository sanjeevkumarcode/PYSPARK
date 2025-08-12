# Databricks notebook source
df = spark.read.option('header', True).csv('dbfs:/FileStore/CSV_1207.csv')
df = df.withColumnRenamed('Total Cases','Total_Cases')
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.filter(df.Year.startswith('2019'))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.filter(df.Year.endswith('19'))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.filter(df.Year.contains('2019'))
display(df1)