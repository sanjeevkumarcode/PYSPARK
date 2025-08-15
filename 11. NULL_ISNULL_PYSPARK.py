# Databricks notebook source
df = spark.read.option('header','true').csv('dbfs:/FileStore/CSV_1207.csv')
df=df.withColumnRenamed('State/UTs','States')
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
display(df.select('*', df.Population.isNull()))
display(df.filter(df.Population.isNotNull()))

# COMMAND ----------

