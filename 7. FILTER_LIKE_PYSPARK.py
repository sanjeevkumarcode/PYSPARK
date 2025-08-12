# Databricks notebook source
df = spark.read.option('header', True).csv('dbfs:/FileStore/CSV_1207.csv')
display(df)

# COMMAND ----------

df = df.withColumnRenamed('State/UTs','States')

display(df)

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumnRenamed('Total Cases', 'Total_Cases')
df1 = df.filter(df.States == "Goa")
display(df1)                          

# COMMAND ----------

from pyspark.sql.functions import *
df = df.withColumnRenamed('Total Cases', 'Total_Cases')
df1 = df.filter(col("States") == "Goa")
display(df1) 

# COMMAND ----------

from pyspark.sql.functions import *
df = df.withColumnRenamed('Total Cases', 'Total_Cases')
df1 = df.filter(col("States").like("Himachal%"))
display(df1) 