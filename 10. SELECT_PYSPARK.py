# Databricks notebook source
df = spark.read.option('header',True).csv('dbfs:/FileStore/CSV_1207.csv')
df = df.withColumnRenamed('State/UTs','States')
display(df)

# COMMAND ----------

display(df.select('States','Population'))

# COMMAND ----------

display(df.select('*'))

# COMMAND ----------

display(df.select('States', (df.Year.cast('int') * df.Year.cast('int')).alias('Total')))