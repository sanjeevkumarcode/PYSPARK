# Databricks notebook source
df = spark.read.option('header',True).csv('dbfs:/FileStore/CSV_1207.csv')
display(df)

# COMMAND ----------

df1 = df.filter(df.Year.isin('2019','2011'))
display(df1)

# COMMAND ----------

df2 = df.filter(~df.Year.isin('2019','2011'))
display(df2)