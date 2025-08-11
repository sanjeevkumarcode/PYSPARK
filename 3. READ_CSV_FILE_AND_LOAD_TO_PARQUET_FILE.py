# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

df1 = spark.read.option('header',True).csv('dbfs:/FileStore/CSV_1207.csv')
display(df1)

# COMMAND ----------

df1.write.option('header',True).parquet('dbfs:/FileStore/sales11')

# COMMAND ----------

df2 = spark.read.option('header',True).parquet('dbfs:/FileStore/sales11')
                                         

# COMMAND ----------

display(df2)