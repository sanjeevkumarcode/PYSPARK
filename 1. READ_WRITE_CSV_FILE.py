# Databricks notebook source
df = spark.read.csv('https://datalakeadf23062025.blob.core.windows.net/adfdemo/input/test.csv')

# COMMAND ----------

df1 = spark.read.csv('dbfs:/FileStore/CSV_1207.csv')

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load('wasbs://adfdemo@datalakeadf23062025.blob.core.windows.net/input/test.csv')
display(df)

# COMMAND ----------

df1 = spark.read.csv('dbfs:/FileStore/CSV_1207.csv')
display(df1)

# COMMAND ----------

display(df1)

# COMMAND ----------

df2 = spark.read.option('header', True).csv('dbfs:/FileStore/CSV_1207.csv')

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.write.csv('dbfs:/Input/CSV_12007.csv')

# COMMAND ----------

