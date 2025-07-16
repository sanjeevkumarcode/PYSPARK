# Databricks notebook source
# Write modes -> 1. Overwrite 2. Append 3. Ignore 4. Error

# COMMAND ----------

df1 = spark.read.option('header',True).csv('dbfs:/FileStore/CSV_1207.csv')
display(df1)

# COMMAND ----------

df1.count()

# COMMAND ----------

df1.write.csv('dbfs:/Output/CSV_1707.csv')

# COMMAND ----------

df1.write.option('header',True).mode('overwrite').csv('dbfs:/Output/CSV_1707.csv')

# COMMAND ----------

df2 = spark.read.option('header',True).csv('dbfs:/Output/CSV_1707.csv')
df2.count()

# COMMAND ----------

