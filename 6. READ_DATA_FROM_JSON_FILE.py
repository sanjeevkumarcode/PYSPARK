# Databricks notebook source
df = spark.read.option('singleLine', True).json('dbfs:/FileStore/titanic_parquet.json')
display(df)