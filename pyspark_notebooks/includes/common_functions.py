# Databricks notebook source
from pyspark.sql.functions import when, col, cast, count

# COMMAND ----------

# MAGIC %md
# MAGIC #### Count Null values

# COMMAND ----------

def count_null(df):
    display(df.select([count(when(col(c).isNull(), c)).alias(c+'_null_count') for c in df.columns]))

