# Databricks notebook source
dbutils.fs.ls('mnt/bronze/SalesLT/')

# COMMAND ----------

dbutils.fs.ls('mnt/silver/')

# COMMAND ----------

input_path = '/mnt/bronze/SalesLT/Address/Address.parquet'

# COMMAND ----------

df = spark.read.format('parquet').load(input_path)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType

df = df.withColumn("ModifiedDate",date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Doing Transformation for All Tables**

# COMMAND ----------


