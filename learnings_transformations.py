# Databricks notebook source
import dlt
import pyspark.sql.functions as F

# COMMAND ----------

access_key = dbutils.secrets.get(scope="s3_secrets", key="access_key")
secret_key = dbutils.secrets.get(scope="s3_secrets", key="secret_key")
spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
file_location = "s3a://sales-files-akash/sales_data.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiline", "true") \
    .option("inferSchema", "true") \
    .load(file_location)
df.show(2)
