# Databricks notebook source
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp, to_timestamp, col, lit, concat

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), True),
                                   StructField("year", IntegerType(), True),
                                   StructField("round", IntegerType(), True),
                                   StructField("circuitId", IntegerType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("date", StringType(), True),
                                   StructField("time", StringType(), True)])

# COMMAND ----------

races_df = spark.read.option("header",True).schema(races_schema).csv("abfss://<container>@<storage_account>.dfs.core.windows.net/races.csv")

display(races_df)

# COMMAND ----------


races_changed_df = races_df.withColumn("Ingestion Date", current_timestamp()).withColumn("Race_TimeStamp", to_timestamp(concat(col('date'), lit(" "), col('time')), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

races_selected_df = races_changed_df.select(
    col("raceId").alias("race_id"),
    col("circuitId").alias("circuit_Id"), 
    col("year").alias("race_year"),col("round"), 
    col("name"), 
    col("Ingestion Date").alias("ingestion_date"), 
    col("Race_TimeStamp").alias("race_timestamp")
    )

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")


# COMMAND ----------

display(spark.read.parquet("abfss://<container>@<storage_account>.dfs.core.windows.net/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")
