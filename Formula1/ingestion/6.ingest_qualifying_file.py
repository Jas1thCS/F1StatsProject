# Databricks notebook source
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True).option("header", "true") \
.csv("abfss://<container>@<storage_account>.dfs.core.windows.net/qualifying.csv")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
display(qualifying_df)

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

#final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/qualifying")
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(spark.read.parquet('abfss://<container>@<storage_account>.dfs.core.windows.net/qualifying'))

# COMMAND ----------

dbutils.notebook.exit("Success")
