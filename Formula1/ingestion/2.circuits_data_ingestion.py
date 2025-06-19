# Databricks notebook source
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False), 
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True), 
                                    StructField("location", StringType(), True), 
                                    StructField("country", StringType(), True), 
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), False),
                                    ])

# COMMAND ----------

circuits_df = spark.read.option("header",True).schema(circuits_schema).csv("abfss://<container>@<storage_account>.dfs.core.windows.net/circuits.csv")

display(circuits_df)

# COMMAND ----------

circuits_renamed_df = circuits_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("name", "circuit_name").withColumnRenamed("location", "circuit_location").withColumnRenamed("country", "circuit_country")

# COMMAND ----------


circuits_finaldf = circuits_renamed_df.withColumn("Ingestion Date", current_timestamp())

# COMMAND ----------

display(circuits_finaldf)

# COMMAND ----------

circuits_finaldf.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")


# COMMAND ----------

display(spark.read.parquet("abfss://<container>@<storage_account>.dfs.core.windows.net/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")
