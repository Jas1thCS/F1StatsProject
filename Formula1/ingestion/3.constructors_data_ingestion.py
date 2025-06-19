# Databricks notebook source
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.option("header",True) \
.schema(constructors_schema) \
.csv("abfss://<container>@<storage_account>.dfs.core.windows.net/constructors.csv")

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")


# COMMAND ----------

display(spark.read.parquet("abfss://<container>@<storage_account>.dfs.core.windows.net/constructors"))

# COMMAND ----------

#pyspark method
constructor_final_df.write.mode("overwrite").parquet("abfss://<container>@<storage_account>.dfs.core.windows.net/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
