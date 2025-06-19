# Databricks notebook source
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema).option("header",True) \
.csv("abfss://<container>@<storage_account>.dfs.core.windows.net/drivers.csv")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name", concat(col("forename"), lit(" "), col("surname")))

display(drivers_with_columns_df)

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url")).drop(col("surname")).drop(col("forename"))

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")


# COMMAND ----------

display(spark.read.parquet("abfss://<container>@<storage_account>.dfs.core.windows.net/drivers"))

# COMMAND ----------

#pyspark method
drivers_final_df.write.mode("overwrite").parquet("abfss://<container>@<storage_account>.dfs.core.windows.net/drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
