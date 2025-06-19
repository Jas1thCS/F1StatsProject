# Databricks notebook source
# MAGIC %run "../configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col
from pyspark.sql import functions as F

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality", "team") \
    .agg(
        F.sum("points").alias("total_points"),
        F.count(F.when(F.col("position") == 1, True)).alias("wins"),
        F.count(F.when(
            (F.col("position") == 1) | 
            (F.col("position") == 2) | 
            (F.col("position") == 3), True
        )).alias("podiums"),
        F.count(F.when(F.col("qual_position") == 1, True)).alias("pole_positions")
    )

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC use f1_presentation;
# MAGIC select * from f1_presentation.driver_standings;
