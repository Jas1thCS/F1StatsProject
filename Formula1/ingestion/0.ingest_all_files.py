# Databricks notebook source
v_result = dbutils.notebook.run("1_races_data_ingestion", 0, {"p_data_source": "F1Data"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.circuits_data_ingestion", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.constructors_data_ingestion", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.drivers_data_ingestion", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.race_results_ingestion", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_qualifying_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result
