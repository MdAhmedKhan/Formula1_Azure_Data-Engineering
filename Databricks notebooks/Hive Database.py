# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists formula1;
# MAGIC

# COMMAND ----------

presentation_folder_path = "/mnt/formulaPresentation"

raceresults_df = spark.read.parquet(f"{presentation_folder_path}/race_results/race_results")

raceresults_df.write.mode("overwrite").format("parquet").saveAsTable("formula1.race_results")


driver_Standings_df = spark.read.parquet(f"{presentation_folder_path}/driver_standings/driverStandings")

driver_Standings_df.write.mode("overwrite").format("parquet").saveAsTable("formula1.driverStandings")


constructorStandings_df = spark.read.parquet(f"{presentation_folder_path}/constructor_standings/constructorStandings")

constructorStandings_df.write.mode("overwrite").format("parquet").saveAsTable("formula1.constructorStandings")


# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_name, SUM(points) as TotalPoints, count(1) as TotalRaces from formula1.race_results 
# MAGIC GROUP BY driver_name 
# MAGIC ORDER BY TotalPoints DESC
# MAGIC limit 20

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This table will persist across cluster restarts and allow various users across different notebooks to query this data.