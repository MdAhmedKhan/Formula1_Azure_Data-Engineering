# Databricks notebook source
presentation_folder_path = "/mnt/formulaPresentation/race_results"

#first, we can reuse the race_results that we created earlier
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

#aggregate the points and wins
from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team_name") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

#display(driver_standings_df)

#save the result to presentation layer
driver_standings_df.write.mode("overwrite").parquet('/mnt/formulaPresentation/driver_standings')


# Rename files in the "Presentation/driver_Standings" directory
driver_standings_filenames = dbutils.fs.ls('/mnt/formulaPresentation/driver_standings')
new_driver_Standings_path = '/mnt/formulaPresentation/driver_standings/driverStandings'

for file_info in driver_standings_filenames:
    if file_info.name.startswith('part-'):
        old_path = '/mnt/formulaPresentation/driver_standings/'+file_info.name
        dbutils.fs.mv(old_path, new_driver_Standings_path)

display(driver_standings_df)