# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,concat

raw_folder_path = "/mnt/formulaOne"
processed_folder_path = "/mnt/formulaProcessed"
presentation_folder_path = "/mnt/formulaPresentation"

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("forename", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

final_driver_df= drivers_df.withColumn("driver_name", concat(col("driver_name"),lit(' '),col("surname"))).drop("surname")

#display(final_driver_df)
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors/constructors") \
.withColumnRenamed("name", "team_name") \


circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits/circuits")\
    .withColumnRenamed("locality","location")

races_df = spark.read.parquet(f"{processed_folder_path}/races/races") \
.withColumnRenamed("year","race_year")\
.withColumnRenamed("name","race_name")\
.withColumnRenamed("date", "race_date") 

results_df = spark.read.parquet(f"{processed_folder_path}/results/results") \
.withColumnRenamed("time", "race_time") 

#join the tables
race_results_df = results_df.join(races_df, results_df.raceId == races_df.raceId) \
                     .join(final_driver_df, results_df.driverId == final_driver_df.driverId) \
                    .join(constructors_df, results_df.constructorId == constructors_df.constructorId)\
                                .join(circuits_df, races_df.circuitId == circuits_df.circuitId) 

#extract required columns
final_race_results_df = race_results_df.select("race_year", "race_name", "race_date", "location", "driver_name", "driver_number", "driver_nationality","team_name", "grid", "fastestLap", "race_time", "points", "position")


final_race_results_df.write.mode("overwrite").parquet("/mnt/formulaPresentation/race_results")



# Rename files in the "Presentation/race_results" directory
race_results_filenames = dbutils.fs.ls('/mnt/formulaPresentation/race_results/')
new_race_results_path = '/mnt/formulaPresentation/race_results/race_results'

for file_info in race_results_filenames:
    if file_info.name.startswith('part-'):
        old_path = '/mnt/formulaPresentation/race_results/'+file_info.name
        dbutils.fs.mv(old_path, new_race_results_path)

display(race_results_df)

