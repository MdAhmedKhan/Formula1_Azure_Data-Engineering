# Databricks notebook source

raw_folder_path = "/mnt/formulaOne"
processed_folder_path = "/mnt/formulaProcessed"
presentation_folder_path = "/mnt/formulaPresentation"


drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers/drivers")

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors/constructors") \
.withColumnRenamed("nationality", "team_nationality") \
    .withColumnRenamed("name", "team_name") \


circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits/circuits")

races_df = spark.read.parquet(f"{processed_folder_path}/races/races")\
    .withColumnRenamed("year","race_year")

results_df = spark.read.parquet(f"{processed_folder_path}/results/results")

#join the tables
race_results_df = results_df.join(races_df, results_df.raceId == races_df.raceId) \
                     .join(drivers_df, results_df.driverId == drivers_df.driverId) \
                    .join(constructors_df, results_df.constructorId == constructors_df.constructorId)\
                                .join(circuits_df, races_df.circuitId == circuits_df.circuitId) 

#extract required columns
constructor_standings_df = race_results_df.select("race_year","team_name","team_nationality", "grid", "fastestLap","points", "position")


#aggregate the points and wins
from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = constructor_standings_df \
.groupBy("race_year","team_name","team_nationality") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))


#save the result to presentation layer
constructor_standings_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")


# Rename files in the "Presentation/race_results" directory
constructor_standings_filenames = dbutils.fs.ls('/mnt/formulaPresentation/constructor_standings/')
new_constructor_standings_path = '/mnt/formulaPresentation/constructor_standings/constructorStandings'

for file_info in constructor_standings_filenames:
    if file_info.name.startswith('part-'):
        old_path = '/mnt/formulaPresentation/constructor_standings/'+file_info.name
        dbutils.fs.mv(old_path, new_constructor_standings_path)


display(constructor_standings_df)
