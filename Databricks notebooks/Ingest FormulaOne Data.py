# Databricks notebook source
# DBTITLE 1,INGEST FORMULAONE DATA
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from py4j.java_gateway import java_import
from pyspark.sql.functions import current_timestamp

#drivers

df_drivers= spark.read.format("json").option("multiline","true").option("inferSchema", "true").load("/mnt/formulaOne/drivers.json")
df_exploded_drivers = df_drivers.selectExpr("MRData.DriverTable.Drivers as Drivers").select(explode("Drivers").alias("Driver"))
final_df_drivers = df_exploded_drivers.select("Driver.code", "Driver.dateOfBirth", "Driver.driverId", "Driver.familyName", "Driver.givenName", "Driver.nationality", "Driver.permanentNumber", "Driver.url")

final_df_drivers=final_df_drivers.withColumn("ingestion_date", current_timestamp())


#circuits

df_circuit= spark.read.format("json").option("multiline","true").option("inferSchema", "true").load("/mnt/formulaOne/circuits.json")
df_exploded_circuits = df_circuit.selectExpr("MRData.CircuitTable.Circuits as Circuits").select(explode("Circuits").alias("Circuits"))
final_df_exploded_circuits = df_exploded_circuits.select("Circuits.circuitId", "Circuits.circuitName", "Circuits.url","Circuits.Location.country", "Circuits.Location.lat", "Circuits.Location.locality", "Circuits.Location.long")

final_df_exploded_circuits=final_df_exploded_circuits.withColumn("ingestion_date", current_timestamp())

#Constructors

df_constructors= spark.read.format("json").option("multiline","true").option("inferSchema", "true").load("/mnt/formulaOne/constructors.json")
df_exploded_constructors = df_constructors.selectExpr("MRData.ConstructorTable.Constructors as Constructors").select(explode("Constructors").alias("Constructors"))
final_df_exploded_constructors = df_exploded_constructors.select("Constructors.constructorId", 
                                                                 "Constructors.name", "Constructors.nationality","Constructors.url")

final_df_exploded_constructors=final_df_exploded_constructors.withColumn("ingestion_date", current_timestamp())

#Constructor Standings
df_constructorStandings= spark.read.format("json").option("multiline","true").option("inferSchema", "true").load("/mnt/formulaOne/constructorStandings")
exploded_constructorStandings = df_constructorStandings.selectExpr(" explode(MRData.StandingsTable.StandingsLists) as standingsLists")
exploded_constructorStandings1 = exploded_constructorStandings.selectExpr("explode(standingsLists.ConstructorStandings) as ConstructorStandings")

final_constructorStandings = exploded_constructorStandings1.select(
                            "constructorStandings.Constructor.constructorId",
                           "constructorStandings.Constructor.name",
                           "constructorStandings.Constructor.nationality",
                           "constructorStandings.Constructor.url",
                           "constructorStandings.points",
                           "constructorStandings.position",
                           "constructorStandings.positionText",
                           "constructorStandings.wins")

final_constructorStandings=final_constructorStandings.withColumn("ingestion_date", current_timestamp())


#races

df_races= spark.read.format("json").option("multiline","true").option("inferSchema", "true").load("/mnt/formulaOne/races.json")
#df_races.printSchema()
exploded_df_races = df_races.selectExpr(" explode(MRData.RaceTable.Races) as races")

final_exploded_df_races1=exploded_df_races.select("races.Circuit.circuitId","races.date","races.raceName","races.round","races.season","races.time","races.url")
#display(final_exploded_df_races1)

final_exploded_df_races1=final_exploded_df_races1.withColumn("ingestion_date", current_timestamp())


#results

df_results= spark.read.format("json").option("multiline","true").option("inferSchema", "true").load("/mnt/formulaOne/results.json")

df_exploded = df_results.selectExpr("MRData.RaceTable.Races as races").select(explode("races").alias("race"))

# Explode the "Results" array within the "race" struct
df_results_exploded = df_exploded.select(
    "race.season",
    "race.round",
    "race.raceName",
    "race.Circuit.circuitId",
    explode("race.Results").alias("result")
)

# Select columns from exploded results, along with driver and circuit information
final_df_results = df_results_exploded.selectExpr(
    "raceName",
    "circuitId",
    "result.Driver.driverId",
    "result.Constructor.constructorId",
    "result.grid",
    "result.laps",
    "result.number",
    "result.points",
    "result.position",
    "result.positionText",
    "result.status",
    "result.Time.time",
    "result.Time.millis",
)
final_df_results=final_df_results.withColumn("ingestion_date", current_timestamp())


display(final_df_results)

# COMMAND ----------

# DBTITLE 1,RAW LAYER(WRITE EXPLODE ROWS TO PROCESSED LAYER)
final_df_drivers.write.mode("overwrite").parquet("/mnt/formulaProcessed/drivers")

final_df_exploded_circuits.write.mode("overwrite").parquet("/mnt/formulaProcessed/circuits")

final_df_exploded_constructors.write.mode("overwrite").parquet("/mnt/formulaProcessed/constructors")

final_constructorStandings.write.mode("overwrite").parquet("/mnt/formulaProcessed/constructorStandings")

final_exploded_df_races1.write.mode("overwrite").parquet("/mnt/formulaProcessed/races")

final_df_results.write.mode("overwrite").parquet("/mnt/formulaProcessed/results")





# COMMAND ----------

