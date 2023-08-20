# Databricks notebook source
load_driver= spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/formulacsv/drivers.csv")
load_races= spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/formulacsv/races.csv")
load_results= spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/formulacsv/results.csv")
load_constructors= spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/formulacsv/constructors.csv")
load_circuits= spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/formulacsv/circuits.csv")


load_races.write.mode("overwrite").parquet("/mnt/formulaProcessed/races")
load_results.write.mode("overwrite").parquet("/mnt/formulaProcessed/results")
load_driver.write.mode("overwrite").parquet("/mnt/formulaProcessed/drivers")
load_constructors.write.mode("overwrite").parquet("/mnt/formulaProcessed/constructors")
load_circuits.write.mode("overwrite").parquet("/mnt/formulaProcessed/circuits")





# COMMAND ----------

driver_filename=dbutils.fs.ls('/mnt/formulaProcessed/drivers')
driver_file_name=""
for files in driver_filename:
    if files.name.startswith('part-'):
        driver_file_name=files.name
        old_driver_path ='/mnt/formulaProcessed/drivers/'+driver_file_name
        new_driver_path ='/mnt/formulaProcessed/drivers/drivers'
        dbutils.fs.mv(old_driver_path,new_driver_path)

circuit_filename=dbutils.fs.ls('/mnt/formulaProcessed/circuits')
circuit_file_name=""
for files_circuit in circuit_filename:
    if files_circuit.name.startswith('part-'):
        circuit_filename=files_circuit.name
        old_circuit_path ='/mnt/formulaProcessed/circuits/'+circuit_filename
        new_circuit_path ='/mnt/formulaProcessed/circuits/circuits'
        dbutils.fs.mv(old_circuit_path,new_circuit_path)


# Rename files in the "results" directory
results_filenames = dbutils.fs.ls('/mnt/formulaProcessed/results')
new_results_path = '/mnt/formulaProcessed/results/results'

for file_info in results_filenames:
    if file_info.name.startswith('part-'):
        old_results_path = '/mnt/formulaProcessed/results/'+file_info.name
        dbutils.fs.mv(old_results_path, new_results_path)

# Rename files in the "constructors" directory
constructors_filenames = dbutils.fs.ls('/mnt/formulaProcessed/constructors')
new_constructors_path = '/mnt/formulaProcessed/constructors/constructors'

for file_info in constructors_filenames:
    if file_info.name.startswith('part-'):
        old_constructors_path ='/mnt/formulaProcessed/constructors/'+file_info.name
        dbutils.fs.mv(old_constructors_path, new_constructors_path)

# Rename files in the "constructorStandings" directory
constructor_standings_filenames = dbutils.fs.ls('/mnt/formulaProcessed/constructorStandings')
new_constructor_standings_path = '/mnt/formulaProcessed/constructorStandings/constructorStandings'

for file_info in constructor_standings_filenames:
    if file_info.name.startswith('part-'):
        old_constructor_standings_path = '/mnt/formulaProcessed/constructorStandings/'+file_info.name
        dbutils.fs.mv(old_constructor_standings_path, new_constructor_standings_path)

# Rename files in the "races" directory
races_filenames = dbutils.fs.ls('/mnt/formulaProcessed/races')
new_races_path = '/mnt/formulaProcessed/races/races'

for file_info in races_filenames:
    if file_info.name.startswith('part-'):
        old_races_path = '/mnt/formulaProcessed/races/'+file_info.name
        dbutils.fs.mv(old_races_path, new_races_path)



