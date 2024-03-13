# Databricks notebook source
# DBTITLE 1,Imports
import dlt
from pyspark.sql.functions import col, to_date, year, month, weekofyear, min, max, avg, stddev, when
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Environment Switch
mode = "test"

# COMMAND ----------

# DBTITLE 1,Paths and Variables
root_path = "abfss://"+mode+"@colibritaskstadls.dfs.core.windows.net/"
raw_turbine_power_path = root_path+"raw/turbine_power"
cleansed_turbine_power_autoloader_path = root_path+"cleansed/turbine_power_autoloader"
cleansed_turbine_power_path = root_path+"cleansed/turbine_power"
enriched_turbine_power_augmented_path = root_path+"enriched/turbine_power_augmented"
enriched_turbine_power_anomalies_path = root_path+"enriched/turbine_power_anomalies"
curated_agg_turbine_power_statistics_day_path = root_path+"curated/agg_turbine_power_statistics_day"
curated_agg_turbine_power_statistics_week_path = root_path+"curated/agg_turbine_power_statistics_week"
curated_agg_turbine_power_statistics_month_path = root_path+"curated/agg_turbine_power_statistics_month"

schema_hints = "timestamp TIMESTAMP, turbine_id INT, wind_speed FLOAT, wind_direction INT, power_output FLOAT"

data_quality = {
    "valid_timestamp" : "timestamp IS NOT NULL",
    "valid_turbine_id" : "turbine_id IS NOT NULL",
    "valid_wind_speed" : "wind_speed IS NOT NULL",
    "valid_wind_direction" : "wind_direction IS NOT NULL",
    "valid_power_output" : "power_output IS NOT NULL"
}

# COMMAND ----------

# DBTITLE 1,Raw to Cleansed - Autoloader
@dlt.table(
    name="turbine_power_autoloader",
    path=cleansed_turbine_power_autoloader_path
)
def autoloader():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaHints", schema_hints)
        .option("cloudFiles.allowOverwrites", True)
        .load(raw_turbine_power_path)
        .withColumn("meta_file_name",col("_metadata.file_name"))
        .withColumn("meta_file_modification_time",col("_metadata.file_modification_time"))
    )

# COMMAND ----------

# DBTITLE 1,Cleansed - Autoloader to CDC Table
dlt.create_streaming_table(
    name="turbine_power",
    path=cleansed_turbine_power_path,
    expect_all_or_drop=data_quality
)

dlt.apply_changes(
    target="turbine_power",
    source="turbine_power_autoloader",
    keys=["timestamp","turbine_id"],
    sequence_by = col("meta_file_modification_time"),
    except_column_list = ["_rescued_data"],
    stored_as_scd_type = 1
)

# COMMAND ----------

# DBTITLE 1,Cleasned to Enriched - Augmentation and Anomalies
@dlt.table(
    name="turbine_power_augmented",
    path=enriched_turbine_power_augmented_path
)
def augment():
    return (
        dlt.read("turbine_power")
        .withColumn("mean_power_output", avg(col("power_output")).over(Window.partitionBy(col("turbine_id"))))
        .withColumn("stddev_power_output", stddev(col("power_output")).over(Window.partitionBy(col("turbine_id"))))
        .withColumn(
            "is_anomaly", 
            when(col("power_output") >= col("mean_power_output")+2*col("stddev_power_output"), True)
            .when(col("power_output") <= col("mean_power_output")-2*col("stddev_power_output"), True)
            .otherwise(False)
        )
    )

@dlt.table(
    name="turbine_power_anomalies",
    path=enriched_turbine_power_anomalies_path
)
def anomalies():
    return (
        dlt.read("turbine_power_augmented")
        .filter(col("is_anomaly") == True)
    )

# COMMAND ----------

# DBTITLE 1,Enriched to Curated - Aggregations
@dlt.table(
    name="agg_turbine_power_statistics_day",
    path=curated_agg_turbine_power_statistics_day_path
)
def statistic():
    return (
        dlt.read("turbine_power_augmented")
        .filter(col("is_anomaly") == False)
        .groupby(col("turbine_id"),to_date(col("timestamp")).alias("date"))
        .agg(
            min(col("power_output")).alias('min_power_output'),
            max(col("power_output")).alias('max_power_output'),
            avg(col("power_output")).alias('avg_power_output'))
    )

@dlt.table(
    name="agg_turbine_power_statistics_week",
    path=curated_agg_turbine_power_statistics_week_path
)
def statistic():
    return (
        dlt.read("turbine_power_augmented")
        .filter(col("is_anomaly") == False)
        .groupby(col("turbine_id"),year(col("timestamp")).alias("year"),weekofyear(col("timestamp")).alias("week"))
        .agg(
            min(col("power_output")).alias('min_power_output'),
            max(col("power_output")).alias('max_power_output'),
            avg(col("power_output")).alias('avg_power_output'))
    )

@dlt.table(
    name="agg_turbine_power_statistics_month",
    path=curated_agg_turbine_power_statistics_month_path
)
def statistic():
    return (
        dlt.read("turbine_power_augmented")
        .filter(col("is_anomaly") == False)
        .groupby(col("turbine_id"),year(col("timestamp")).alias("year"),month(col("timestamp")).alias("month"))
        .agg(
            min(col("power_output")).alias('min_power_output'),
            max(col("power_output")).alias('max_power_output'),
            avg(col("power_output")).alias('avg_power_output'))
    )
