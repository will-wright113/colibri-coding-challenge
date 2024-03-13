# Databricks notebook source
# DBTITLE 1,Connection to Data Lake
spark.conf.set("fs.azure.account.key.colibritaskstadls.dfs.core.windows.net", dbutils.secrets.get(scope="secret-scope", key="colibritaskstadls-key"))

# COMMAND ----------

# DBTITLE 1,Cleansed Rescued Data Count
turbine_power_autoloader_rescued_data_count = spark.read.table("hive_metastore.turbine_power.turbine_power_autoloader").filter("_rescued_data IS NOT NULL").count()
expected_row_count = 5
print("expected_row_count : "+str(expected_row_count))
print("turbine_power_autoloader_rescued_data_count : "+str(turbine_power_autoloader_rescued_data_count))
assert expected_row_count == turbine_power_autoloader_rescued_data_count

# COMMAND ----------

# DBTITLE 1,Cleansed Turbine Power Count
turbine_power_count = spark.read.table("hive_metastore.turbine_power.turbine_power").count()
expected_row_count = (15 * 24 * 31) + 2
print("expected_row_count : "+str(expected_row_count))
print("turbine_power_count : "+str(turbine_power_count))
assert expected_row_count == turbine_power_count

# COMMAND ----------

# DBTITLE 1,Enriched Augmented Count
turbine_power_augmented_count = spark.read.table("hive_metastore.turbine_power.turbine_power_augmented").count()
expected_row_count = (15 * 24 * 31) + 2
print("expected_row_count : "+str(expected_row_count))
print("anomalies_count : "+str(turbine_power_augmented_count))
assert turbine_power_augmented_count == expected_row_count

# COMMAND ----------

# DBTITLE 1,Enriched Anomalies Count
anomalies_count = spark.read.table("hive_metastore.turbine_power.turbine_power_anomalies").count()
expected_row_count = 2
print("expected_row_count : "+str(expected_row_count))
print("anomalies_count : "+str(anomalies_count))
assert anomalies_count == expected_row_count

# COMMAND ----------

# DBTITLE 1,Aggregate Statistics Day Count
agg_turbine_power_statistics_day_count = spark.read.table("hive_metastore.turbine_power.agg_turbine_power_statistics_day").count()
expected_row_count = 15 * 31
print("expected_row_count : "+str(expected_row_count))
print("agg_turbine_power_statistics_day_count : "+str(agg_turbine_power_statistics_day_count))
assert agg_turbine_power_statistics_day_count == expected_row_count

# COMMAND ----------

# DBTITLE 1,Aggregate Statistics Week Count
agg_turbine_power_statistics_week_count = spark.read.table("hive_metastore.turbine_power.agg_turbine_power_statistics_week").count()
expected_row_count = 15 * 5
print("expected_row_count : "+str(expected_row_count))
print("agg_turbine_power_statistics_week_count : "+str(agg_turbine_power_statistics_week_count))
assert agg_turbine_power_statistics_week_count == expected_row_count

# COMMAND ----------

# DBTITLE 1,Aggregate Statistics Month Count
agg_turbine_power_statistics_month_count = spark.read.table("hive_metastore.turbine_power.agg_turbine_power_statistics_month").count()
expected_row_count = 15
print("expected_row_count : "+str(expected_row_count))
print("agg_turbine_power_statistics_month_count : "+str(agg_turbine_power_statistics_month_count))
assert agg_turbine_power_statistics_month_count == expected_row_count
