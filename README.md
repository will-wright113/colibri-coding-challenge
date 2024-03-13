# Colibri Coding Challenge

## Requirements

Create a pipeline that can ingest, transform and aggregate a CSV dataset containing turbine power outputs.

Objectives:
- Clean Data
- Identify Anomalies
- Calculate Summary Statistics
- Store Intermediary Datasets

### Ingestion

- CSV that is appeneded daily
- Turbine IDs distributed across multiple files
- Sometimes entries are missed due to sensor malfunctions

## Solution

The solution makes the assumption that it will be implemented on a [modern analytics architecture with Azure Databricks](https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/articles/azure-databricks-modern-analytics-architecture). However, for this challenge the solution will focus on an Azure Databricks workspace, a Storage Account with the hierarchical namespace enabled (Data Lake) and a Key Vault for storing sensitive credentials.

For a data design pattern, rather than using the [medallion architecture](https://www.databricks.com/glossary/medallion-architecture) from Databricks, this solution will include more defined layers to communicate functinality and data quality. These layers are created as folders in a container within the Storage Account.

| **Data Layer** | **Description** |
|--|--|
| raw | Source data - should be in its original format (CSV, JSON, etc.) and reflect what is stored in the source system. |
| cleansed | Validates schema, supports automatic schema evolution, cleans data based on business rules and performs change data capture if applicable. |
| enriched | Augments and combines datasets together. |
| curated | Analytical modelling and aggregations. |

The pipeline will be implemented using the [Delta Live Tables](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/) declarative framework.

"Delta Live Tables is a declarative framework for building reliable, maintainable, and testable data processing pipelines. You define the transformations to perform on your data and Delta Live Tables manages task orchestration, cluster management, monitoring, data quality, and error handling."

Any sensitive information (i.e. the Storage Account key used for authentication) is stored in Key Vault and accessed via Databricks by using a [secret scope](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes).

### Technical Design (`turbine-power-dlt.py` and `turbine-power-dlt.json`)

![Delta-Live-Tables-Pipeline](/Delta-Live-Tables-Pipeline.png)

**Raw Layer**

`raw/turbine_power`

The source CSV data is loaded into a landing folder.

**Cleansed Layer**

`cleansed/turbine_power_autoloader`

Initially within the cleansed layer, the pipeline loads the raw CSV data into a streaming table using [Auto Loader](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/). This keeps track of files that have been received so that records are processed exaclty once. A schema hint has been provided to imporve the schema inference on such a small dataset. Also Auto Loader has been configured to allow overwrites so that when an existing file has been updated it can subsequently be processed (see [Further Improvements](#further-improvements) for a better ingestion process). Finally, some meta data columns are added to the dataset: file name and file modification time.

`cleansed/turbine_power`

The second step in the cleansed layer performs [change data capture](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/cdc) on the autoloader streaming table. This will ensure that updates to existing records or any late arriving data is processed correctly (e.g. bad/late data due to sensor malfunction). Some boolean conditions using [expectations](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/expectations) are also defined to capture data quality.

**Enriched Layer**

`enriched/turbine_power_augmented`

`enriched/turbine_power_anomalies`

Within the enriched layer, anomalies are calculated and flagged using the provided logic creating an augmented dataset. A separate table then gathers anomalous data for further investigation. Any dependent aggregation tables can filter out these anomalies by reading from the augmented dataset and using the flag.

**Curated Layer**

`curated/agg_turbine_power_statistics_day`

`curated/agg_turbine_power_statistics_week`

`curated/agg_turbine_power_statistics_month`

Three aggregation tables are created for min, max and average power over a day, week or month.

### Testing (`turbine-power-tests.py`)

The pipeline was tested by using the existing datasets as expected data and creating new datasets with malformed records. The testing notebook contains assertions on record counts for all the tables in the pipeline.

The `anomalous_data.csv` dataset contains two records that are considerably higher or lower than the average power output. This dataset is used to test the anomaly logic.

The `bad_data.csv` dataset contains malformed data - wrong typed data or empty fields for each column. The record count assertions validate that these records are dropped in the cleansed layer.

The current test cases are simplistic and can be expanded on.

### Further Improvements

- **Ingestion:** As Auto Loader is ideally meant to process each record exactly once, the current mechanism of updating an existing file with new records is inefficient as Auto Loader will have to process the whole file after each update. Instead the deltas or updates should be written to a new, separate file to stop the re-processing of already seen records.

- **Data Quality Checks and Quarantine:** The current data quality checks are very simple and can be expanded on. This would require further consultation, but could include ensuring `wind_direction` is between 0 and 360 (assumption this measurement is in degrees). The concept of quarantining data could also be introduced so that bad data can be removed from the cleansed layer and then written to a separate table for investigation.