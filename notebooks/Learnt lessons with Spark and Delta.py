# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC That series of notebooks showcases a few lessons learnt while using Spark
# MAGIC (and [Delta Lake](https://delta.io)) to process data. DataBricks is used here for convenience.
# MAGIC However, any other notebook and/or compute engine may be used the same way,
# MAGIC as the concepts presented here are based only on the open source version
# MAGIC of both Spark and Delta Lake.
# MAGIC
# MAGIC ## References
# MAGIC * [GitHub - DataBricks examples](https://github.com/data-engineering-helpers/databricks-examples) (Git repository hosting this notebook)
# MAGIC * [Delta Lake homepage](https://delta.io)
# MAGIC * [Geonames - Data dump folder](http://download.geonames.org/export/dump/)

# COMMAND ----------

import pathlib

# Geonames data
geo_hierarchy_remote_basepath: str = "https://github.com/data-engineering-helpers/databricks-examples/raw/main/data/geonames"
geo_hierarchy_local_basepath_wo_dbfs: str = "/tmp/geonames"
geo_hierarchy_local_basepath: str = f"/dbfs{geo_hierarchy_local_basepath_wo_dbfs}"
geo_hierarchy_prefix: str = "geonames-hierarchy"

# Local temporary folder
local_dir: pathlib.Path = pathlib.Path(geo_hierarchy_local_basepath)

# Max level of hierarchy
max_level: int = 12

# Organization table
geo_hierarchy_table_ref: str = "database.geo_hierarchy" # <--- Replace here with your own Hive metastore table ID

# List of extract dates from CSV extract
g_date_list: list[str] = ["20230602",]
g_latest_date: str = "2023-06-02"

# List of extract dates from geo hierarchy table
g_date_list_from_table: list[str] = None
g_latest_date_from_table: str = None

# Diff list: extract dates existing as CSV data files but not yet in the geo hierarchy table
g_date_list_diff: list[str] = None


# COMMAND ----------

geo_hierarchy_ddl_drop = """
drop table if exists preprod_dps.geo_hierarchy;
"""

geo_hierarchy_ddl_create = """
create or replace table preprod_dps.geo_hierarchy (
 extract_date date,
 parent integer,
 child integer,
 type string
)
using delta
tblproperties (delta.enableChangeDataFeed=true)
"""


# COMMAND ----------

import urllib
import shutil

def downloadCSVFiles(debug: bool = False) -> None:
    # Remove any previous downloaded data
    shutil.rmtree(path=local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    
    # Download the CSV data files to the local /tmp folder
    for extract_date in g_date_list:
        remote_filename: str = f"{geo_hierarchy_prefix}-{extract_date}.csv.bz2"
        remote_file: str = f"{geo_hierarchy_remote_basepath}/{remote_filename}"
        local_file: str = f"{geo_hierarchy_local_basepath}/{remote_filename}"
        urllib.request.urlretrieve(remote_file, local_file)
        
    # Check the downloaded files
    if debug:
        local_file_list = [fp for fp in local_dir.iterdir()]    
        print(local_file_list)


# COMMAND ----------

downloadCSVFiles(debug=True)

# COMMAND ----------

import pyspark.sql.dataframe
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Raw schema (without extract date)
geoHierarchySchema = (
    StructType()
    .add(StructField("parent", IntegerType(), True))
    .add(StructField("child", IntegerType(), True))
    .add(StructField("type", StringType(), True))
)

def get_list_from_df(
                    key: str,
                    input_df: pyspark.sql.dataframe.DataFrame,
                    debug: bool=False
                    ) -> list[str]:
    row_list = input_df.select(key).collect()
    raw_list: list[str] = [row.uid for row in row_list]
    return raw_list

def elt(
        extract_date: str,
        local_path: str,
        debug: bool=False
        ) -> pyspark.sql.dataframe.DataFrame:
    geo_adm = spark.read.option("sep", "\t").option("header", "false").schema(geoHierarchySchema).csv(local_path)

    # Add the extraction date
    geo_adm = geo_adm.withColumn("extract_date", lit(extract_date)).withColumn("extract_date", to_date("extract_date", "yyyy-MM-dd"))

    return geo_adm

def elt_all(
            date_list: list[str],
            debug: bool=False
            ) -> dict[str, pyspark.sql.dataframe.DataFrame]:
    # Initialize the global list of geo hierarchy DataFrames
    geo_adm_list: dict[str, pyspark.sql.dataframe.DataFrame] = dict()

    # Browse every data file and load a DataFrame with it
    for extract_date in date_list:        
        extract_date_compact: str = extract_date.replace("-", "")
        local_path: str = f"{geo_hierarchy_local_basepath_wo_dbfs}/{geo_hierarchy_prefix}-{extract_date_compact}.csv.bz2"
        geo_adm = elt(extract_date=extract_date, local_path=local_path, debug=debug)
        if debug: print(f"Extract date: {extract_date} - Nb of records: {geo_adm.count()}")
        geo_adm_list[extract_date] = geo_adm

    return geo_adm_list

def save_as_table(
                  input_geo_list: dict[str, pyspark.sql.dataframe.DataFrame],
                  date_list: list[str],
                  debug: bool=False
                  ) -> None:
    # Debug
    if debug: print(f"Saving the DataFrames, parsed from CSV data files, for the following ({len(date_list)}) extract dates: {date_list}")

    # Save every extract as a version in the geo hierarchy Delta table,
    # and store the mapping in the dedicated table
    for extract_date in sorted(date_list):
        # As, in the widget, the empty string is required (in case there is no extract date in the diff),
        # it must be filtered out here
        if extract_date == "":
            continue

        geo_adm = input_geo_list.get(extract_date, None)
        if not geo_adm:
            print(f"Error - For some reason, there is no DataFrame, parsed from CSV data file, for the {extract_date} extract date. List of extract dates for the DataFrames: {input_geo_list.keys()}")
            continue

        if debug: print(f"Saving {extract_date} DataFrame as {geo_hierarchy_table_ref} table...")
        geo_adm.write.format("delta").mode("append").saveAsTable(geo_hierarchy_table_ref)
        if debug: print(f"Saved {extract_date} DataFrame as {geo_hierarchy_table_ref} table...")
    #
    return


# COMMAND ----------

import re, datetime
from pyspark.sql.functions import *

def derive_diff(
                date_list_from_table: list[str],
                date_list: list[str],
                debug=False
                ) -> (str, list[str]):
    diff_list = list(set(date_list_from_table).symmetric_difference(set(date_list)))
    diff_list = sorted(diff_list, reverse=True)
    # Extract the latest date (first element of the reverse-sorted list)
    latest_date = diff_list[0] if diff_list else None

    # Debug
    if debug:
        print(f"Differences in both lists ({len(diff_list)} records): {diff_list}")
        if latest_date:
            print(f"  => latest extract date from diff list: {latest_date}")

    return latest_date, diff_list

def list_from_main_table(
              debug=False
              ) -> (str, list[str]):
    # Build a DataFrame from the main table
    geo_all = spark.read.table(geo_hierarchy_table_ref)

    # List all the extract dates and build a pure Python list
    extract_date_df_list_from_table = geo_all.select(col("extract_date")).distinct().collect()
    date_list = [extract_date_row.extract_date.strftime("%Y-%m-%d") for extract_date_row in extract_date_df_list_from_table]

    # Sort the dates, just in case the DataFrame would not do it already
    date_list = sorted(date_list, reverse=True)
    # Extract the latest date (first element of the reverse-sorted list)
    latest_date = date_list[0] if date_list else None

    # Debug
    if debug:
        print(f"List of extract dates from the geo hierarchy table ({len(date_list)} records): {date_list}")
        if latest_date:
            print(f"  => latest extract date from geo hierarchy table: {latest_date}")

    return latest_date, date_list

def list_from_csv_extracts(
                           debug=False
                           ) -> (str, list[str]):
    """Derive the dates for which there are CSV extract files"""
    date_list: list[str] = []

    for csv_extract in local_dir.glob(f"{geo_hierarchy_prefix}-*.csv.bz2"):
        csv_filename: str = csv_extract.name
        m = re.match(r"geonames-hierarchy-(\d+).csv.bz2", csv_filename)
        extract_date_str: str = m.group(1)
        extract_date: datetime.date = datetime.datetime.strptime(extract_date_str, "%Y%m%d").date().strftime("%Y-%m-%d")
        date_list.append(extract_date)

    # Sort the dates, just in case Cloudpathlib or AWS S3 would not do it already
    date_list = sorted(date_list, reverse=True)
    # Extract the latest date (first element of the reverse-sorted list)
    latest_date = date_list[0] if date_list else None

    # Debug
    if debug:
        print(f"List of extract dates from the '{local_dir}/' directory ({len(date_list)} records): {date_list}")

        if latest_date:
            print(f"  => latest extract date from CSV data files: {latest_date}")

    return latest_date, date_list


# COMMAND ----------

# Extracts from the main table
g_latest_date_from_table, g_date_list_from_table = list_from_main_table(debug=True)

# Extracts from the CSV data files on the S3 folder
g_latest_date, g_date_list = list_from_csv_extracts(debug=True)

# Difference between both
g_latest_date_diff, g_date_list_diff = derive_diff(date_list_from_table=g_date_list_from_table, date_list=g_date_list, debug=True)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

#
dbutils.widgets.text("latest_extract", g_latest_date)

#
g_date_list_from_table_for_widget = g_date_list_from_table.copy()
if len(g_date_list_from_table_for_widget)==0:
    g_latest_date_from_table = ""
    g_date_list_from_table_for_widget.append(g_latest_date_from_table)
dbutils.widgets.dropdown("extracts", g_latest_date_from_table, g_date_list_from_table_for_widget)

#
dbutils.widgets.multiselect("csv_extracts", g_latest_date, g_date_list)
g_date_list_diff_for_widget = g_date_list_diff.copy()
if len(g_date_list_diff_for_widget)==0:
    g_latest_date_diff = ""
    g_date_list_diff_for_widget.append(g_latest_date_diff)
else:
    g_latest_date_diff = g_date_list_diff_for_widget[0]
dbutils.widgets.multiselect("new_extracts", g_latest_date_diff, g_date_list_diff_for_widget)


# COMMAND ----------

geo_adm_list = elt_all(date_list=g_date_list, debug=False)
print(f"Number of extracts: {len(geo_adm_list)}")

# COMMAND ----------

latest_extract_date = getArgument("latest_extract")
print(f"Pick up the extract for {latest_extract_date}")
geo_adm_last = geo_adm_list.get(latest_extract_date)
display(geo_adm_last)

# COMMAND ----------

# Delete the table
spark.sql(geo_hierarchy_ddl_drop)

# (Re-)create the table
spark.sql(geo_hierarchy_ddl_create)

# COMMAND ----------

save_as_table(input_geo_list=geo_adm_list, date_list=g_date_list, debug=True)

# COMMAND ----------

geo_all = spark.read.table(geo_hierarchy_table_ref).filter(col("extract_date") == lit(latest_extract_date))
display(geo_all)

# COMMAND ----------


