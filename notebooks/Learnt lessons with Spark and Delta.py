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

# Geonames data
geo_hierarchy_remote_basepath: str = "https://github.com/data-engineering-helpers/databricks-examples/raw/main/data/geonames"
geo_hierarchy_local_basepath: str = "/dbfs/tmp/geonames"
geo_hierarchy_prefix: str = "geonames-hierarchy"
extract_list: list[str] = ["2023-06-02",]

# COMMAND ----------

import urllib
import pathlib

# Local temporary folder
local_dir = pathlib.Path(geo_hierarchy_local_basepath)

# Download the CSV data files to the local /tmp folder
for extract_date in extract_list:
    remote_filename: str = f"{geo_hierarchy_prefix}-{extract_date}.csv.bz2"
    remote_file: str = f"{geo_hierarchy_remote_basepath}/{remote_filename}"
    local_file: str = f"{geo_hierarchy_local_basepath}/{remote_filename}"
    urllib.request.urlretrieve(remote_file, local_file)

# Check the downloaded files
local_file_list = [fp for fp in local_dir.iterdir()]
local_file_list

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType 

geoHierarchySchema = (
    StructType()
    .add(StructField("parent", IntegerType(), True))
    .add(StructField("child", IntegerType(), True))
    .add(StructField("type", StringType(), True))
)

# COMMAND ----------

geo_adm__20230602 = (
    spark.read
    .option("sep", "\t")
    .option("header", "false")
    .option("compression", "bzip2")
    .schema(geoHierarchySchema)
    .csv("/tmp/geonames/geonames-hierarchy-2023-06-02.csv.bz2")
)
display(geo_adm__20230602)

# COMMAND ----------


