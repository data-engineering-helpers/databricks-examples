Examples of code with DataBricks
================================

# Table of Content (ToC)
* [Examples of code with DataBricks](#examples-of-code-with-databricks)
* [Overview](#overview)
  * [References](#references)
    * [Spark](#spark)
    * [Spark Connect](#spark-connect)
    * [Jupyter](#jupyter)
* [Quick start](#quick-start)
* [Use cases](#use-cases)
* [Initial setup](#initial-setup)
  * [PySpark and Jupyter](#pyspark-and-jupyter)
    * [Install native Spark manually](#install-native-spark-manually)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)

# Overview
This
[Git repository](https://github.com/data-engineering-helpers/databricks-examples)
features use cases of good and bad practices when using Spark-based tools
to process and analyze data.

## References

### Spark
* [Apache Spark - Download Spark manually](https://spark.apache.org/docs/latest/api/python/getting_started/install.html#manually-downloading)
* [Apache Spark - Doc - Getting started / Installation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)

### Spark Connect
* [Apache Spark - Doc - Spark Connect - Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
* [Apache Spark - Doc - Spark Connect - Quick start](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_connect.html)

### Python

#### Python for geo-related analysis
* [Data Engineering Helpers - Knowledge Sharing - Geo-related Python analysis](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/programming/python/geo/README.md)

### Jupyter
* [BMC - Integrate PySpark with Jupyter](https://www.bmc.com/blogs/jupyter-notebooks-apache-spark/)

# Quick start
* From a dedicated terminal window/tab, launch Spark Connect server.
  Note that the `SPARK_REMOTE` environment variable should not be set at this
  stage, otherwise the Spark Connect server will try to connect to the
  corresponding Spark Connect server and will therefore not start
```bash
$ sparkconnectstart
```

* From the current terminal/tab, different from the window/tab having launched
  the Spark Connect server, launch PySpark from the command-line, which in
  turn launches Jupyter Lab
  + Follow the details given by PySpark to open Jupyter in a web browser
```bash
$ export SPARK_REMOTE="sc://localhost:15002"; pyspark
...
[C 2023-06-27 21:54:04.720 ServerApp] 
    
    To access the server, open this file in a browser:
        file://$HOME/Library/Jupyter/runtime/jpserver-21219-open.html
    Or copy and paste one of these URLs:
        http://localhost:8889/lab?token=dd69151c26a3b91fabda4b2b7e9724d13b49561f2c00908d
        http://127.0.0.1:8889/lab?token=dd69151c26a3b91fabda4b2b7e9724d13b49561f2c00908d
...
```
  + Open Jupyter in a web browser. For instance, on MacOS:
```bash
$ open ~/Library/Jupyter/runtime/jpserver-*-open.html
```

* Open a notebook, for instance
  [`ipython-notebooks/simple-connect.ipynb`](https://github.com/data-engineering-helpers/databricks-examples/blob/main/ipython-notebooks/simple-connect.ipynb)
  + Run the cells. The third cell should give a result like:
```txt
+-------+--------+-------+-------+
|User ID|Username|Browser|     OS|
+-------+--------+-------+-------+
|   1580|   Barry|FireFox|Windows|
|   5820|     Sam|MS Edge|  Linux|
|   2340|   Harry|Vivaldi|Windows|
|   7860|  Albert| Chrome|Windows|
|   1123|     May| Safari|  macOS|
+-------+--------+-------+-------+
```

* Notes:
  + The first cell stops the initial Spark session,
    when that latter has been started by Spark without making use of
	Spark Connect, for instance when the `SPARK_REMOTE` environment
	variable has not been set properly.
    There is a try-catch clause, as once the Spark session has been
    started through Spark Connect, it cannot be stopped that way;
    the first cell may thus be re-executed at will with no further
    side-effect on the Spark session
  + The same first cell then starts, or uses when already existing,
    the Spark session through Spark Connect

# Use cases
* [Learnt lessons with Spark and Delta](https://github.com/data-engineering-helpers/databricks-examples/blob/main/notebooks/Learnt%20lessons%20with%20Spark%20and%20Delta.py)

# Initial setup

## PySpark and Jupyter
* As per the official
  [Apache Spark documentation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html),
  PyPi-installed PySpark (`pip install pyspark[connect]`) comes with
  Spark Connect from Spark version 3.4 or later. However, as of Spark
  version up to 3.4.1, the PySpark installation lacks the two new
  administration scripts allowing to start and to stop the Spark Connect
  server. For convenience, these two scripts have therefore been copied into
  this Git repository, in the [`tools/` directory](tools/). They may then simply
  copied in the PySpark `sbin` directory, once PySpark has been installed
  with `pip`

* Install PySpark and JupyterLab, along with a few other Python libraries,
  from PyPi:
```bash
$ pip install -U pyspark[connect,sql,pandas_on_spark] plotly pyvis jupyterlab
```

* Add the following in the Bash/Zsh init script:
```bash
$ cat >> ~/.bashrc << _EOF

# Spark
PY_LIBDIR="$(python -mpip show pyspark|grep "^Location:"|cut -d' ' -f2,2)"
export SPARK_VERSION="\$(python -mpip show pyspark|grep "^Version:"|cut -d' ' -f2,2)"
export SPARK_HOME="\$PY_LIBDIR/pyspark"
export PATH="\$SPARK_HOME/sbin:\$PATH"
export PYSPARK_PYTHON="\$(which python3)"
export PYSPARK_DRIVER_PYTHON='jupyter'
export PYSPARK_DRIVER_PYTHON_OPTS='lab --no-browser --port=8889'

_EOF
```

* Re-read the Shell init scripts:
```
$ exec bash
```

* Copy the two Spark connect administrative scripts into the PySpark
  installation:
```bash
$ cp tools/st*-connect*.sh $SPARK_HOME/sbin/
```

* Check that the scripts are installed correctly:
```bash
$ ls -lFh $SPARK_HOME/sbin/*connect*.sh
-rwxr-xr-x  1 user  staff   1.5K Jun 28 16:54 $PY_LIBDIR/pyspark/sbin/start-connect-server.sh*
-rwxr-xr-x  1 user  staff   1.0K Jun 28 16:54 $PY_LIBDIR/pyspark/sbin/stop-connect-server.sh*
```

* Add the following Shell aliases to start and stop Spark Connect server:
```bash
$ cat >> ~/.bash_aliases << _EOF

# Spark Connect
alias sparkconnectstart=unset SPARK_REMOTE; start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:\$SPARK_VERSION,io.delta:delta-core_2.12:2.4.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"'
alias sparkconnectstop='stop-connect-server.sh'
# PySpark
alias pysparkdelta='pyspark --packages io.delta:delta-core_2.12:2.4.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"'

_EOF
```

* Re-read the Shell aliases:
```bash
. ~/.bash_aliases
```

### Install native Spark manually
* That section is kept for reference only. It is normally not needed

* Install Spark/PySpark manually, _e.g._ with Spark 3.4.1:
```bash
$ export SPARK_VERSION="3.4.1"
  wget https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz
  tar zxf spark-$SPARK_VERSION-bin-hadoop3.tgz && \
  mv spark-$SPARK_VERSION-bin-hadoop3 ~/ && \
  rm -f spark-$SPARK_VERSION-bin-hadoop3.tgz
```

* Add the following in the Bash/Zsh init script:
```bash
$ cat >> ~/.bashrc << _EOF

# Spark
export SPARK_VERSION="${SPARK_VERSION}"
export SPARK_HOME="\$HOME/spark-\$SPARK_VERSION-bin-hadoop3"
export PATH="\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\${PATH}"
export PYTHONPATH=\$(ZIPS=("\$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "\${ZIPS[*]}"):\$PYTHONPATH
export PYSPARK_PYTHON="\$(which python3)"
export PYSPARK_DRIVER_PYTHON='jupyter'
export PYSPARK_DRIVER_PYTHON_OPTS='lab --no-browser --port=8889'

_EOF
exec bash
```

* Add the following Shell aliases to start and stop Spark Connect server:
```bash
$ cat >> ~/.bash_aliases << _EOF

# Spark Connect
alias sparkconnectstart='start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:${SPARK_VERSION}'
alias sparkconnectstop='stop-connect-server.sh'

_EOF
. ~/.bash_aliases
```


