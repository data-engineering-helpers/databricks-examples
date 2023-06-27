Examples of code with DataBricks
================================

# Overview
That [Git repository](https://github.com/data-engineering-helpers/databricks-examples)
features use cases of good and bad practices when using Spark-based tools
to process and analyze data.

## References

### Spark
* [Apache Spark - Download Spark manually](https://spark.apache.org/docs/latest/api/python/getting_started/install.html#manually-downloading)

### Spark Connect
* [Apache Spark - Doc - Installation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
* [Apache Spark - Doc - Quick start Spark Connect](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_connect.html)

### Jupyter
* [BMC - Integrate PySpark with Jupyter](https://www.bmc.com/blogs/jupyter-notebooks-apache-spark/)

# Quick start
* Launch Spark Connect server:
```bash
$ sparkconnectstart
```

* Launch PySpark from the command-line, which in turn launches Jupyter Lab
  + Follow the details given by PySpark to open Jupyter in a web browser
```bash
$ pyspark
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
  [`jupyter-notebooks/simple-connect.ipynb`](https://github.com/data-engineering-helpers/databricks-examples/blob/main/jupyter-notebooks/simple-connect.ipynb)
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
    which has been started by Spark without making use of Spark Connect.
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


