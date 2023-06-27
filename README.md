Examples of code with DataBricks
================================

# Overview
That [Git repository](https://github.com/data-engineering-helpers/databricks-examples)
features use cases of good and bad practices when using Spark-based tools
to process and analyze data.

## References

### Spark Connect
* [Apache Spark - Doc - Installation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
* [Apache Spark - Doc - Quick start Spark Connect](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_connect.html)

### Jupyter
* [BMC - Integrate PySpark with Jupyter](https://www.bmc.com/blogs/jupyter-notebooks-apache-spark/)

# Use cases
* [Learnt lessons with Spark and Delta](https://github.com/data-engineering-helpers/databricks-examples/blob/main/notebooks/Learnt%20lessons%20with%20Spark%20and%20Delta.py)

# Initial setup

## PySpark and Jupyter

* Add the following in the Bash/Zsh init script:
```bash
$ cat >> ~/.bashrc << _EOF

# Spark
export SPARK_VERSION="3.4.1"
export SPARK_HOME="\${HOME}/spark-\${SPARK_VERSION}-bin-hadoop3"
export PATH="\${SPARK_HOME}/bin:\${SPARK_HOME}/sbin:\${PATH}"
export PYTHONPATH=\$(ZIPS=("\$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "\${ZIPS[*]}"):\$PYTHONPATH
export PYSPARK_PYTHON="\$(which python3)"
export PYSPARK_DRIVER_PYTHON='jupyter'
export PYSPARK_DRIVER_PYTHON_OPTS='lab --no-browser --port=8889'

_EOF
```

