from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os

# set up Spark
SUBMIT_ARGS = "--jars mysql-connector-java-8.0.18.jar pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"

conf = SparkConf()
context = SparkContext(conf=conf)

spark = SparkSession(context)
