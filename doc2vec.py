from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os
import core
from db import *

# set up
SUBMIT_ARGS = "--jars mysql-connector-java-8.0.18.jar pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"

conf = SparkConf()
context = SparkContext(conf=conf)
spark = SparkSession(context)

# Spark connect mysql
connectionProperties = {
  "user": jdbcuser,
  "password": jdbcpwd,
  "driver": jdbcdriver
}

docdf = spark.read.jdbc(url=jdbcUrl, table="document", properties=connectionProperties)
docdf.show(n=2)

# computer vector
# (id,full_text)
comments = docdf.rdd.map(lambda r: (r[0], r[1]))
vectors = comments.map(lambda c: (c[0], core.docvec(c[1]))).collect()
print("Finish collecting")
persist(vectors)
spark.stop()

