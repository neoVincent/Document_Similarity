from db import *
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os
import core

# Given an docId, find the top k similar document in the database

# set up Spark
SUBMIT_ARGS = "--jars mysql-connector-java-8.0.18.jar pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"

# DEBUG
docId = 1
K = 1

# Connect to db get the document full text by id
vec = getVec(docId)

# Spark connect mysql
connectionProperties = {
  "user": jdbcuser,
  "password": jdbcpwd,
  "driver": jdbcdriver
}

conf = SparkConf()
context = SparkContext(conf=conf)
spark = SparkSession(context)

docdf = spark.read.jdbc(url=jdbcUrl, table="document", properties=connectionProperties)

# compute cosine
cosines = docdf.rdd.map(lambda d: (core.cosine(vec.tolist(), getVec(d[0]).tolist()), d[0]))
topK = cosines.sortByKey(ascending=False, numPartitions=2).collect()

# output result
for i in range(1, K+1):
    print("cosine: %f, docId: %d" % (topK[i][0], topK[i][1]))

