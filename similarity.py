from db import *
from pyspark.sql import SQLContext
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

def tonparray(barray):
    i = np.arange(300 * 1).reshape(300, 1)
    arr = np.frombuffer(barray, count=300, dtype=np.float32)
    print(arr)
    print(arr.shape)
    return arr

# DEBUG
docId = 1
K = 2

# Connect to db get the document full text by id
vec = getVec(docId)
print(vec.shape)
print(type(vec[0]))

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
docdf.show(n=2)

# compute cosine
cosines = docdf.rdd.map(lambda d: (core.cosine(vec.tolist(), tonparray(d[2]).tolist()), d[0]))
topK = cosines.sortByKey(ascending=False, numPartitions=2).collect()
for c,id in topK:
    print(c)
    print(id)

# return the document id or text
