from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os
import core

# set up
SUBMIT_ARGS = "--jars mysql-connector-java-8.0.18.jar pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"

conf = SparkConf()
context = SparkContext(conf=conf)
spark = SparkSession(context)

# Spark connect mysql
user = "root"
pwd = "123456"
driver = "com.mysql.jdbc.Driver"
jdbcHostname = "localhost"
jdbcDatabase = "moviereview"
jdbcPort = 3306

jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : user,
  "password" : pwd,
  "driver" : driver
}

docdf = spark.read.jdbc(url=jdbcUrl, table="document", properties=connectionProperties)
docdf.show(n=2)

# computer vector
comments = docdf.rdd.map(lambda r: r[1])
print("===")
vectors = comments.map(lambda c: core.docvec(c)).collect()
for vec in vectors:
    print(vec)
    print("==")

# save it into database



spark.stop()
 # You could use ndarray.dumps() to pickle it to a string then write it to a BLOB field? Recover it using numpy.loads()
