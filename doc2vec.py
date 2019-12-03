from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os

# Spark connect mysql
url = "jdbc:mysql://localhost:3306/moviereview"
doctb = "document"
costb = "similarity"
user = "root"
pwd = "123456"
driver = "com.mysql.jdbc.Driver"

SUBMIT_ARGS = "--jars mysql-connector-java-8.0.18.jar pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
conf = SparkConf()
context = SparkContext(conf=conf)
spark = SparkSession(context)

mysqldf = spark.read.format("jdbc").options(
    url=url,
    driver=driver,
    dbtable=doctb,
    user=user,
    password=pwd).load()



docdf = spark.read.jdbc()

spark.stop()
 # You could use ndarray.dumps() to pickle it to a string then write it to a BLOB field? Recover it using numpy.loads()
