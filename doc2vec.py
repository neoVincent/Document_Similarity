from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os
import core
import mysql.connector
from mysql.connector import errorcode
import json

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
# (id,full_text)
comments = docdf.rdd.map(lambda r: (r[0], r[1]))
vectors = comments.map(lambda c: (c[0], core.docvec(c[1]))).collect()

print("Finish collecting")

# save it into database


def dbConnect():
    try:
      conn = mysql.connector.connect(
        host=jdbcHostname,
        user=user,
        passwd=pwd,
        database=jdbcDatabase,
        auth_plugin='mysql_native_password')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        return None
    return conn


conn = dbConnect()
cursor = conn.cursor()
sql = "UPDATE document set vec = %s where id = %s"

for id, vec in vectors:
    cursor.execute(sql,(json.dumps(vec),id))

conn.commit()
cursor.close()
conn.close()

spark.stop()

 # You could use ndarray.dumps() to pickle it to a string then write it to a BLOB field? Recover it using numpy.loads()
