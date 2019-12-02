from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys
from operator import add

spark = SparkSession \
    .builder \
    .appName("PythonWordCount") \
    .getOrCreate()

lines = spark.read.text("word.txt").rdd.map(lambda r: r[0])
counts = lines.flatMap(lambda x: x.split(' ')) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(add)
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

spark.stop()