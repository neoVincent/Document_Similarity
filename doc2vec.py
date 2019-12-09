from mySpark import spark
import core
from db import *
import time


def doc2vec():
    docdf = spark.read.jdbc(url=jdbcUrl, table="test", properties=connectionProperties)
    docdf.show(n=2)
    start = time.time()
    comments = docdf.rdd.map(lambda r: (r[0], r[1]))
    vectors = comments.map(lambda c: (c[0], core.docvec(c[0], c[1]))).collect()
    end = time.time()
    print("Total time for doc2vec: ", (end - start))
    persist(vectors)
    end = time.time()
    print("Total time for persist: ", (end - start))
    spark.stop()


doc2vec()
