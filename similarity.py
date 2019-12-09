from db import *
from mySpark import spark
import core
import time
# Given an docId, find the top k similar document in the database

def topKSimilarity(docId=1 ,K=1):
    # Connect to db get the document full text by id
    vec = getVec(docId)
    docdf = spark.read.jdbc(url=jdbcUrl, table="reviews", properties=connectionProperties)

    start = time.time()
    # compute cosine
    # TODO: find a way to decode blob into nparray correctly
    cosines = docdf.rdd.map(lambda d: (core.cosine(vec.tolist(), getVec(d[0]).tolist()), d[0]))
    topK = cosines.sortByKey(ascending=False, numPartitions=1).collect()

    end = time.time()
    print("Total time for cosine: ", (end - start))
    print(type(topK))
    print(type(topK[0]))
    res = []
    # output result
    for i in range(1, min((K + 1), len(topK))):
        print("cosine: %f, docId: %d" % (topK[i][0], topK[i][1]))
        res.append((topK[i][0], topK[i][1]))

    spark.stop()
    return res

topKSimilarity()