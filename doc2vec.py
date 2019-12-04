from mySpark import spark
import core
from db import *


def doc2vec():
  docdf = spark.read.jdbc(url=jdbcUrl, table="document", properties=connectionProperties)
  docdf.show(n=2)
  # computer vector
  # (id,full_text)
  comments = docdf.rdd.map(lambda r: (r[0], r[1]))
  vectors = comments.map(lambda c: (c[0], core.docvec(c[1]))).collect()
  persist(vectors)
  spark.stop()

# doc2vec()