from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os

import spacy
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize
import re
from autocorrect import spell
import numpy as np
from numpy import dot
from numpy.linalg import norm

def text_cleaner(text):
    rules = [
        {r'>\s+': u'>'},  # remove spaces after a tag opens or closes
        {r'\s+': u' '},  # replace consecutive spaces
        {r'\s*<br\s*/?>\s*': u'\n'},  # newline after a <br>
        {r'</(div)\s*>\s*': u'\n'},  # newline after </p> and </div> and <h1/>...
        {r'</(p|h\d)\s*>\s*': u'\n\n'},  # newline after </p> and </div> and <h1/>...
        {r'<head>.*<\s*(/head|body)[^>]*>': u''},  # remove <head> to </head>
        {r'<a\s+href="([^"]+)"[^>]*>.*</a>': r'\1'},  # show links instead of texts
        {r'[ \t]*<[^<]*?/?>': u''},  # remove remaining tags
        {r'^\s+': u''},  # remove spaces at the beginning
        {r'[\.\,\?\!]+': u''} # remove dot and comma

    ]
    for rule in rules:
        for (k, v) in rule.items():
            regex = re.compile(k)
            text = regex.sub(v, text)
    text = text.rstrip()
    return text.lower()


def doc_clean(doc):
    print(doc)
    # remove noise/ punctuations or special characters and to lower case
    doc = text_cleaner(doc)
    print(doc)

    # Spelling Correction
    doc = [spell[w] for w in doc]

    # remove stop words
    stop_words = set(stopwords.words('english'))
    word_tokens = word_tokenize(doc)
    filtered_doc = [w for w in word_tokens if not w in stop_words]
    print(filtered_doc)

    # stem word
    ps = PorterStemmer()
    stemmed_doc = [ps.stem(w) for w in filtered_doc]
    print(stemmed_doc)
    return stemmed_doc

# get the vector for a document word2vec
def docvec(doc):
    # print(type(doc))
    nlp = spacy.load("en_vectors_web_lg")
    vec = nlp(doc)
    return np.mean([w.vector for w in vec], axis=0)

def similarity(docvec1,docvec2):
    return cosine(docvec1, docvec2)

# cosine similarity
def cosine(v1, v2):
    if norm(v1) > 0 and norm(v2) > 0:
        return dot(v1, v2) / (norm(v1) * norm(v2))
    else:
        return 0.0

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
vectors = comments.map(lambda c: docvec(c)).collect()
for vec in vectors:
    print(vec)
    print("==")

# save it into database



spark.stop()
 # You could use ndarray.dumps() to pickle it to a string then write it to a BLOB field? Recover it using numpy.loads()
