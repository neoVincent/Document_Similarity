# Document Similarity

## Work flow
- create two tables
    - document (id,full_text,vec)
    - similarity (from_id, to_id, cosine)
- load movie comment into db
- compute document vector for each comment
    - Spark Parallel
        - map and collect: compute NLP word2vec for each doc element(details in ``NLP task``)
    - persis the value in database for cosine similarity computation
        - dumps the vector list int string 
        - stored as blob type in database
- find top k similarity documents in db given a docid by user
    - Spark Parallel
        - map: compute cosine similarity, map into a key value pair (cosine, docid)
        - collect: sortedbykey
    - return the top k docid in the list

## NLP tasks
- document cleaning
    - remove spaces and invalid tags
    - remove punctuations
    - spelling correction
    - remove stop words 
    - stemming
- document vector representation
    - for each word do word2vec
    - use the mean of these vectors as the document vector
- similarity
    - cosine value to measure the similarity 

> note: 
> - the trained ``Glove`` model is used as vector space
> - NLP related functions are located in ``core.py``

### SpaCy
NLP packages
#### Models & Languages
load model
```python
python3 -m spacy download en_core_web_sm 
```
Get model info
```python
print(spacy.info("en_core_web_sd"))
```
> en_core_web_sm: doesn't come with a word vectors table
> en_core_web_md: has a relatively small number of vectors(between 10k and 20k)

## Spark 
### Set up
- install required package
    - Java 8, Python 2.7+/3.4+ and R 3.1+. 
    - For the Scala API, Spark 2.4.4 uses Scala 2.12. You will need to use a compatible Scala version (2.12.x)
- [Set environment variables](https://stackoverflow.com/questions/48260412/environment-variables-pyspark-python-and-pyspark-driver-python)
    - JAVA_HOME : java 8
    - SPARK_HOME
    - PYSPARK_PYTHON: ```/usr/local/bin/python3```
    - PYSPARK_DRIVER_PYTHON:  ```/usr/local/bin/python3```


## Trouble shooting

### Spark unable to load the conf 
maybe set the SPARK_HOME='/usr/local/bin/***/libexec
https://spark.apache.org/docs/latest/configuration.html#environment-variables

### Spark use different python version
Set the following environment variables in .bash_profile
- PYSPARK_DRIVER_PYTHON
- PYSPARK_PYTHON

> note: 
> - use zsh instead of bash
>   - add source ~/.bash_profile in .zsh_rc


### load-spark-env.sh: Permission denied
set the SPARK_HOME='/usr/local/bin/***/libexec
[stackoverflow](https://stackoverflow.com/questions/34624821/apache-spark-upgrade-from-1-5-2-to-1-6-0-using-homebrew-leading-to-permission-de)

### objc_initializeAfterForkError
This is a multi-thread issue for python. set the following environment variables for workaround 
```export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES```
[stackoverflow](https://github.com/rtomayko/shotgun/issues/69)


