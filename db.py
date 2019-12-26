import mysql.connector
from mysql.connector import errorcode
import numpy as np
import time

jdbcuser = "root"
jdbcpwd = ""
jdbcdriver = "com.mysql.jdbc.Driver"
jdbcHostname = ""
jdbcDatabase = ""
jdbcPort = 3306

jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

# Spark connect mysql
connectionProperties = {
  "user": jdbcuser,
  "password": jdbcpwd,
  "driver": jdbcdriver
}

# save it into database
def persist(vectors):
    conn = dbConnect()
    cursor = conn.cursor()
    sql = "UPDATE test set vec = %s where id = %s"

    print("start to persist")
    start = time.time()
    values = []
    for id, vec in vectors:
        values.append((vec.dumps(), id))
        # cursor.execute(sql, (vec.dumps(), id))
    cursor.executemany(sql, values)
    conn.commit()
    cursor.close()
    conn.close()

    end = time.time()
    print("finish persisting by ", (end - start))


def dbConnect():
    try:
        conn = mysql.connector.connect(
            host=jdbcHostname,
            user=jdbcuser,
            passwd=jdbcpwd,
            database=jdbcDatabase,
            charset='utf8',
            use_pure='True',
            auth_plugin='mysql_native_password')
        conn.set_charset_collation("utf8")
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


def getVec(id):
    # cursor = conn.cursor()
    print("GET VEC ", id)
    sql = "SELECT vec FROM reviews WHERE id = %s"
    cursor.execute(sql, (id,))
    res = cursor.fetchone()[0]
    return np.loads(res)
