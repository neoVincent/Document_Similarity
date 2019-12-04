import mysql.connector
from mysql.connector import errorcode
import numpy as np

jdbcuser = "root"
jdbcpwd = "123456"
jdbcdriver = "com.mysql.jdbc.Driver"
jdbcHostname = "localhost"
jdbcDatabase = "moviereview"
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
    sql = "UPDATE document set vec = %s where id = %s"

    for id, vec in vectors:
        cursor.execute(sql, (vec.dumps(), id))

    conn.commit()
    cursor.close()
    conn.close()


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

def getVec(id):
    conn = dbConnect()
    cursor = conn.cursor()
    sql = "SELECT vec FROM document WHERE id = %s"
    cursor.execute(sql, (id,))
    res = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return np.loads(res)
