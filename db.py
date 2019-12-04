import mysql.connector
from mysql.connector import errorcode

jdbcuser = "root"
jdbcpwd = "123456"
jdbcdriver = "com.mysql.jdbc.Driver"
jdbcHostname = "localhost"
jdbcDatabase = "moviereview"
jdbcPort = 3306

jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)


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