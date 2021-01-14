import time

import simplejson
import psycopg2


from PSPSProject.src.Tests.conftest import config

global conn

# <Method>: database connection
# <input Param> : database.ini file
# returns : connection details
def connecttodb():
    global conn
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        print('connection done')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn


def closeconn():
    global conn
    if conn is not None:
        conn.close()
        print('Database connection closed')


# <Method>: get single value metadata information
# <input Param> : versioname, database.ini file
# returns : metadata info
def queryresults_get_one(query):
    result = 0
    global conn
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        print('DB connection successful')
        cur = conn.cursor()
        cur.execute(query)
        qresult = cur.fetchone()
        result = qresult
        print(qresult)
        print("Query ran successfully")
        cur.close()
    except (Exception, psycopg2.DataError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed')
    return result


# <Method>: get single row
# <input Param> : database.ini file
# returns : query result
def queryresults_fetchone(query):
    result = 0
    global conn
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        print('DB connection successful')
        cur = conn.cursor()
        cur.execute(query)
        qresult = cur.fetchone()
        result = simplejson.dumps(qresult[0])
        result = result.replace('"', '')
        print(result)
        print("Query ran successfully")
        cur.close()
    except (Exception, psycopg2.DataError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed')
    return result


# <Method>: get all value  information
# <input Param> : database.ini file
# returns : CCB info
def queryresults_get_alldata(query):
    result = 0
    global conn
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        print('DB connection successful')
        cur = conn.cursor()
        cur.execute(query)
        qresult = cur.fetchall()
        result = qresult
        print(qresult)
        print("Query ran successfully")
        cur.close()
    except (Exception, psycopg2.DataError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed')
    return result


# <Method>:
# <input Param> :
# returns :
def queryresults_get_data(query):
    result = 0
    global conn
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        print('DB connection successful')
        cur = conn.cursor()
        while True:
            cur.execute(query)
            qresult = cur.fetchone()
            result = qresult[0]
            if result == "Completed":
                break
            elif result == "Failed":
                break
            elif result == "In Queue" or "In Progress":
                time.sleep(2)
        print("Query ran successfully")
        cur.close()
    except (Exception, psycopg2.DataError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed')
    return result
