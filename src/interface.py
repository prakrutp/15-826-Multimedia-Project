#!/usr/bin/env python
import psycopg2
import sys
from constants import *

def connectDB():
    try:
        sys.stdout.write("Connecting to " + DBNAME +
                          " DB on Port - " + str(PGPORT) +
                          " Host - " + PGHOST +
                          " for User " + USERNAME + "\n")
        # conn = psycopg2.connect(dbname=DBNAME, user=USERNAME, host=PGHOST, port=PGPORT)
        conn = psycopg2.connect(dbname=DBNAME, user=USERNAME)
    except Exception as e:
        sys.exit("Unable to connect to the database\n" + str(e))

    sys.stdout.write("Established connection to the database\n")
    return conn

def closeDB(conn):
    try:
        if conn is not None:
            conn.close()
        sys.stdout.write("Closed connection to the database\n")
    except Exception as e:
        sys.exit("Closing connection to DB failed\n" + str(e))

#### Function to read input csv and create table
# Example:
#  001.002.003.004 , 172.016.112.050 , 06/03/1998 , 15
#  001.002.003.004 , 172.016.114.050 , 06/04/1998 , 450
#  017.139.040.001 , 172.016.114.050 , 06/03/1998 , 1
def createInputTable(conn):
    try:
        cur = conn.cursor()
        inp_param = ''
        for i in range(int(NUM_ATTRIBUTES)):
            tmp = 'Attr' + str(i) + ' VARCHAR, '
            inp_param += tmp
        inp_param = 'CREATE TABLE input_table(' + inp_param + 'count INTEGER)'
        cur.execute(inp_param)
        inp_param = "COPY input_table FROM '" + INPUT_CSV + "' DELIMITER AS '" + INPUT_DELIMITER + "' CSV"
        cur.execute(inp_param)
        sys.stdout.write("Data inserted from input CSV to input_table\n")
    except Exception as e:
        sys.exit("Input table creation failed\n" + str(e))


def createOutputTable(conn):
    try:
        cur = conn.cursor()
        sql_query = ''
        for i in range(int(NUM_ATTRIBUTES)):
            tmp = 'Attr' + str(i) + ' VARCHAR, '
            sql_query += tmp
        sql_query = 'CREATE TABLE ' + OUTPUT_TABLE_NAME + '(blockId INTEGER, ' + sql_query + 'count INTEGER)'
        cur.execute(sql_query)
        sys.stdout.write("Output Table - " + OUTPUT_TABLE_NAME +  " created\n")
    except Exception as e:
        sys.exit("Output table creation failed\n" + str(e))

def dropTable(conn, tables):
    cur = conn.cursor()
    for table in tables:
        try:
            cur.execute("DROP TABLE IF EXISTS input_table")
            # sys.stdout.write("Dropped " + str(table) + "\n")
        except Exception as e:
            sys.exit("Dropping " + str(table) + " failed\n" + str(e))
