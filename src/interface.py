#!/usr/bin/env python
import psycopg2
import sys
from constants import *

#### Function to connect to the database
def connectDB():
    try:
        conn = psycopg2.connect(dbname=DBNAME, user=USERNAME, host=PGHOST, port=PGPORT)
    except:
        sys.exit("Unable to connect to the database\n")

    cur = conn.cursor()
    sys.stdout.write("Established connection to the database\n")
    return conn, cur

#### Function to close the connection to database
def closeDB(cur, conn):
    try:
        cur.close()
        if conn is not None:
            conn.close()
        sys.stdout.write("Closed connection to the database\n")
    except:
        sys.exit("Closing connection to DB failed\n")

#### Function to read input csv and create table
def createInputTable(cur):
    try:
        inp_param = ''
        for i in range(int(NUM_ATTRIBUTES)):
            tmp = 'Attr' + str(i) + ' VARCHAR, '
            inp_param += tmp
        inp_param = 'CREATE TABLE input_table(' + inp_param + 'count INTEGER)'
        cur.execute(inp_param)
        sys.stdout.write("Input Table created\n")
        inp_param = "COPY input_table FROM '" + INPUT_CSV + "' DELIMITER AS '" + INPUT_DELIMITER + "' CSV"
        cur.execute(inp_param)
        sys.stdout.write("Data from CSV input to the input_table\n")
    except:
        sys.exit("Input table creation failed\n")

def dropTable(cur):
    try:
        cur.execute("DROP TABLE IF EXISTS input_table")
        sys.stdout.write("Dropped input_table\n")
    except:
        sys.exit("Dropping table failed\n")
