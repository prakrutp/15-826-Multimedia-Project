#!/usr/bin/env python
import psycopg2
import sys
from constants import *

#### Function to connect to the database
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



# def createInputTable(cur):
#     try:
#         inp_param = ''
#         for i in range(int(NUM_ATTRIBUTES_TEMP)):
#             tmp = 'Attr' + str(i) + ' VARCHAR, '
#             inp_param += tmp
#         inp_param = inp_param[:-2]
#         inp_param = 'CREATE TABLE temp_table(' + inp_param + ')'
#         cur.execute(inp_param)
#         sys.stdout.write("Input Table - " + inp_param +  " created\n")
#         inp_param = "COPY temp_table FROM '" + INPUT_CSV + "' DELIMITER AS '" + INPUT_DELIMITER + "' CSV"
#         cur.execute(inp_param)
#         sys.stdout.write("Data from CSV input to the input_table\n")
#         sql_query = "create table input_table as select Attr0, Attr1, Attr2, count(*) as count from temp_table group by Attr0, Attr1, Attr2"
#         cur.execute(sql_query)

#     except Exception as e:
#         sys.exit("Input table creation failed\n" + str(e))


#### Function to read input csv and create table
def createInputTable(cur):
    try:
        inp_param = ''
        for i in range(int(NUM_ATTRIBUTES)):
            tmp = 'Attr' + str(i) + ' VARCHAR, '
            inp_param += tmp
        inp_param = 'CREATE TABLE input_table(' + inp_param + 'count INTEGER)'
        cur.execute(inp_param)
        sys.stdout.write("Input Table - " + inp_param +  " created\n")
        inp_param = "COPY input_table FROM '" + INPUT_CSV + "' DELIMITER AS '" + INPUT_DELIMITER + "' CSV"
        cur.execute(inp_param)
        sys.stdout.write("Data from CSV input to the input_table\n")
    except Exception as e:
        sys.exit("Input table creation failed\n" + str(e))

def createOutputTable(cur, output_table_name):
    try:
        sql_query = ''
        for i in range(int(NUM_ATTRIBUTES)):
            tmp = 'Attr' + str(i) + ' VARCHAR, '
            sql_query += tmp
        sql_query = 'CREATE TABLE ' + output_table_name + '(blockId INTEGER, ' + sql_query + 'count INTEGER)'
        cur.execute(sql_query)
        sys.stdout.write("Output Table - " + output_table_name +  " created\n")
    except Exception as e:
        sys.exit("Output table creation failed\n" + str(e))

def dropTable(cur):
    try:
        cur.execute("DROP TABLE IF EXISTS input_table")
        sys.stdout.write("Dropped input_table\n")
    except:
        sys.exit("Dropping table failed\n")
