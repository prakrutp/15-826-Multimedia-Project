#!/usr/bin/env python
import psycopg2
import sys
from constants import *
import csv
import pprint

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
    # sys.stdout.write("Established connection to the database\n")
    return conn, cur

def closeDB(cur, conn):
    try:
        cur.close()
        if conn is not None:
            conn.close()
        # sys.stdout.write("Closed connection to the database\n")
    except Exception as e:
        sys.exit("Closing connection to DB failed\n" + str(e))

def createInputTable(cur):
    try:
        inp_param = ''
        rdr = csv.reader(open(RAW_INPUT))
        line1 = rdr.next()
        dimension = len(line1) - 1
        for i in range(dimension + 1):
            tmp = 'Attr' + str(i) + ' VARCHAR, '
            inp_param += tmp
        sql_query = 'CREATE TABLE input_table(' + inp_param[:-2] + ')'
        cur.execute(sql_query)
        sql_query = "COPY input_table FROM '" + RAW_INPUT + "' DELIMITER AS '" + INPUT_DELIMITER + "' CSV"
        cur.execute(sql_query)
        sql_query = "UPDATE input_table SET Attr" + str(dimension) + " = " + "split_part(Attr" + str(dimension - 1) + ", '-', 2)";
        cur.execute(sql_query)
        sql_query = "UPDATE input_table SET Attr" + str(dimension - 1) + " = " + "split_part(Attr" + str(dimension - 1) + ", '-', 1)";
        cur.execute(sql_query)
        if BINARIZE == False and GRANULARITY == "Date":
            segment = "Select Attr0, Attr1, Attr2, count(*) From input_table group by Attr0, Attr1, Attr2"
        elif BINARIZE == True and GRANULARITY == "Date":
            segment = "Select Attr0, Attr1, Attr2," + str(1) + " From input_table"
        elif BINARIZE == True and GRANULARITY == "Time":
            segment = "Select Attr0, Attr1, Attr2, Attr3," + str(1) + " From input_table"
        else:
            return
        sql_query = "Copy (" + segment + ") To '"+ INPUT_CSV +"' With CSV DELIMITER ','";
        cur.execute(sql_query)
        # cur.execute("SELECT * FROM input_table LIMIT 10")
        # records = cur.fetchall()
        # pprint.pprint(records)
        sys.stdout.write("Input CSV now at " + INPUT_CSV + '\n')
        return 'input_table'
    except Exception as e:
        sys.exit("Input table creation failed\n" + str(e))

def dropTable(cur, tables):
    for table in tables:
        try:
            cur.execute("DROP TABLE IF EXISTS input_table")
            sys.stdout.write("Dropped " + table + "\n")
        except Exception as e:
            sys.exit("Dropping " + table + " failed\n" + str(e))

def main():
    conn, cur = connectDB()
    tables = []
    tables.append(createInputTable(cur))
    dropTable(cur, tables)
    closeDB(conn, cur)

if __name__ == "__main__":
    main()
