#!/usr/bin/env python
import psycopg2
import sys
from constants import *


def createAggragatedTable():
	


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
