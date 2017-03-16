import psycopg2
import sys
import interface
import pprint

def main():
    conn, cur = interface.connectDB()
    interface.createInputTable(cur)
    cur.execute("SELECT * FROM input_table")
    records = cur.fetchall()
    pprint.pprint(records)
    interface.dropTable(cur)
    interface.closeDB(cur, conn)
    
if __name__ == "__main__":
    main()
