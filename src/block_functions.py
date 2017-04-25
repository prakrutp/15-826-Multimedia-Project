import psycopg2

def findAttributes(conn, name):
    cur = conn.cursor()
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='" + name +"'")
    ret_val = cur.fetchall()
    cur.close()
    return ret_val

def findCardinalities(conn, name, attributes):
    cur = conn.cursor()
    cardinalities = [0]*len(attributes)
    cur.execute("DROP TABLE IF EXISTS distinctValuesForAttributes")
    sql_query = 'CREATE TABLE distinctValuesForAttributes(attr_no INTEGER, attr_val VARCHAR, attr_mass INTEGER, attr_flag Boolean)'
    # print sql_query, "4"
    cur.execute(sql_query)
    for i, attribute in enumerate(attributes):
        attribute[0]
        sql_query = "INSERT INTO distinctValuesForAttributes SELECT " + str(i) + ", " + attribute[0] + \
                    ", 0, false FROM input_table GROUP BY " + attribute[0]
        # print sql_query, "5"
        cur.execute(sql_query)
        sql_query = "SELECT COUNT(DISTINCT " + attribute[0] +") FROM " + name
        cur.execute(sql_query)
        cardinalities[i] = int(cur.fetchall()[0][0])
    cur.close()
    return 'distinctValuesForAttributes', cardinalities

def findCardinalitiesFromTable(conn, name, N):
    cur = conn.cursor()
    cardinalities = []
    for i in xrange(N):
        cur.execute("SELECT COUNT(*) FROM " + name + " WHERE attr_no = " + str(i))
        distinctValues = cur.fetchall()
        cardinalities += [int(distinctValues[0][0])]
    cur.close()
    return cardinalities

def findNewBlockCardinalitiesFromTable(conn, attributes, rFinal):
    cur = conn.cursor()
    cardinalities = []
    for i, attribute in enumerate(attributes):
        cur.execute("SELECT COUNT(*) FROM order_ WHERE attr_no = " + str(i) + " AND attr_order >= " + str(rFinal))
        distinctValues = cur.fetchall()
        cardinalities += [int(distinctValues[0][0])]
    cur.close()
    return cardinalities

def findMass(conn, name):
    cur = conn.cursor()
    cur.execute("SELECT SUM(count) FROM "+ name)
    result = cur.fetchall()[0][0]
    if result==None:
        return 0
    cur.close()
    return int(result)

# Check if any attribute has values
def checkEmptyCardinalities(conn, distinctValuesForAttributes):
    cur = conn.cursor()
    sql_query = "SELECT COUNT(*) FROM " + distinctValuesForAttributes
    cur.execute(sql_query)
    val = cur.fetchall()
    if val[0][0] != 0:
        cur.close()
        return True
    cur.close()
    return False

def getAttributeValuesMass(conn, name, attributes, distinctValuesForAttributes):
    cur = conn.cursor()
    sql_query = "UPDATE " + distinctValuesForAttributes + " SET attr_mass = 0"
    cur.execute(sql_query)
    for i, attribute in enumerate(attributes):
        sql_query = "UPDATE " + distinctValuesForAttributes + " SET attr_mass = tab.sc FROM (SELECT " + \
                attribute[0] + " AS attr_val_, sum(count) AS sc FROM " + name + " GROUP BY " + attribute[0] + \
                ") tab WHERE attr_no = " + str(i) + " AND attr_val = tab.attr_val_"
        # print sql_query,"9.."
        cur.execute(sql_query)
    cur.close()

def tableCopy(conn, name, new_name):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS " + new_name)
    cur.execute("CREATE TABLE " + new_name + " AS SELECT * FROM " + name)
    cur.close()

def filterTable(conn, name, attributes, rFinal):
    cur = conn.cursor()
    where_clause = ''
    for i, attribute in enumerate(attributes):
        sql_query = "SELECT attr_val FROM order_ WHERE attr_order >= " + str(rFinal) + " AND attr_no = " + str(i)      #
        where_clause += attributes[i][0] + " IN (" + sql_query + ") AND "
    cur.execute("DELETE FROM " + name + " WHERE " + where_clause[:-5])
    cur.close()
    return
