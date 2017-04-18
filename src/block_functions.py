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
    cur.execute(sql_query)
    cur1 = conn.cursor()
    for i, attribute in enumerate(attributes):
        cur.execute("SELECT DISTINCT " + attribute[0] +" FROM " + name + " WHERE flag = TRUE")
        for val in cur:
            sql_query = "INSERT INTO distinctValuesForAttributes VALUES (" + str(i) + ", '" + val[0] + "', 0, false)"
            cur1.execute(sql_query)
            cardinalities[i] += 1
    #INDEX distinctValuesForAttributes
    #cur.execute("CREATE INDEX index_distinctValuesForAttributes ON distinctValuesForAttributes (attr_no, attr_val)");
    cur1.close()
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
    cur.execute("SELECT SUM(count) FROM "+ name + " WHERE flag = TRUE")
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
    cur1 = conn.cursor()
    cur.execute("SELECT * FROM " + distinctValuesForAttributes)
    
    for row in cur:
        sql_query = "SELECT SUM(count) FROM " + name + " WHERE " + attributes[row[0]][0] + "='" + row[1] + "' AND flag = TRUE AND b_flag = TRUE"
        cur1.execute(sql_query)
        result = cur1.fetchall()[0][0]
        if result==None:
            mass = 0
        else:
            mass = int(result)
        sql_query = "UPDATE " +  distinctValuesForAttributes + " SET attr_mass = " + str(mass) + " WHERE attr_no = " + str(row[0]) + " and attr_val = '" + row[1] + "'"
        cur1.execute(sql_query)
    cur1.close()
    cur.close()

def tableCopy(conn, name, new_name):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS " + new_name)
    cur.execute("CREATE TABLE " + new_name + " (LIKE " + name + ")")
    cur.execute("INSERT INTO " + new_name  + " SELECT * FROM " + name)
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

def filterTable_impl2(conn, name, attributes, rFinal):
    cur = conn.cursor()
    where_clause = ''
    for i, attribute in enumerate(attributes):
        sql_query = "SELECT attr_val FROM order_ WHERE attr_order >= " + str(rFinal) + " AND attr_no = " + str(i)      #
        where_clause += attributes[i][0] + " IN (" + sql_query + ") AND "
    cur.execute("UPDATE " + name + " SET flag = FALSE WHERE " + where_clause + "flag = TRUE")
    cur.close()
    return

