import psycopg2

def findAttributes(cur, name):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='" + name +"'")
    return cur.fetchall()

def findCardinalities(cur, name, attributes):
    cardinalities = []
    distinctValuesForAttributes = []
    
    for attribute in attributes:
        distinctValuesForAttribute = []
        cur.execute("SELECT DISTINCT " + attribute[0] +" FROM " + name)
        distinctValues = cur.fetchall()
        for val in distinctValues:
            distinctValuesForAttribute.append(val[0])
        cardinalities += [len(distinctValuesForAttribute)]
        distinctValuesForAttributes.append(distinctValuesForAttribute)

    return distinctValuesForAttributes, cardinalities

def findMass(cur, name):
    cur.execute("SELECT SUM(count) FROM "+ name)
    result = cur.fetchall()[0][0]
    if result==None:
        return 0
    return int(result)

def tableCopy(cur, name, new_name):
    cur.execute("DROP TABLE IF EXISTS " + new_name)
    cur.execute("CREATE TABLE " + new_name + " AS SELECT * FROM " + name)

def filterTable(cur, name, attributes, distinctValuesForAttributes_B):
    where_clause = ''
    for i, distinctValues in enumerate(distinctValuesForAttributes_B):
        if len(distinctValues) > 0:
            where_clause += attributes[i][0] + " IN ('"+ "','".join(distinctValues) + "') AND "
    cur.execute("DELETE FROM " + name + " WHERE " + where_clause[:-5])
    return
