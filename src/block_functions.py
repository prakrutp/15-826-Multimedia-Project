import psycopg2

def findAttributes(cur, name):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='" + name +"'")
    return cur.fetchall()

def findCardinalities(cur, name, attributes):
    cardinalities = []
    distinctValuesForAttributes = []
    
    for attribute in attributes[:-1]:
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
    return int(cur.fetchall()[0][0])

def copy(cur, name):
    new_table = 'original_' + name
    cur.execute("CREATE TABLE " + new_table + " AS SELECT * FROM " + name)
    return new_table

def filterTable(cur, name, attributes, distinctValuesForAttributes_B):
    where_clause = ''
    for i, distinctValues in enumerate(distinctValuesForAttributes_B):
        where_clause += attributes[i][0] + " IN "+ str(tuple(l)) + " AND "
    cur.execute("DELETE FROM " + name + " WHERE " + where_clause[:-5])
    return
