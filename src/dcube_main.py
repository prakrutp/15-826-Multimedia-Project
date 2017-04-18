import psycopg2
import sys
import interface
import pprint
from constants import *
import math
from block_functions import *
from density import *
from policies import *
import copy
import time

#-------- WRITE OUTPUT ----------
def writeBlockDensity(output, block_density, block_num):
    output.write(str(block_num) + ' : ' + str(block_density) + '\n')

def extractBlock(conn, name, attributes, rFinal, block_num):
    cur = conn.cursor()
    where_clause = ''
    select_clause = ''
    for i, attribute in enumerate(attributes):
        sql_query = "SELECT attr_val FROM order_ WHERE attr_order >= " + str(rFinal) + " AND attr_no = " + str(i)
        where_clause += attributes[i][0] + " IN (" + sql_query + ") AND "
        select_clause += ", " + attributes[i][0] 
    sql_query = "INSERT INTO " + OUTPUT_TABLE_NAME + " SELECT " + str(block_num) + select_clause + " FROM " + name + " WHERE " + where_clause[:-5]
    cur.execute(sql_query)
    sql_query = "SELECT SUM(count) FROM " + name + " WHERE " + where_clause[:-5]
    cur.execute(sql_query)
    mass = cur.fetchall()[0][0]
    return mass

#-------- ALGORITHM 2 ----------
def findSingleBlock(conn, name, distinctValuesForAttributes, mass, attributes, rho, policy):
    cur = conn.cursor()
    tableCopy(conn, name, 'block')
    #cur.execute("CREATE INDEX index_block_attr1 ON block ("+ attributes[0][0]+")");
    #cur.execute("CREATE INDEX index_block_attr2 ON block ("+ attributes[1][0]+")");
    #cur.execute("CREATE INDEX index_block_attr3 ON block ("+ attributes[2][0]+")");
    mass_B = copy.deepcopy(mass)
    distinctValuesForAttributes_B = 'distinctValuesForAttributes_B'
    tableCopy(conn, distinctValuesForAttributes , distinctValuesForAttributes_B)

    # cur.execute("SELECT * FROM " + distinctValuesForAttributes_B)     #*
    # records = cur.fetchall()      #*
    # pprint.pprint(records)        #*
    # print "mass_B: ", mass_B      #*

    N = len(attributes)
    cardinalities_R = findCardinalitiesFromTable(conn, distinctValuesForAttributes_B, N)
    cardinalities = copy.deepcopy(cardinalities_R)
    currDensity = getDensity(rho, N, cardinalities, mass, cardinalities, mass)

    # print "cardinalities: ",cardinalities         #*
    # print "currDensity: ", currDensity        #*

    r = 1
    rFinal = 1
    cur.execute("DROP TABLE IF EXISTS order_")
    sql_query = "CREATE TABLE order_(attr_no INTEGER, attr_val VARCHAR, attr_order INTEGER)"
    cur.execute(sql_query)
    cur1 = conn.cursor()
    for i, attribute in enumerate(attributes):
        cur.execute("SELECT DISTINCT " + attribute[0] +" FROM " + name)
        for val in cur:
            sql_query = "INSERT INTO order_ VALUES (" + str(i) + ", '" + val[0] + "', 0)"
            cur1.execute(sql_query)
    
    #cur.execute("CREATE INDEX index_attrno_order_ ON order_ (attr_no, attr_val)");
    #cur.execute("CREATE INDEX index_attrno_order_ ON order_ (attr_no)");
    iterno = 0
    while(checkEmptyCardinalities(conn, distinctValuesForAttributes_B)):
        iterno +=1
        # print "++++++ Iter no of while loop: ", iterno        #*
        getAttributeValuesMass(conn, 'block', attributes, distinctValuesForAttributes_B)

        # cur.execute("SELECT * FROM " + distinctValuesForAttributes_B)         #*
        # records = cur.fetchall()      #*
        # pprint.pprint(records)        #*

        dim = selectDimension(conn, policy, cardinalities, distinctValuesForAttributes_B, mass_B, rho, cardinalities_R, mass)

        # print "Selected dimension: ", dim     #*
        # print "Order: ",      #*
        # cur.execute("SELECT * FROM order_")       #*
        # records = cur.fetchall()      #*
        # pprint.pprint(records)        #*

        massThreshold = (float(mass_B))/(float(cardinalities[dim]))
        sql_query = "UPDATE " + distinctValuesForAttributes_B + " SET attr_flag=true WHERE attr_no = " + str(dim) + " and attr_mass <= " +str(massThreshold)
        cur.execute(sql_query)

        
        # cur.execute("SELECT * FROM " + distinctValuesForAttributes_B + " WHERE attr_no = " + str(dim) + " and attr_flag = true")      #*
        # records = cur.fetchall()      #*
        # print "Removed candidates: ", records     #*
        # print "MassThreshold: ", massThreshold        #*

        
        sql_query = "SELECT * FROM " + distinctValuesForAttributes_B + " WHERE attr_no = " + str(dim) + " and attr_flag = true ORDER BY attr_mass"
        cur.execute(sql_query)
        for i, candidate in enumerate(cur):
            # print "------ Iter no of for loop: ", i       #*

            mass_B -= int(candidate[2])
            sql_query = "DELETE FROM " + distinctValuesForAttributes_B + " WHERE attr_no = " + str(dim) + " and attr_val = '"+ candidate[1] + "'"
            cur1.execute(sql_query)
            cardinalities[dim] -= 1
            newDensity = getDensity(rho, N, cardinalities, mass_B, cardinalities_R, mass)

            # print "Mass: ", mass_B        #*
            # print "distinctValuesForAttributes_B: ",      #*
            # cur1.execute("SELECT * FROM " + distinctValuesForAttributes_B)        #*
            # records = cur1.fetchall()     #*
            # pprint.pprint(records)        #*
            # print "newDensity: ", newDensity      #*
            
            sql_query = "UPDATE order_ SET attr_order = " + str(r) + " WHERE attr_no = " + str(dim) + " and attr_val = '"+ candidate[1] + "'"
            cur1.execute(sql_query)
            r += 1
            if newDensity > currDensity:
                # print "Updating......."       #*
                currDensity = newDensity
                rFinal = r

            sql_query = "DELETE FROM block WHERE " + attributes[dim][0] + " = '" + candidate[1] + "'"
            cur1.execute(sql_query)

        # print "Order: ",      #*
        # cur.execute("SELECT * FROM order_")       #*
        # records = cur.fetchall()      #*
        # pprint.pprint(records)        #*

    # print "newBlock: ",       #*
    # sql_query = "SELECT * FROM order_ WHERE attr_order >= " + str(rFinal)     #*
    # cur.execute(sql_query)        #*
    # print cur.fetchall()      #*
    return rFinal

def findSingleBlock_impl2(conn, name, distinctValuesForAttributes, mass, attributes, rho, policy):
    cur = conn.cursor()
    # tableCopy(conn, name, 'block')
    sql_query = "UPDATE " + name + " SET b_flag = TRUE WHERE flag = TRUE" 
    cur.execute(sql_query)
    #cur.execute("CREATE INDEX index_block_attr1 ON block ("+ attributes[0][0]+")");
    #cur.execute("CREATE INDEX index_block_attr2 ON block ("+ attributes[1][0]+")");
    #cur.execute("CREATE INDEX index_block_attr3 ON block ("+ attributes[2][0]+")");
    mass_B = copy.deepcopy(mass)
    distinctValuesForAttributes_B = 'distinctValuesForAttributes_B'
    tableCopy(conn, distinctValuesForAttributes , distinctValuesForAttributes_B)

    # cur.execute("SELECT * FROM " + distinctValuesForAttributes_B)     #*
    # records = cur.fetchall()      #*
    # pprint.pprint(records)        #*
    # print "mass_B: ", mass_B      #*

    N = len(attributes)
    cardinalities_R = findCardinalitiesFromTable(conn, distinctValuesForAttributes_B, N)
    cardinalities = copy.deepcopy(cardinalities_R)
    currDensity = getDensity(rho, N, cardinalities, mass, cardinalities, mass)

    # print "cardinalities: ",cardinalities         #*
    # print "currDensity: ", currDensity        #*

    r = 1
    rFinal = 1
    cur.execute("DROP TABLE IF EXISTS order_")
    sql_query = "CREATE TABLE order_(attr_no INTEGER, attr_val VARCHAR, attr_order INTEGER)"
    cur.execute(sql_query)
    cur1 = conn.cursor()
    for i, attribute in enumerate(attributes):
        cur.execute("SELECT DISTINCT " + attribute[0] +" FROM " + name + " WHERE flag = TRUE")
        for val in cur:
            sql_query = "INSERT INTO order_ VALUES (" + str(i) + ", '" + val[0] + "', 0)"
            cur1.execute(sql_query)
    
    print "BLOCK Setup Done" #**
    #cur.execute("CREATE INDEX index_attrno_order_ ON order_ (attr_no, attr_val)");
    #cur.execute("CREATE INDEX index_attrno_order_ ON order_ (attr_no)");
    iterno = 0
    while(checkEmptyCardinalities(conn, distinctValuesForAttributes_B)):
        iterno +=1
        print "++++++ Iter no of while loop: ", iterno        #*
        getAttributeValuesMass(conn, name , attributes, distinctValuesForAttributes_B)

        # cur.execute("SELECT * FROM " + distinctValuesForAttributes_B)         #*
        # records = cur.fetchall()      #*
        # pprint.pprint(records)        #*
        
        print "BLOCK selecting dimension" #**
        dim = selectDimension(conn, policy, cardinalities, distinctValuesForAttributes_B, mass_B, rho, cardinalities_R, mass)

        # print "Selected dimension: ", dim     #*
        # print "Order: ",      #*
        # cur.execute("SELECT * FROM order_")       #*
        # records = cur.fetchall()      #*
        # pprint.pprint(records)        #*

        print "BLOCK before update" #**

        massThreshold = (float(mass_B))/(float(cardinalities[dim]))
        sql_query = "UPDATE " + distinctValuesForAttributes_B + " SET attr_flag=true WHERE attr_no = " + str(dim) + " and attr_mass <= " +str(massThreshold)
        cur.execute(sql_query)

        
        # cur.execute("SELECT * FROM " + distinctValuesForAttributes_B + " WHERE attr_no = " + str(dim) + " and attr_flag = true")      #*
        # records = cur.fetchall()      #*
        # print "Removed candidates: ", records     #*
        # print "MassThreshold: ", massThreshold        #*

        print "BLOCK after update" #**
        sql_query = "SELECT * FROM " + distinctValuesForAttributes_B + " WHERE attr_no = " + str(dim) + " and attr_flag = true ORDER BY attr_mass"
        cur.execute(sql_query)
        for i, candidate in enumerate(cur):
            print "------ Iter no of for loop: ", i       #*

            mass_B -= int(candidate[2])
            sql_query = "DELETE FROM " + distinctValuesForAttributes_B + " WHERE attr_no = " + str(dim) + " and attr_val = '"+ candidate[1] + "'"
            cur1.execute(sql_query)
            cardinalities[dim] -= 1
            newDensity = getDensity(rho, N, cardinalities, mass_B, cardinalities_R, mass)

            # print "Mass: ", mass_B        #*
            # print "distinctValuesForAttributes_B: ",      #*
            # cur1.execute("SELECT * FROM " + distinctValuesForAttributes_B)        #*
            # records = cur1.fetchall()     #*
            # pprint.pprint(records)        #*
            # print "newDensity: ", newDensity      #*
            
            sql_query = "UPDATE order_ SET attr_order = " + str(r) + " WHERE attr_no = " + str(dim) + " and attr_val = '"+ candidate[1] + "'"
            cur1.execute(sql_query)
            r += 1
            if newDensity > currDensity:
                # print "Updating......."       #*
                currDensity = newDensity
                rFinal = r

            sql_query = "UPDATE " + name + " SET b_flag = FALSE WHERE " + attributes[dim][0] + " = '" + candidate[1] + "' AND flag = TRUE AND b_flag = TRUE"
            cur1.execute(sql_query)

        # print "Order: ",      #*
        # cur.execute("SELECT * FROM order_")       #*
        # records = cur.fetchall()      #*
        # pprint.pprint(records)        #*

    # print "newBlock: ",       #*
    # sql_query = "SELECT * FROM order_ WHERE attr_order >= " + str(rFinal)     #*
    # cur.execute(sql_query)        #*
    # print cur.fetchall()      #*
    return rFinal

# def main():
#     #-------- SET-UP ----------
#     conn = interface.connectDB()
#     cur = conn.cursor()
#     interface.createInputTable(conn)
#     interface.createOutputTable(conn)
#     output = open(OUTPUT_LOCATION, 'w')
#     # cur.execute('SELECT * FROM input_table limit 10')
#     # records = cur.fetchall()
#     # pprint.pprint(records)
#     sys.stdout.write("Trying to find " + str(NUM_DENSE_BLOCKS) + ' blocks\nDensity Measure Used: ' + DENSITY_MEASURE + '\nPolicy Used: ' + POLICY + '\n')

#     #-------- ALGORITHM 1 ----------
#     startTime = time.time()
#     dimension = NUM_ATTRIBUTES
#     attributes = findAttributes(conn, 'input_table')[:-1]
#     sql_query = "ALTER TABLE input_table ADD COLUMN flag boolean"
#     cur.execute(sql_query)
#     sql_query = "UPDATE input_table SET flag = TRUE, b_flag = TRUE" 
#     cur.execute(sql_query)
#     tableCopy(conn, 'input_table', 'original_input_table')
#     #cur.execute("CREATE INDEX index_original_input_table_attr1 ON original_input_table ("+ attributes[0][0]+")");
#     #cur.execute("CREATE INDEX index_original_input_table_attr2 ON original_input_table ("+ attributes[1][0]+")");
#     #cur.execute("CREATE INDEX index_original_input_table_attr3 ON original_input_table ("+ attributes[2][0]+")");
#     distinctValuesForAttributes_R, cardinalities_R = findCardinalities(conn, 'input_table', attributes)
#     # mass_R = findMass(conn, 'input_table')
#     block_num = 0; block_densities = []

#     for block_num in xrange(NUM_DENSE_BLOCKS):
#         mass_R = findMass(conn, 'input_table')
#         if (mass_R == 0):
#             block_num -= 1
#             break;
#         print "*** Finding Block "+str(block_num)
#         distinctValuesForAttributes_R, cardinalities_R_temp = findCardinalities(conn, 'input_table', attributes)
#         rFinal = findSingleBlock(conn, 'input_table', 
#                 distinctValuesForAttributes_R, mass_R, attributes, DENSITY_MEASURE, POLICY)
#         filterTable(conn, 'input_table', attributes, rFinal)
#         mass_B = extractBlock(conn, 'original_input_table', attributes, rFinal, block_num)
#         cardinalities_B = findNewBlockCardinalitiesFromTable(conn, attributes, rFinal)
#         block_density = getDensity(DENSITY_MEASURE, dimension, cardinalities_B, mass_B, cardinalities_R, mass_R)

#         print "Block found with Density ", block_density
#         block_densities.append(block_density)
#         writeBlockDensity(output, block_density, block_num)
#     endTime = time.time()
#     sys.stdout.write("Total time taken by algorithm: "+str(endTime - startTime)+'\n')
#     sys.stdout.write("Number of dense blocks found: " + str(block_num+1) + '\n')

#     # cur.execute("SELECT * FROM " + OUTPUT_TABLE_NAME + " LIMIT 10")       #*
#     # records = cur.fetchall()      #*
#     # pprint.pprint(records)        #*

#     #-------- CLEAN-UP ----------
#     output.close()
#     sql_query = "Copy (Select * From " + OUTPUT_TABLE_NAME + ") To '" + OUTPUT_LOCATION_BLOCKS + "' With CSV DELIMITER ','";
#     cur.execute(sql_query)
#     interface.dropTable(conn, ['input_table', 'original_input_table','order_'])
#     cur.close()
#     interface.closeDB(conn)

def main():
    #-------- SET-UP ----------
    conn = interface.connectDB()
    cur = conn.cursor()
    interface.createInputTable(conn)
    interface.createOutputTable(conn)
    output = open(OUTPUT_LOCATION, 'w')
    # cur.execute('SELECT * FROM input_table limit 10')
    # records = cur.fetchall()
    # pprint.pprint(records)
    sys.stdout.write("Trying to find " + str(NUM_DENSE_BLOCKS) + ' blocks\nDensity Measure Used: ' + DENSITY_MEASURE + '\nPolicy Used: ' + POLICY + '\n')

    #-------- ALGORITHM 1 ----------
    startTime = time.time()
    dimension = NUM_ATTRIBUTES
    attributes = findAttributes(conn, 'input_table')[:-1]
    sql_query = "ALTER TABLE input_table ADD COLUMN flag boolean, ADD COLUMN b_flag boolean"
    cur.execute(sql_query)
    sql_query = "UPDATE input_table SET flag = TRUE, b_flag = TRUE" 
    cur.execute(sql_query)
    print "Initial set done"     #**
    # tableCopy(conn, 'input_table', 'original_input_table')
    #cur.execute("CREATE INDEX index_original_input_table_attr1 ON original_input_table ("+ attributes[0][0]+")");
    #cur.execute("CREATE INDEX index_original_input_table_attr2 ON original_input_table ("+ attributes[1][0]+")");
    #cur.execute("CREATE INDEX index_original_input_table_attr3 ON original_input_table ("+ attributes[2][0]+")");
    distinctValuesForAttributes_R, cardinalities_R = findCardinalities(conn, 'input_table', attributes)
    # mass_R = findMass(conn, 'input_table')
    block_num = 0; block_densities = []

    for block_num in xrange(NUM_DENSE_BLOCKS):
        mass_R = findMass(conn, 'input_table')
        if (mass_R == 0):
            block_num -= 1
            break;
        print "*** Finding Block "+str(block_num)
        distinctValuesForAttributes_R, cardinalities_R_temp = findCardinalities(conn, 'input_table', attributes)
        print "Calling algo 2" #**
        rFinal = findSingleBlock_impl2(conn, 'input_table', 
                distinctValuesForAttributes_R, mass_R, attributes, DENSITY_MEASURE, POLICY)
    #     cur.execute(sql_query)
    # sql_query = "UPDATE input_table SET flag = TRUE, b_flag = TRUE" 
    # cur.execute(sql_query)
        print "Filtering Table" #**
        filterTable_impl2(conn, 'input_table', attributes, rFinal)
        print "Extracting Table" #**
        mass_B = extractBlock(conn, 'input_table', attributes, rFinal, block_num)
        cardinalities_B = findNewBlockCardinalitiesFromTable(conn, attributes, rFinal)
        block_density = getDensity(DENSITY_MEASURE, dimension, cardinalities_B, mass_B, cardinalities_R, mass_R)

        print "Block found with Density ", block_density
        block_densities.append(block_density)
        writeBlockDensity(output, block_density, block_num)
    endTime = time.time()
    sys.stdout.write("Total time taken by algorithm: "+str(endTime - startTime)+'\n')
    sys.stdout.write("Number of dense blocks found: " + str(block_num+1) + '\n')

    # cur.execute("SELECT * FROM " + OUTPUT_TABLE_NAME + " LIMIT 10")       #*
    # records = cur.fetchall()      #*
    # pprint.pprint(records)        #*

    #-------- CLEAN-UP ----------
    output.close()
    sql_query = "Copy (Select * From " + OUTPUT_TABLE_NAME + ") To '" + OUTPUT_LOCATION_BLOCKS + "' With CSV DELIMITER ','";
    cur.execute(sql_query)
    interface.dropTable(conn, ['input_table', 'original_input_table','order_'])
    cur.close()
    interface.closeDB(conn)
    
if __name__ == "__main__":
    main()

