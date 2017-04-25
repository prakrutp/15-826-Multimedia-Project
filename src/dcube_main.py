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

#-------- WRITE OUTPUT ----------
def writeBlockDensity(output, block_density, block_num):
	output.write(str(block_num) + ' : ' + str(block_density) + '\n')

def extractBlock(conn, name, attributes, rFinal, block_num):
	cur = conn.cursor()
	where_clause = ''
	for i, attribute in enumerate(attributes):
		sql_query = "SELECT attr_val FROM order_ WHERE attr_order >= " + str(rFinal) + " AND attr_no = " + str(i)
		where_clause += attributes[i][0] + " IN (" + sql_query + ") AND "
	sql_query = "INSERT INTO " + OUTPUT_TABLE_NAME + " SELECT " + str(block_num) + ", * FROM " + name + " WHERE " + where_clause[:-5]
	cur.execute(sql_query)
	sql_query = "SELECT SUM(count) FROM " + name + " WHERE " + where_clause[:-5]
	cur.execute(sql_query)
	mass = cur.fetchall()[0][0]
	return mass

#-------- ALGORITHM 2 ----------
def findSingleBlock(conn, name, distinctValuesForAttributes, mass, attributes, rho, policy):
	cur = conn.cursor()
	cur1 = conn.cursor()
	tableCopy(conn, name, 'block')
	mass_B = copy.deepcopy(mass)
	distinctValuesForAttributes_B = 'distinctValuesForAttributes_B'
	tableCopy(conn, distinctValuesForAttributes , distinctValuesForAttributes_B)

	N = len(attributes)
	cardinalities_R = findCardinalitiesFromTable(conn, distinctValuesForAttributes_B, N)
	cardinalities = copy.deepcopy(cardinalities_R)
	currDensity = getDensity(rho, N, cardinalities, mass, cardinalities, mass)

	r = 1
	rFinal = 1
	cur.execute("DROP TABLE IF EXISTS order_")
	sql_query = "CREATE TABLE order_ AS (SELECT attr_no, attr_val, 0 as attr_order from distinctValuesForAttributes)"
	cur.execute(sql_query)
	
	iterno = 0
	while(checkEmptyCardinalities(conn, distinctValuesForAttributes_B)):
		iterno +=1
		getAttributeValuesMass(conn, 'block', attributes, distinctValuesForAttributes_B)

		dim = selectDimension(conn, policy, cardinalities, distinctValuesForAttributes_B, mass_B, rho, cardinalities_R, mass)

		massThreshold = (float(mass_B))/(float(cardinalities[dim]))
		sql_query = "UPDATE " + distinctValuesForAttributes_B + " SET attr_flag=true WHERE attr_no = " + str(dim) + " and attr_mass <= " +str(massThreshold)
		cur.execute(sql_query)
		
		sql_query = "SELECT * FROM " + distinctValuesForAttributes_B + " WHERE attr_no = " + str(dim) + " and attr_flag = true ORDER BY attr_mass"
		cur.execute(sql_query)

		temp = []
		for i, candidate in enumerate(cur):

			mass_B -= int(candidate[2])
			cardinalities[dim] -= 1
			newDensity = getDensity(rho, N, cardinalities, mass_B, cardinalities_R, mass)
			temp.append("('"+str(candidate[1])+"',"+str(r)+')')
			r += 1
			if newDensity > currDensity:
				currDensity = newDensity
				rFinal = r

		cur.execute("DROP TABLE IF EXISTS temp")
		sql_query = "CREATE TABLE temp(attr_val VARCHAR, attr_order INTEGER)"
		cur.execute(sql_query)
		sql_query = "INSERT INTO temp VALUES " + ','.join(temp)
		cur.execute(sql_query)

		sql_query = "UPDATE order_ SET attr_order = t.attr_order FROM (SELECT attr_val, attr_order from temp)" + \
					" AS t WHERE order_.attr_no = " + str(dim) + " AND order_.attr_val=t.attr_val"	
		cur.execute(sql_query)

		sql_query = "DELETE FROM block WHERE " + attributes[dim][0] + " IN (SELECT attr_val FROM " + \
		 distinctValuesForAttributes_B + " WHERE attr_no = " + str(dim) + " and attr_flag = true)"
		cur.execute(sql_query)
		sql_query = "DELETE FROM " + distinctValuesForAttributes_B + " WHERE attr_no = " + str(dim) + " and attr_flag = true"
		cur.execute(sql_query)

	return rFinal

def main():
	#-------- SET-UP ----------
	conn = interface.connectDB()
	cur = conn.cursor()
	interface.createInputTable(conn)
	interface.createOutputTable(conn)
	print OUTPUT_LOCATION
	output = open(OUTPUT_LOCATION, 'w')
	sys.stdout.write("Trying to find " + str(NUM_DENSE_BLOCKS) + ' blocks\nDensity Measure Used: ' + DENSITY_MEASURE + '\nPolicy Used: ' + POLICY + '\n')

	#-------- ALGORITHM 1 ----------
	dimension = NUM_ATTRIBUTES
	tableCopy(conn, 'input_table', 'original_input_table')
	attributes = findAttributes(conn, 'input_table')[:-1]
	distinctValuesForAttributes_R, cardinalities_R = findCardinalities(conn, 'input_table', attributes)
	mass_R = findMass(conn, 'input_table')
	block_num = 0; block_densities = []

	for block_num in xrange(NUM_DENSE_BLOCKS):
		mass_R = findMass(conn, 'input_table')
		if (mass_R == 0):
			block_num -= 1
			break;
		print "*** Finding Block "+str(block_num)
		distinctValuesForAttributes_R, cardinalities_R_temp = findCardinalities(conn, 'input_table', attributes)
		rFinal = findSingleBlock(conn, 'input_table', 
				distinctValuesForAttributes_R, mass_R, attributes, DENSITY_MEASURE, POLICY)
		filterTable(conn, 'input_table', attributes, rFinal)
		mass_B = extractBlock(conn, 'original_input_table', attributes, rFinal, block_num)
		cardinalities_B = findNewBlockCardinalitiesFromTable(conn, attributes, rFinal)
		block_density = getDensity(DENSITY_MEASURE, dimension, cardinalities_B, mass_B, cardinalities_R, mass_R)

		print "Block found with Density ", block_density
		block_densities.append(block_density)
		writeBlockDensity(output, block_density, block_num)

	sys.stdout.write("Number of dense blocks found: " + str(block_num+1) + '\n')

	#-------- CLEAN-UP ----------
	output.close()
	sql_query = "Copy (Select * From " + OUTPUT_TABLE_NAME + ") To '" + OUTPUT_LOCATION_BLOCKS + "' With CSV DELIMITER ','";
	cur.execute(sql_query)
	interface.dropTable(conn, ['input_table', 'original_input_table','order_'])
	cur.close()
	interface.closeDB(conn)
    
if __name__ == "__main__":
    main()
