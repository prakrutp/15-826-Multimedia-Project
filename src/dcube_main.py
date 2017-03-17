import psycopg2
import sys
import interface
import pprint
from constants import *
import math

def arithmeticDensity(dimension, cardinalities, mass):
	sumOfCardinalities = float(sum(cardinalities))
	if sumOfCardinalities == 0.0:
		return -1
	return mass/(sumOfCardinalities/dimension)

def geometricDensity(dimension, cardinalities, mass):
	productOfCardinalities = 1.0
	for cardinality in cardinalities:
		productOfCardinalities *= cardinality
	if productOfCardinalities == 0.0:
		return -1
	return mass/math.pow(productOfCardinalities, 1.0/dimension)

def suspiciousnessDensity(dimension, cardinalities_B, mass_B, cardinalities_R, mass_R):
	productOfCardinalities_B = 1.0
	productOfCardinalities_R = 1.0
	for i, cardinality_R in enumerate(cardinalities_R):
		productOfCardinalities_R *= cardinality_R
		productOfCardinalities_B *= cardinalities_B[i]
	if productOfCardinalities_R == 0 or productOfCardinalities_B == 0 or mass_B == 0 or mass_R == 0:
		return -1;
	return (mass_B * (math.log(mass_B/ float(mass_R)) - 1) 
		+ mass_R * (productOfCardinalities_B / productOfCardinalities_R) 
		- mass_B * math.log(productOfCardinalities_B / productOfCardinalities_R))

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

def findSingleBlock(cur, name, distinctValuesForAttributes, mass_R, rho, policy):
	distinctValuesForAttributes_B = []

	return distinctValuesForAttributes_B
	
def filterTable(cur, name, attributes, distinctValuesForAttributes_B):
	where_clause = ''
	for i, distinctValues in enumerate(distinctValuesForAttributes_B):
		where_clause += attributes[i][0] + " IN "+ str(tuple(l)) + " AND "
	cur.execute("DELETE FROM " + name + " WHERE " + where_clause[:-5])
	return

def extractBlock(cur, name, attributes, distinctValuesForAttributes_B):
	where_clause = ''
	for i, distinctValues in enumerate(distinctValuesForAttributes_B):
		where_clause += attributes[i][0] + " IN "+ str(tuple(l)) + " AND "
	cur.execute("SELECT * FROM " + name + " WHERE " + where_clause[:-5])
	return cur.fetchall()

def writeBlock(block, output):
	#TODO: Prettify
	output.write(block)


def main():
	#-------- SET-UP ----------
	conn, cur = interface.connectDB()
	interface.createInputTable(cur)
	output = open(OUTPUT_LOCATION, 'w')
	cur.execute("SELECT * FROM input_table")
	records = cur.fetchall()
	pprint.pprint(records)

	#-------- Relation/Block Definitions ----------
	dimension = NUM_ATTRIBUTES
	attributes = findAttributes(cur, 'input_table')
	distinctValuesForAttributes, cardinalities = findCardinalities(cur, 'input_table', attributes)
	mass_R = findMass(cur, 'input_table')
	print "Mass of Relation ", mass_R
	#TODO: Define a block


	#-------- DENSITY COMPUTATION ----------
	print "Arithmetic Density ", arithmeticDensity(dimension, cardinalities, mass_R)
	print "Geometric Density ", geometricDensity(dimension, cardinalities, mass_R)
	#TODO: Test Suspiciousness

	#-------- ALGORITHM 1 ----------
	original_table = copy(cur, 'input_table')
	attributes = findAttributes(cur, 'input_table')
	distinctValuesForAttributes_R, cardinalities_R = findCardinalities(cur, 'input_table', attributes)
	for block_num in xrange(NUM_DENSE_BLOCKS):
		mass_R = findMass(cur, 'input_table')
		#Recompute distinctValuesForAttributes_R
		distinctValuesForAttributes_B = findSingleBlock(cur, 'input_table', 
				distinctValuesForAttributes_R, mass_R, DENSITY_MEASURE, POLICY)
		filterTable(cur, name, attributes, distinctValuesForAttributes_B)
		block = extractBlock(cur, original_table, attributes, distinctValuesForAttributes_B)
		writeBlock(block, output)

	#-------- CLEAN-UP ----------
	#Drop copied table
	output.close()
	interface.dropTable(cur)
	interface.closeDB(cur, conn)
    
if __name__ == "__main__":
    main()
