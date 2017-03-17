import psycopg2
import sys
import interface
import pprint
from constants import *
import math
from block_functions import *
from density import *

def writeBlock(block, output):
    #TODO: Prettify
    output.write(block)


def findSingleBlock(cur, name, distinctValuesForAttributes, mass_R, rho, policy):
	distinctValuesForAttributes_B = []

	return distinctValuesForAttributes_B
	


def extractBlock(cur, name, attributes, distinctValuesForAttributes_B):
	where_clause = ''
	for i, distinctValues in enumerate(distinctValuesForAttributes_B):
		where_clause += attributes[i][0] + " IN "+ str(tuple(l)) + " AND "
	cur.execute("SELECT * FROM " + name + " WHERE " + where_clause[:-5])
	return cur.fetchall()




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
	print distinctValuesForAttributes, cardinalities
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
		filterTable(cur, original_table, attributes, distinctValuesForAttributes_B)
		block = extractBlock(cur, original_table, attributes, distinctValuesForAttributes_B)
		writeBlock(block, output)

	#-------- CLEAN-UP ----------
	#Drop copied table
	output.close()
	interface.dropTable(cur)
	interface.closeDB(cur, conn)
    
if __name__ == "__main__":
    main()
