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

# Check if any attribute has values
def checkEmptyCardinalities(distinctValuesForAttributes):
	for distinctValues in distinctValuesForAttributes:
		if len(distinctValues) != 0:
			return True
	return False

def getAttributeValuesMass(cur, name, attributes, distinctValuesForAttributes):
	attributeValuesMass = dict()
	for i, attribute in enumerate(attributes):
		attributeValuesMass[i] = dict()
		for val in distinctValuesForAttributes[i]:
			sql_query = "SELECT SUM(count) FROM " + name + " WHERE " + attribute[0] + " = " val
			cur.execute(sql_query)
			mass = int(cur.fetchall())
			attributeValuesMass[i][val] = mass
	return attributeValuesMass

#TODO
def selectDimensionbyDensity():
	return -1

def selectDimensionbyCardinality(distinctValuesForAttributes):
	maxIndex = -1
	maxValue = -1
	for i, distinctValues in enumerate(distinctValuesForAttributes):
		currLen = len(distinctValues)
		if currLen > maxValue:
			maxValue = currLen
			maxIndex = i
	return maxIndex

def selectDimension(policy, distinctValuesForAttributes):
	if policy == 'C':
		return selectDimensionbyCardinality(distinctValuesForAttributes)
	else if policy == 'D':
		return selectDimensionbyDensity()
	sys.exit("Error: Policy Not Known\n")


def findSingleBlock(cur, name, distinctValuesForAttributes, mass, attributes, rho, policy):
	copy(cur, name, 'block')
	mass_B = mass
	distinctValuesForAttributes_B = distinctValuesForAttributes
	N = len(attributes)
	cardinalities = [len(arr) for arr in distinctValuesForAttributes_B]
	currDensity = getDensity(rho, N, cardinalities, mass)

	r = 1
	rFinal = 1
	while(checkEmptyCardinalities(distinctValuesForAttributes_B)):
		order = dict()
		attributeValuesMass = getAttributeValuesMass(cur, 'block', attributes, distinctValuesForAttributes_B)
		dim = selectDimension(policy, distinctValuesForAttributes_B)
		order[dim] = dict()
		removeCandidates = []
		D = attributeValuesMass[dim]
		massThreshold = ((float)mass_B)/len(distinctValuesForAttributes[dim])
		for val, valMass in sorted(D.items(), key=lambda x: x[1]):
			if valMass <= massThreshold:
				removeCandidates.append(val)
		for candidate in removeCandidates:

			mass_B -= attributeValuesMass[dim][candidate]
			distinctValuesForAttributes_B[dim].remove(candidate)
			cardinalities[dim] -= 1

			newDensity = getDensity(rho, N, cardinalities, mass_B)
			order[dim][candidate] = r
			r += 1

			if newDensity > currDensity:
				currDensity = newDensity
				rFinal = r

    	cur.execute("DELETE FROM block WHERE " + attributes[dim][0] + " IN (" + ','.join(removeCandidates) + ")")
    newBlock = []
    for i in xrange(N):
    	newBlockAttribute = []
    	for k, v in order[i]:
    		if v >= rFinal:
    			newBlockAttribute.append(k)
    	newBlock.append(newBlockAttribute)
    return newBlock


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
	copy(cur, 'input_table', 'original_input_table')
	attributes = findAttributes(cur, 'input_table')
	distinctValuesForAttributes_R, cardinalities_R = findCardinalities(cur, 'input_table', attributes)
	for block_num in xrange(NUM_DENSE_BLOCKS):
		print block_num
		mass_R = findMass(cur, 'input_table')
		#Recompute distinctValuesForAttributes_R
		print 'DistValues',distinctValuesForAttributes_R
		print 'M_R', mass_R
		distinctValuesForAttributes_B = findSingleBlock(cur, 'input_table', 
				distinctValuesForAttributes_R, mass_R, attributes, DENSITY_MEASURE, POLICY)
		# filterTable(cur, original_input_table, attributes, distinctValuesForAttributes_B)
		# block = extractBlock(cur, original_input_table, attributes, distinctValuesForAttributes_B)
		# writeBlock(block, output)

	#-------- CLEAN-UP ----------
	#Drop copied table
	output.close()
	interface.dropTable(cur)
	interface.closeDB(cur, conn)
    
if __name__ == "__main__":
    main()
