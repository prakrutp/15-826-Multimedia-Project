import psycopg2
import sys
import interface
import pprint
from constants import *
import math
from block_functions import *
from density import *
import copy

def writeBlock(block, output):
    #TODO: Prettify
    output.write(str(block)+'\n')

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
			sql_query = "SELECT SUM(count) FROM " + name + " WHERE " + attribute[0] + "='" + val + "'"
			cur.execute(sql_query)
			result = cur.fetchall()[0][0]
			if result==None:
				mass = 0
			else:
				mass = int(result)
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
	elif policy == 'D':
		return selectDimensionbyDensity()
	sys.exit("Error: Policy Not Known\n")


def findSingleBlock(cur, name, distinctValuesForAttributes, mass, attributes, rho, policy):
	tableCopy(cur, name, 'block')
	mass_B = mass
	distinctValuesForAttributes_B = copy.deepcopy(distinctValuesForAttributes)
	print "distinctValuesForAttributes_B: ",distinctValuesForAttributes_B
	print "mass_B: ", mass_B
	N = len(attributes)
	cardinalities = [len(arr) for arr in distinctValuesForAttributes_B]
	currDensity = getDensity(rho, N, cardinalities, mass)
	print "cardinalities: ",cardinalities
	print "currDensity: ", currDensity

	r = 1
	rFinal = 1
	order = dict()

	iterno = 0
	while(checkEmptyCardinalities(distinctValuesForAttributes_B)):
		iterno +=1
		print "++++++ Iter no of while loop: ",iterno
		attributeValuesMass = getAttributeValuesMass(cur, 'block', attributes, distinctValuesForAttributes_B)
		print "attributeValuesMass: ", attributeValuesMass
		dim = selectDimension(policy, distinctValuesForAttributes_B)
		print "Selected dimension: ",dim
		if not order.has_key(dim):
			order[dim] = dict()
		removeCandidates = []
		D = attributeValuesMass[dim]
		massThreshold = (float(mass_B))/(float(len(distinctValuesForAttributes_B[dim])))
		for val, valMass in sorted(D.items(), key=lambda x: x[1]):
			if valMass <= massThreshold:
				removeCandidates.append(val)
		print "Removed candidates: ", removeCandidates
		print "MassThreshold: ", massThreshold
		for i, candidate in enumerate(removeCandidates):
			print "------ Iter no of for loop: ",i
			mass_B -= attributeValuesMass[dim][candidate]
			distinctValuesForAttributes_B[dim].remove(candidate)
			cardinalities[dim] -= 1
			newDensity = getDensity(rho, N, cardinalities, mass_B)
			print "Mass: ", mass_B
			print "distinctValuesForAttributes_B: ", distinctValuesForAttributes_B
			print "newDensity: ", newDensity
			order[dim][candidate] = r
			r += 1
			if newDensity > currDensity:
				print "Updating......."
				currDensity = newDensity
				rFinal = r
		sql_query = "DELETE FROM block WHERE " + attributes[dim][0] + " IN ('" + "','".join(removeCandidates) + "')"
		cur.execute(sql_query)
		print "Order: ", order

	newBlock = []
	for i in xrange(N):
		newBlockAttribute = []
		for k, v in order[i].iteritems():
			if v >= rFinal:
				newBlockAttribute.append(k)
		newBlock.append(newBlockAttribute)
	print "newBlock: ", newBlock
	return newBlock


def extractBlock(cur, name, attributes, distinctValuesForAttributes_B):
	where_clause = ''
	for i, distinctValues in enumerate(distinctValuesForAttributes_B):
		where_clause += attributes[i][0] + " IN ('" + "','".join(distinctValues) + "') AND "
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

	"""
	#-------- DENSITY COMPUTATION ----------
	print "Arithmetic Density ", arithmeticDensity(dimension, cardinalities, mass_R)
	print "Geometric Density ", geometricDensity(dimension, cardinalities, mass_R)
	#TODO: Test Suspiciousness
	"""

	#-------- ALGORITHM 1 ----------
	tableCopy(cur, 'input_table', 'original_input_table')
	attributes = findAttributes(cur, 'input_table')[:-1]
	distinctValuesForAttributes_R, cardinalities_R = findCardinalities(cur, 'input_table', attributes)
	block_num = 0
	for block_num in xrange(NUM_DENSE_BLOCKS):
		print "******* Iteration number: "+str(block_num)
		mass_R = findMass(cur, 'input_table')
		if (mass_R == 0):
			block_num -= 1
			break;
		distinctValuesForAttributes_R, cardinalities_R = findCardinalities(cur, 'input_table', attributes)
		distinctValuesForAttributes_B = findSingleBlock(cur, 'input_table', 
				distinctValuesForAttributes_R, mass_R, attributes, DENSITY_MEASURE, POLICY)
		filterTable(cur, 'input_table', attributes, distinctValuesForAttributes_B)
		block = extractBlock(cur, 'original_input_table', attributes, distinctValuesForAttributes_B)
		writeBlock(block, output)

	sys.stdout.write("Number of dense blocks found: " + str(block_num+1) + '\n')
	#-------- CLEAN-UP ----------
	#Drop copied table
	output.close()
	interface.dropTable(cur)
	interface.closeDB(cur, conn)
    
if __name__ == "__main__":
    main()
