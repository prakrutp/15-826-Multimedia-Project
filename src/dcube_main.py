import psycopg2
import sys
import interface
import pprint
from constants import *
import math
from block_functions import *
from density import *
import copy

def writeBlockDensity(output, block_density, block_num):
	output.write(str(block_num) + ' : ' + str(block_density) + '\n')

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

def selectDimensionbyDensity(inpDistinctValuesForAttributes, mass, attributeValuesMass, rho, cardinalities_R, mass_R):
	currDensity = -1
	dimFinal = 0
	for dim in xrange(len(inpDistinctValuesForAttributes)):
		mass_updated = copy.deepcopy(mass)
		distinctValuesForAttributes = copy.deepcopy(inpDistinctValuesForAttributes)
		cardinalities = [len(arr) for arr in distinctValuesForAttributes]
		distinctValues = distinctValuesForAttributes[dim]
		if len(distinctValues) != 0:
			removeCandidates = []
			D = attributeValuesMass[dim]
			massThreshold = (float(mass_updated))/(float(len(distinctValues)))
			for val, valMass in D.items():
				if valMass <= massThreshold:
					removeCandidates.append(val)

			for i, candidate in enumerate(removeCandidates):
				mass_updated -= D[candidate]
				distinctValuesForAttributes[dim].remove(candidate)
				cardinalities[dim] -= 1
			newDensity = getDensity(rho, len(cardinalities), cardinalities, mass_updated, cardinalities_R, mass_R)
			if newDensity >= currDensity:
				currDensity = newDensity
				dimFinal = dim
	return dimFinal

def selectDimensionbyCardinality(distinctValuesForAttributes):
	maxIndex = -1
	maxValue = -1
	for i, distinctValues in enumerate(distinctValuesForAttributes):
		currLen = len(distinctValues)
		if currLen > maxValue:
			maxValue = currLen
			maxIndex = i
	return maxIndex

def selectDimension(policy, distinctValuesForAttributes, mass, 
					attributeValuesMass, rho, cardinalities_R, mass_R):
	if policy == 'C':
		return selectDimensionbyCardinality(distinctValuesForAttributes)
	elif policy == 'D':
		return selectDimensionbyDensity(distinctValuesForAttributes, mass, 
										attributeValuesMass, rho, cardinalities_R, mass_R)
	sys.exit("Error: Policy Not Known\n")

def findSingleBlock(cur, name, distinctValuesForAttributes, mass, attributes, rho, policy):
	tableCopy(cur, name, 'block')
	mass_B = copy.deepcopy(mass)
	distinctValuesForAttributes_B = copy.deepcopy(distinctValuesForAttributes)
	print "distinctValuesForAttributes_B: ",distinctValuesForAttributes_B
	print "mass_B: ", mass_B
	N = len(attributes)
	cardinalities_R = [len(arr) for arr in distinctValuesForAttributes_B]
	cardinalities = copy.deepcopy(cardinalities_R)
	currDensity = getDensity(rho, N, cardinalities, mass, cardinalities, mass)
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
		dim = selectDimension(policy, distinctValuesForAttributes_B, mass_B, 
								attributeValuesMass, rho, cardinalities_R, mass)
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
			newDensity = getDensity(rho, N, cardinalities, mass_B, cardinalities_R, mass)
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


def extractBlock(cur, name, attributes, distinctValuesForAttributes_B, block_num):
	where_clause = ''
	for i, distinctValues in enumerate(distinctValuesForAttributes_B):
		where_clause += attributes[i][0] + " IN ('" + "','".join(distinctValues) + "') AND "
	# col_names = ' (blockId, '
	# for attribute in attributes:
	# 	col_names += attribute[0] + ', '
	# col_names += 'count)'
	sql_query = "INSERT INTO " + OUTPUT_TABLE_NAME + " SELECT " + str(block_num) + ", * FROM " + name + " WHERE " + where_clause[:-5]
	print 'SQL', sql_query
	cur.execute(sql_query)
	sql_query = "SELECT SUM(count) FROM " + name + " WHERE " + where_clause[:-5]
	cur.execute(sql_query)
	mass = cur.fetchall()[0][0]
	return mass

def main():
	#-------- SET-UP ----------
	conn, cur = interface.connectDB()
	interface.createInputTable(cur)
	interface.createOutputTable(cur, OUTPUT_TABLE_NAME)
	output = open(OUTPUT_LOCATION, 'w')
	cur.execute("SELECT * FROM input_table limit 10")
	records = cur.fetchall()
	pprint.pprint(records)

	#-------- Relation/Block Definitions ----------
	# dimension = NUM_ATTRIBUTES
	# attributes = findAttributes(cur, 'input_table')
	# distinctValuesForAttributes, cardinalities = findCardinalities(cur, 'input_table', attributes)
	# print distinctValuesForAttributes, cardinalities
	# mass_R = findMass(cur, 'input_table')
	# print "Mass of Relation ", mass_R
	#TODO: Define a block

	"""
	#-------- DENSITY COMPUTATION ----------
	print "Arithmetic Density ", arithmeticDensity(dimension, cardinalities, mass_R)
	print "Geometric Density ", geometricDensity(dimension, cardinalities, mass_R)
	#TODO: Test Suspiciousness
	"""

	#-------- ALGORITHM 1 ----------
	dimension = NUM_ATTRIBUTES
	tableCopy(cur, 'input_table', 'original_input_table')
	attributes = findAttributes(cur, 'input_table')[:-1]
	distinctValuesForAttributes_R, cardinalities_R = findCardinalities(cur, 'input_table', attributes)
	mass_R = findMass(cur, 'input_table')
	block_num = 0
	block_densities = []
	for block_num in xrange(NUM_DENSE_BLOCKS):
		print "******* Iteration number: "+str(block_num)
		mass_R = findMass(cur, 'input_table')
		if (mass_R == 0):
			block_num -= 1
			break;
		distinctValuesForAttributes_R, cardinalities_R = findCardinalities(cur, 'input_table', attributes)
		distinctValuesForAttributes_B = findSingleBlock(cur, 'input_table', 
				distinctValuesForAttributes_R, mass_R, attributes, DENSITY_MEASURE, POLICY)
		cardinalities_B = [len(arr) for arr in distinctValuesForAttributes_B]
		filterTable(cur, 'input_table', attributes, distinctValuesForAttributes_B)
		mass_B = extractBlock(cur, 'original_input_table', attributes, distinctValuesForAttributes_B, block_num)
		block_density = getDensity(DENSITY_MEASURE, dimension, cardinalities_B, mass_B, cardinalities_R, mass_R)
		print "BLOCK DENSITY", block_density
		block_densities.append(block_density)
		writeBlockDensity(output, block_density, block_num)

	sys.stdout.write("Number of dense blocks found: " + str(block_num+1) + '\n')
	cur.execute("SELECT * FROM " + OUTPUT_TABLE_NAME + "")
	records = cur.fetchall()
	pprint.pprint(records)
	#-------- CLEAN-UP ----------
	#Drop copied table
	output.close()
	interface.dropTable(cur)
	interface.closeDB(cur, conn)
    
if __name__ == "__main__":
    main()
