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

def extractBlock(cur, name, attributes, distinctValuesForAttributes_B, block_num):
	where_clause = ''
	for i, distinctValues in enumerate(distinctValuesForAttributes_B):
		where_clause += attributes[i][0] + " IN ('" + "','".join(distinctValues) + "') AND "
	sql_query = "INSERT INTO " + OUTPUT_TABLE_NAME + " SELECT " + str(block_num) + ", * FROM " + name + " WHERE " + where_clause[:-5]
	cur.execute(sql_query)
	sql_query = "SELECT SUM(count) FROM " + name + " WHERE " + where_clause[:-5]
	cur.execute(sql_query)
	mass = cur.fetchall()[0][0]
	return mass

#-------- ALGORITHM 2 ----------
def findSingleBlock(cur, name, distinctValuesForAttributes, mass, attributes, rho, policy):
	tableCopy(cur, name, 'block')
	mass_B = copy.deepcopy(mass)
	distinctValuesForAttributes_B = copy.deepcopy(distinctValuesForAttributes)
	# print "distinctValuesForAttributes_B: ",distinctValuesForAttributes_B
	# print "mass_B: ", mass_B
	N = len(attributes)
	cardinalities_R = [len(arr) for arr in distinctValuesForAttributes_B]
	cardinalities = copy.deepcopy(cardinalities_R)
	currDensity = getDensity(rho, N, cardinalities, mass, cardinalities, mass)
	# print "cardinalities: ",cardinalities
	# print "currDensity: ", currDensity

	r = 1
	rFinal = 1
	order = dict()

	iterno = 0
	while(checkEmptyCardinalities(distinctValuesForAttributes_B)):
		iterno +=1
		# print "++++++ Iter no of while loop: ",iterno
		attributeValuesMass = getAttributeValuesMass(cur, 'block', attributes, distinctValuesForAttributes_B)
		# print "attributeValuesMass: ", attributeValuesMass
		dim = selectDimension(policy, distinctValuesForAttributes_B, mass_B, 
								attributeValuesMass, rho, cardinalities_R, mass)
		# print "Selected dimension: ",dim
		if not order.has_key(dim):
			order[dim] = dict()
		removeCandidates = []
		D = attributeValuesMass[dim]
		massThreshold = (float(mass_B))/(float(len(distinctValuesForAttributes_B[dim])))
		for val, valMass in sorted(D.items(), key=lambda x: x[1]):
			if valMass <= massThreshold:
				removeCandidates.append(val)
		# print "Removed candidates: ", removeCandidates
		# print "MassThreshold: ", massThreshold
		for i, candidate in enumerate(removeCandidates):
			# print "------ Iter no of for loop: ",i
			mass_B -= attributeValuesMass[dim][candidate]
			distinctValuesForAttributes_B[dim].remove(candidate)
			cardinalities[dim] -= 1
			newDensity = getDensity(rho, N, cardinalities, mass_B, cardinalities_R, mass)
			# print "Mass: ", mass_B
			# print "distinctValuesForAttributes_B: ", distinctValuesForAttributes_B
			# print "newDensity: ", newDensity
			order[dim][candidate] = r
			r += 1
			if newDensity > currDensity:
				# print "Updating......."
				currDensity = newDensity
				rFinal = r
		sql_query = "DELETE FROM block WHERE " + attributes[dim][0] + " IN ('" + "','".join(removeCandidates) + "')"
		cur.execute(sql_query)
		# print "Order: ", order

	newBlock = []
	for i in xrange(N):
		newBlockAttribute = []
		for k, v in order[i].iteritems():
			if v >= rFinal:
				newBlockAttribute.append(k)
		newBlock.append(newBlockAttribute)
	# print "newBlock: ", newBlock
	return newBlock

def main():
	#-------- SET-UP ----------
	conn, cur = interface.connectDB()
	interface.createInputTable(cur)
	interface.createOutputTable(cur)
	output = open(OUTPUT_LOCATION, 'w')
	# cur.execute('SELECT * FROM input_table limit 10')
	# records = cur.fetchall()
	# pprint.pprint(records)
	sys.stdout.write("Finding " + str(NUM_DENSE_BLOCKS) + ' blocks\nDensity Measure Used: ' + DENSITY_MEASURE + '\nPolicy Used: ' + POLICY + '\n')

	#-------- ALGORITHM 1 ----------
	dimension = NUM_ATTRIBUTES
	tableCopy(cur, 'input_table', 'original_input_table')
	attributes = findAttributes(cur, 'input_table')[:-1]
	distinctValuesForAttributes_R, cardinalities_R = findCardinalities(cur, 'input_table', attributes)
	mass_R = findMass(cur, 'input_table')
	block_num = 0; block_densities = []

	for block_num in xrange(NUM_DENSE_BLOCKS):
		print "******* Block number: "+str(block_num)
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
	output.close()
	interface.dropTable(cur, ['input_table', 'original_input_table'])
	interface.closeDB(cur, conn)
    
if __name__ == "__main__":
    main()
