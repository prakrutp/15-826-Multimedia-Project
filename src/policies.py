import copy
from density import *
from block_functions import *

def selectDimensionbyDensity(conn, inpDistinctValuesForAttributes, mass, rho, cardinalities_R, mass_R):
	currDensity = -1
	dimFinal = 0
	cur = conn.cursor()
	cur1 = conn.cursor()
	new_table = 'distinctValuesForAttributes_ds'

	for dim in xrange(len(cardinalities_R)):
		mass_updated = copy.deepcopy(mass)
		tableCopy(conn, inpDistinctValuesForAttributes, new_table)
		cardinalities = findCardinalitiesFromTable(conn, new_table, len(cardinalities_R))

		if cardinalities[dim] != 0:
			sql_query = "UPDATE " + new_table + " SET attr_flag = false"
			cur.execute(sql_query)
			massThreshold = (float(mass_updated))/(float(cardinalities[dim]))
			sql_query = "UPDATE " + new_table + " SET attr_flag = true WHERE attr_no = " + str(dim) + " and attr_mass <= " +str(massThreshold)
			cur.execute(sql_query)

			sql_query = "SELECT * FROM " + new_table + " WHERE attr_no = " + str(dim) + " and attr_flag = true"
			cur.execute(sql_query)
			for i, candidate in enumerate(cur):
				mass_updated -= int(candidate[2])
				sql_query = "DELETE FROM " + new_table + " WHERE attr_no = " + str(dim) + " and attr_val = '"+ candidate[1] + "'"
				cur1.execute(sql_query)
				cardinalities[dim] -= 1
				newDensity = getDensity(rho, len(cardinalities), cardinalities, mass_updated, cardinalities_R, mass_R)
			if newDensity >= currDensity:
				currDensity = newDensity
				dimFinal = dim
	cur.execute("DROP TABLE IF EXISTS " + new_table)
	cur1.close()
	cur.close()
	return dimFinal

def selectDimensionbyCardinality(cardinalities):
	maxIndex = -1
	maxValue = -1
	for i, cardinality in enumerate(cardinalities):
		if cardinality > maxValue:
			maxValue = cardinality
			maxIndex = i
	return maxIndex

def selectDimension(conn, policy, cardinalities_B, distinctValuesForAttributes, mass, 
					rho, cardinalities_R, mass_R):
	if policy == 'C':
		return selectDimensionbyCardinality(cardinalities_B)
	elif policy == 'D':
		return selectDimensionbyDensity(conn, distinctValuesForAttributes, mass, 
										rho, cardinalities_R, mass_R)
	sys.exit("Error: Policy Not Known\n")

