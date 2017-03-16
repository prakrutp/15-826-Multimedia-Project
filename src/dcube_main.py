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
	# return mass/sumOfCardinalities
	return mass/(sumOfCardinalities/dimension)

def geometricDensity(dimension, cardinalities, mass):
	productOfCardinalities = 1.0
	for cardinality in cardinalities:
		productOfCardinalities *= cardinality
	if productOfCardinalities == 0.0:
		return -1
	# return mass/productOfCardinalities
	return mass/math.pow(productOfCardinalities, 1.0/dimension)

def suspiciousnessDensity(dimension, cardinalities_B, mass_B, cardinalities_R, mass_R):
	productOfCardinalities_B = 1.0
	productOfCardinalities_R = 1.0
	for i, cardinality_R in enumerate(cardinalities_R):
		productOfCardinalities_R *= cardinality_R
		productOfCardinalities_B *= cardinalities_B[i]
	if productOfCardinalities_R == 0 or productOfCardinalities_B == 0 or mass_B == 0 or mass_R == 0:
		return - 1;
	return (mass_B * (math.log(mass_B/ float(mass_R) - 1)) 
		+ mass_R * (productOfCardinalities_B / productOfCardinalities_R) 
		- mass_B * math.log(productOfCardinalities_B / productOfCardinalities_R))


def main():
	#-------- SET-UP ----------
	conn, cur = interface.connectDB()
	interface.createInputTable(cur)
	cur.execute("SELECT * FROM input_table")
	records = cur.fetchall()
	pprint.pprint(records)

	#-------- Relation/Block Definitions ----------
	dimension = NUM_ATTRIBUTES
	cardinalities = []
	cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='input_table'");
	attributes = cur.fetchall()
	X = attributes[-1][0]    #Assuming last attribute is X
	for attribute in attributes[:-1]:
		cur.execute("SELECT COUNT(DISTINCT " + attribute[0] +") FROM input_table");
		cardinalities += [int(cur.fetchall()[0][0])]

	cur.execute("SELECT SUM( "+ X +") FROM input_table");
	mass_R = int(cur.fetchall()[0][0])
	print "Mass of Relation ", mass_R
	#TODO: Define a block


	#-------- DENSITY COMPUTATION ----------
	print "Arithmetic Density ", arithmeticDensity(dimension, cardinalities, mass_R)
	print "Geometric Density ", geometricDensity(dimension, cardinalities, mass_R)
	#TODO: Test Suspiciousness

	#-------- CLEAN-UP ----------
	interface.dropTable(cur)
	interface.closeDB(cur, conn)
    
if __name__ == "__main__":
    main()
