import copy
from density import *

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


