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

def getDensity(rho, N, cardinalities, mass):
    if rho == 'A':
        return arithmeticDensity(N, cardinalities, mass)
    else if rho == 'G':
        return geometricDensity(N, cardinalities, mass)
    else if rho == 'S':
        #TODO
        return 0.0
    else:
        sys.exit("Error: Density Measure Not Known\n")