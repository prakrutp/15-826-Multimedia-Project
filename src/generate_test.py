from random import *

def get_dense_block(x,y,z):
	data = []
	for i in range(x):
		datatmp = []
		for j in range(y):
			datak = [0.00001]*range(k)
			datatmp.append(datak)
		data.append(datatmp)
	for i in range(1:4):
		for j in range(1:7):
			for k in range(1:11):
				if random()<0.9:
					data[i][j][k] = random()*100.0
	for i in range(:4):
		for j in range(1:7):
			for k in range(1:11):
				if random()<0.9:
					data[i][j][k] = random()*100.0