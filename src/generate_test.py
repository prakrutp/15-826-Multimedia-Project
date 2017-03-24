from random import *

def get_dense_block(x,y,z):
	data = []
	for i in range(x):
		datatmp = []
		for j in range(y):
			datak = [0]*(z)
			datatmp.append(datak)
		data.append(datatmp)
	print 'prelim created'
	for i in range(1,4):
		for j in range(1,7):
			for k in range(1,11):
				if random()<0.9:
					data[i][j][k] = 1
	print 'creating first dense block'
	for i in range(3,75):
		for j in range(4,62):
			for k in range(8,137):
				if random()<0.8:
					data[i][j][k] = 1
	print 'creating second dense block'
	return data

def print_dense_block(data,x,y,z,fid):
	for i in range(x):
		for j in range(y):
			for k in range(z):
				if data[i][j][k]!=0:
					fid.write(str(i)+','+str(j)+','+str(k)+','+str(data[i][j][k])+'\n')

data = get_dense_block(100,150,170)
fid = open('../data/test_threeway_modified.csv','w')
print_dense_block(data,100,150,170,fid)
fid.close()