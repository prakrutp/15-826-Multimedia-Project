import sys

def readlines(fname):
	fid = open(fname, 'r')
	lines = fid.readlines()
	fid.close()
	return lines

def block_to_csv():
	blockToCSV = {}
	i = 0
	for line in lines:
		i+=1
		line = line.strip('\n')
		arr = line.split(',')
		for j in range(len(arr)):
			arr[j] = int(arr[j].strip(' '))
			if arr[j]!=0:
				blockToCSV[(i,j+1)] = arr[j]

	fid = open(sys.argv[2], 'w')
	for key in blockToCSV:
		fid.write(str(key[0])+','+str(key[1])+','+str(blockToCSV[key])+'\n')
	fid.close()


def csv_to_block(lines, fname):
	maxx = -1
	maxy = -1
	csvToBlock = {}
	for line in lines:
		line = line.strip('\n')
		arr = line.split(',')
		blockid = int(arr[0]) + 1
		x = int(arr[1])-1
		y = int(arr[2])-1
		if x>maxx:
			maxx = x
		if y>maxy:
			maxy = y
		if csvToBlock.has_key(blockid):
			csvToBlock[blockid].append((x,y))
		else:
			csvToBlock[blockid] = [(x,y)]
	fid = open(fname, 'w')
	for blockid in csvToBlock:
		block = []
		for i in range(maxx+1):
			block.append([0]*(maxy+1))
		for key in csvToBlock[blockid]:
			block[key[0]][key[1]] = blockid	
		for key in block:
			fid.write(str(key).strip('[').strip(']')+'\n')
		fid.write('\n\n')
	fid.close()
		
def run_test(testn, casen):
	direc = '../output/test'+testn+'/test'+testn+'_'
	lines = readlines(direc+casen+'.csv')
	csv_to_block(lines,direc+casen+'_dbs.csv')

run_test('1','CA')
run_test('1','CG')
run_test('1','CS')
run_test('1','DA')
run_test('1','DG')
run_test('1','DS')

run_test('2','CA')
run_test('2','CG')
run_test('2','CS')
run_test('2','DA')
run_test('2','DG')
run_test('2','DS')

run_test('3','CA')
run_test('3','CG')
run_test('3','CS')
run_test('3','DA')
run_test('3','DG')
run_test('3','DS')

run_test('4','CA')
run_test('4','CG')
run_test('4','CS')
run_test('4','DA')
run_test('4','DG')
run_test('4','DS')

run_test('5','CA')
run_test('5','CG')
run_test('5','CS')
run_test('5','DA')
run_test('5','DG')
run_test('5','DS')













