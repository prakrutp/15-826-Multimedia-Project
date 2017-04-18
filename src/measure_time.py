import os
from math import *
from numpy import *
import sys

fidfinal = open(sys.argv[1],'w')
for dm in ['A','G','S']:
	for pol in ['C', 'D']:
		times = []
		fid1 = open('constants.py','w')
		fid2 = open('constantsref.py','r')
		for line in fid2.readlines():
			if 'DENSITY_MEASURE' in line:
				fid1.write('DENSITY_MEASURE = \''+str(dm)+'\'\n')
			elif 'POLICY' in line:
				fid1.write('POLICY = \''+str(pol)+'\'\n')
			else:
				fid1.write(line)
		fid1.close()
		fid2.close()
		times = {}
		for j in range(5):
			times[j] = []
		for i in range(3):
			for j in range(5):
				os.system('python dcube_main'+str(j)+'.py')
				times[j].append(float(line))
		for j in range(5):
			fidfinal.write(dm+','+pol+','+str(j)+','+str(times[j])+','+str(mean(times[j]))+','+str(std(times[j]))+'\n')
fidfinal.close()
