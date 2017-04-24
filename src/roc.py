import sys
import matplotlib.pyplot as plt
import numpy as np
from sklearn import metrics

# [labels, assignment] = sys.argv[1:]
# fid = open(labels, 'r')
# lines = fid.readlines()
# fid.close()

# dataWithLabel = {}
# for line in lines:
#         line = line.strip('\n')
#         arr = line.split(',')
#         inpdata = ','.join(arr[0:3])
#         label = arr[-1]
#         if dataWithLabel.has_key(inpdata):
#                 if label not in dataWithLabel[inpdata]:
#                         dataWithLabel[inpdata].append(label)
#         else:
#                 dataWithLabel[inpdata] = [label]

# fid = open(sys.argv[1], 'r')
# for line in fid.readlines():
#         line = line.strip('\n')
#         arr = line.split(',')
#         inpdata = ','.join(arr[1:4])
#         sys.stdout.write(line+','+':'.join(dataWithLabel[inpdata])+'\n')

y = np.array([1,1,1,1,1,0,0,0,0,0,0])
y = np.array([1,0,1,0,1,0,1,0,1,0,1])
fpr, tpr, thresholds = metrics.roc_curve(y, scores, pos_label=1)

# This is the ROC curve
plt.plot(fpr,tpr)
plt.show() 

# This is the AUC
# auc = np.trapz(y,x)