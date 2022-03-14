import _env, time, os, csv, re, bndict, sys, ow_utils

from bni_netica import Net
import bni_netica, bni_smile
from ow_utils import *

def printLatent(net, dataFn):
	with open(dataFn, 'r') as inFile:
		inCsv = csv.DictReader(inFile, delimiter = ',')
		print('latent vars: '+str(list(set([node.name() for node in net.nodes()]) - set(inCsv.fieldnames))))
		
def addStatusConstraint(node):
	cpt = node.cpt()
	parents = node.parents()
	numChildStates = node.numberStates()
	
	parentStates = [0]*len(parents)
	parentStates[len(parents)-1] = -1
	row = 0
	while not nextStates(parentStates,parents):
		if parentStates[0] == 0: cpt[row] = [1,0,0,0]
		elif parentStates[0] == 3: cpt[row] = [0,0,0,1]
		row = row+1
	node.cpt(cpt)
		

def train(netFn, dataFn):
	print('Training BN')
	t = time.time()
	net = Net(netFn)
	printLatent(net, dataFn)
	
	net.experience(1)
	net.learn(dataFn)
	
	addStatusConstraint(net.node('ci_status_t1'))
	addStatusConstraint(net.node('ci_status_plus1day'))
	
	net.write(os.path.splitext(netFn)[0]+'.trained.dne')
	
	print(f'Done ({round(time.time()-t,2)}s)')
	



if __name__ == "__main__":
	train('bns/progression.dne', 'out/progression_training.csv')

	matchLayout('bns/progression.trained.dne', 'bns/template_iddo.dne')
	dne2xdsl('bns/progression.trained.dne')
	