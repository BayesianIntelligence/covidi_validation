import _env, os, argparse
import build_progression, train_progression, make_sample_sets, make_casefile, net_comp, interpolator
from bni_netica import *
from ow_utils import *

def saveJson(obj, outFn):
	with open(outFn, 'w') as outFile:
		json.dump(obj, outFile)

def runProgression(windowSize):
	try:
		os.remove('bns/cptsetup.json')
	except:
		pass

	make_sample_sets.main(windowSize)
	make_casefile.makeCaseFile('out/progression_training.csv')
	
	for i in range(3):
		print('iteration', i)
		
		build_progression.build()
		matchLayout('bns/progression.dne', 'bns/template_iddo.dne')

		train_progression.train('bns/progression.dne', 'out/progression_training.csv')
		matchLayout('bns/progression.trained.dne', 'bns/template_iddo.dne')
		
		net1 = Net('bns/progression.dne')
		net2 = Net('bns/progression.trained.dne')

		saveJson(net_comp.compareNets(net1, net2), 'out/compare.json')
		saveJson(interpolator.fitSetups(net2), 'bns/cptsetup.json')
		
		
		# dne2xdsl('bns/progression.dne')
		# dne2xdsl('bns/progression.trained.dne')
		
		
	try:
		os.remove(f'bns/progression{windowSize}.dne')
	except:
		pass
	os.rename('bns/progression.dne', f'bns/progression{windowSize}.dne')
	
	try:
		os.remove(f'bns/progression{windowSize}.trained.dne')
	except:
		pass
	os.rename('bns/progression.trained.dne', f'bns/progression{windowSize}.trained.dne')
	
runProgression(3)
runProgression(5)
runProgression(10)
# runProgression(20)
	
	
# net = Net('bns/progression.trained_10.dne')
# print(interpolator.fitSetup(net.node('ci_func_pul_t0')))
