import time
from statistics import mean
from bni_netica import Net
from ow_utils import *

def nextStates (states, nodes):
	for i in reversed(range(0,len(nodes))):
		states[i] = states[i] + 1
		if states[i] < nodes[i].numberStates():
			return False
		states[i] = 0
	return True
	
def compareNodes(node1, node2):
	cpt1 = node1.cpt()
	cpt2 = node2.cpt()
	
	cpds = []
	parents = node1.parents()
	if parents:
		parentStates = [0]*len(parents)
		parentStates[len(parents)-1] = -1
		row = 0
		while not nextStates(parentStates,parents):
			cpds.append({'cpd': ', '.join([node.state(state).name() for state, node in zip(parentStates, parents)]), 'kl': round(klDiv(cpt1[row],cpt2[row]),4)})
			row += 1
	else:
		cpds.append({'cpd':'', 'kl': round(klDiv(cpt1[0],cpt2[0]),4)})
		
	return {'node': node1.name(), 'kl': round(mean([cpd['kl'] for cpd in cpds]),4), 'cpt': cpds}
	

def compareNets(net1, net2):
	print('Making Comparison File')
	t = time.time()
	nodes = [compareNodes(n1, n2) for n1, n2 in zip(net1.nodes(), net2.nodes())]
	print(f'Done ({round(time.time()-t,2)}s)')
	return {'kl': round(mean([node['kl'] for node in nodes])), 'nodes': nodes} 


if __name__ == "__main__":
	print(json.dumps(compareNets(Net('progression5.dne'), Net('progression10.dne')), indent=2))
	
	