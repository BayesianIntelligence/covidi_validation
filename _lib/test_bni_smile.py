from __future__ import print_function
from bni_smile import *
import time

def mainTests():
	print("[Open NF_V1.dne]")
	myNet = Net("../nets/NF_V1.xdsl")
	print(myNet.xdsl)
	#myNet.write('../nets/NF_V1.xdsl')
	print()

	print("[Set to None (garbage collect)]")
	# This will trigger the deletion
	myNet = None
	print()

	print("[Re-open NF_V1.dne]")
	myNet = Net("../nets/NF_V1.dne")
	print()

	print("Net name:", myNet.name())
	print("[Change name to NFAV1]")
	myNet.name("NFAV1")
	print("Check new net name:", myNet.name())
	print()

	print("Net title:", myNet.title())
	print("[Change title to 'Native Fish A V1']")
	myNet.title("Native Fish A V1")
	print("Check new net title:", myNet.title())
	print()

	print("[Get RiverFlow node]")
	rf = myNet.node("RiverFlow")
	print(rf)

	print("RiverFlow number states:", rf.numberStates())
	print("RiverFlow node beliefs:", rf.beliefs())
	print("FishAbundance node beliefs:", myNet.node("FishAbundance").beliefs())
	print("P(All Evidence):", myNet.findingsProbability())
	print()

	print("[Set RiverFlow = state0]")
	rf.finding(0)
	print("RiverFlow node beliefs|RiverFlow=state0:", rf.beliefs())
	print("FishAbundance node beliefs|RiverFlow=state0:", myNet.node("FishAbundance").beliefs())
	print("New P(Evidence):", myNet.findingsProbability())
	print()
	
	print("[Use likelihood sampling; then set RiverFlow = state0]")
	prevAlgorithm = myNet.updateAlgorithm()
	print("Previous algorithm:", prevAlgorithm)
	myNet.updateAlgorithm(Net.ALG_BN_LSAMPLING)
	print("Algorithm set to:", myNet.updateAlgorithm())
	rf.finding(0)
	print("RiverFlow node beliefs|RiverFlow=state0:", rf.beliefs())
	print("FishAbundance node beliefs|RiverFlow=state0:", myNet.node("FishAbundance").beliefs())
	print("New P(Evidence):", myNet.findingsProbability())
	print()
	myNet.updateAlgorithm(prevAlgorithm)

	print("[Clear findings]")
	myNet.node("RiverFlow").retractFindings()
	print("New P(Evidence):", myNet.findingsProbability())
	print()

	print("[Set RiverFlow = state0]")
	rf.finding(0)
	print("RiverFlow node beliefs|RiverFlow=state0:", rf.beliefs())
	print("[Clear all findings]")
	myNet.node("RiverFlow").retractFindings()
	print("RiverFlow node beliefs:", rf.beliefs())
	print()

	print("RiverFlow Virtual Evidence (Likelihoods):", rf.likelihoods())
	print("P(RiverFlow):", rf.beliefs())
	print()

	print("[Set RiverFlow Likelihoods = 0.3,0.2]")
	rf.likelihoods([0.3,0.2])
	print("New P(RiverFlow):", rf.beliefs())
	print("New Virtual Evidence:", rf.likelihoods())
	print()

	print("[Set RiverFlow Likelihoods = 0.4,0.2]")
	rf.likelihoods([0.4,0.2])
	print("New P(RiverFlow):", rf.beliefs())
	print("New Virtual Evidence:", rf.likelihoods())
	print()

	print("[Create node called TestA]")
	node = Node(myNet, "TestA")
	print()

	print("TestA states:", node.stateNames())
	print()
	
	print("[Add state called 'three']")
	node.addState('three')
	print("TestA states:", node.stateNames())
	print()

	print("[Rename state0 to 'one']")
	node.renameState(0, 'one')
	print("TestA states:", node.stateNames())
	print()

	print("[Rename all states to one,two,three]")
	node.renameStates(['one','two','three'])
	print("TestA states:", node.stateNames())
	print()

	print("[Create node TestB with one state called 'a'")
	print(" (fails in GeNIe because doesn't allow it")
	print(" --- i.e. creates node with 2 states, wrongly named)]")
	node = Node(myNet, "TestB", ['a'])
	print("TestB states:", node.stateNames())
	print()

	print("[Create node TestC with 3 states, called a,b,c]")
	node = Node(myNet, "TestC", ['a','b','c'])
	print("TestC states:", node.stateNames())
	print()

	print("TestC CPT:", node.cpt1d())
	print()

	print("RiverFlow CPT:", rf.cpt1d())
	print()

	print("[Set TestC CPT with 0.3,0.3,0.3. Should give 1/3,1/3,1/3]")
	node.cpt1d([0.3,0.3,0.3])
	print("New TestC CPT:", node.cpt1d())
	print()

	print("[Add RiverFlow as parent to TestC]")
	node.addParents(["RiverFlow"])
	print("[Add FishAbundance as child of TestC]")
	node.addChildren(["FishAbundance"])
	print()

	print("1D CPT:", node.cpt1d())
	node.cpt1d([0.1,0.3,0.7,0.2,0.1,0.1])
	print("[Set TestC CPT to [0.1,0.3,0.7,0.2,0.1,0.1] using 1D array]")
	print("New 1D CPT:", node.cpt1d())
	print()

	print("2D CPT:", node.cpt())
	node.cpt([[0.1,0.3,0.2],[0.2,0.4,0.4]])
	print("[Set TestC CPT to [[0.1,0.3,0.2],[0.2,0.4,0.4]] using 2D array]")
	print("New 2D CPT:", node.cpt())
	print("New 1D CPT:", node.cpt1d())
	print()

	print("[Run through all nodes, and print names and titles]")
	for node in myNet.nodes():
		print(node.name(), node.title())
	print()

	print("[Run through all parents of FishAbundance, print names and titles]")
	for node in myNet.node("FishAbundance").parents():
		print(node.name(), node.title())
	print()
	
	print("[Print FishAbundance comment]")
	print(myNet.node("FishAbundance").comment())
	print("[Change and print comment]")
	myNet.node("FishAbundance").comment("This is a new comment")
	print(myNet.node("FishAbundance").comment())
	print()

	print("RiverFlow's visual position (x, y):", rf.position())
	print("[Set RiverFlow's visual position to 120,400]")
	print("RiverFlow's visual position:", rf.position(120,400).position())
	print()

	print("RiverFlow's visual size (width, height):", rf.size())
	print("[Set RiverFlow's visual size to 100,100]")
	print("RiverFlow's visual size:", rf.size(100,100).size())
	print()

	fap = myNet.node("FishAbundance").parents()
	print("Combinations of parent states for FishAbundance: ", myNet.numberCombinations(fap))

	print("Parent state combinations for FishAbundance:")
	parentIndexes = [0]*len(fap)
	while 1:
		print([fap[i].state(pi).name() for i,pi in enumerate(parentIndexes)])
		if not myNet.nextCombination(parentIndexes, fap): break

	print("[Write net to file called ../nets/output_NF_V1_test.dne]")
	myNet.write("../nets/output_NF_V1_test.dne")
	myNet.write("../nets/output_NF_V1_test.xdsl")
	print()


def specialTests():
	print("[Testing Netica5 .dne]")
	net = Net("../nets/netica5parens.dne")
	
	net.write("../nets/netica5parens-from-smile.dne")

def equationTests():
	print("[Test equations]")
	net = Net('../nets/equations.xdsl')
	print('[Get equation for node "A"]')
	print(net.node('A').equation())
	'''print "[Add ordinary nature node and print beliefs]"
	node = net.addNode("TestNature")
	print node.beliefs()'''
	
	print("[Create equation node]")
	node = net.addNode("TestEquation", nodeType=Node.EQUATION_NODE)
	node.setEquation("TestEquation=1")
	print(node)
	print()
	
	net.node("TestEquation").setEquation("TestEquation=Uniform(0,1)")
	net.update()
	print("Uniform mean, SD:", net.node("TestEquation")._equationMean(), net.node("TestEquation")._equationSd())
	
	net.node("TestEquation").setEquation("TestEquation=Poisson(3)")
	net.update()
	print("[Poisson lambda=3]")
	print("Poisson mean, SD:", net.node("TestEquation")._equationMean(), net.node("TestEquation")._equationSd())
	
	#net.write('test-equation2.xdsl')
	
	#print node.beliefs()
	#net.update()
	#print(node._equationMean())
	
	#print()

def utilityTests():
	print("[Decision/Utility tests]")
	net = Net()
	
	print("[Create nature node PesticideInRiver]")
	pestNode = net.addNode("PesticideInRiver", states = ["True", "False"])
	pestNode.cpt1d([0.3,0.7])
	
	print("[Create utility node as child of PesticideInRiver]")
	utilNode = net.addNode("Util", Node.UTILITY_NODE)
	utilNode.addParents([pestNode])
	
	print("[Set utility node table]")
	utilNode.utilities([
		0.2, # PesticideInRiver=True
		8    # PesticideInRiver=False
	])
	
	print("[Get utility node table]")
	print(utilNode.utilities())
	
	print("[Get expected value of node]")
	print(utilNode.expectedValues())
	
	print("[Add decision node UsePesticide as parent of PesticideInRiver]")
	usePestNode = net.addNode("UsePesticide", states = ["Yes","No"])
	#print usePestNode.options()
	usePestNode.addChildren(["PesticideInRiver"])
	
	print("[Set conditional probabilities on UsePesticide]")
	pestNode.cpt1d([
		0.7,0.3, # UsePesticide = True
		0.1,0.9  # UsePesticide = False
	])
	print(pestNode.beliefs())
	
	print(usePestNode.numberStates())
	print("[Get expected values for each option]")
	print(utilNode.expectedValues())
	
	#net.write('test-utility.xdsl')
	
def submodelTests():
	print('[Submodels]')
	
	net = Net()
	net.addNode('Treatment', states = ['Yes', 'No'])
	
	print('[Create a submodel]')
	humanBody = net.addSubmodel('HumanBody')
	healthyNode = humanBody.addNode('Healthy', states = ['Yes', 'No']).position(200,200)
	humanBody.position(400,400).size(250,250)
	
	print('[Create a submodel in a submodel]')
	lungs = humanBody.addSubmodel('Lungs').position(300,200)
	lungs.addNode('Infection', states = ['Yes', 'No']).position(200,200)
	print('[And another]')
	heart = humanBody.addSubmodel('Heart').position(380, 200)
	heart.addNode('Pumping', states = ['Yes','No'])
	print('[Print Heart\'s parent submodel name (i.e. HumanBody)]')
	print(heart.parentSubmodel().name())
	print('[Print Treatment and HumanBody\'s parent submodels (None) and Lungs parent submodel (should be submodel object)]')
	print('Treatment parent submodel:', net.node('Treatment').parentSubmodel())
	print('HumanBody parent submodel:', humanBody.parentSubmodel())
	print('Lungs parent submodel:', lungs.parentSubmodel())
	print('[Add Ventilation node to Heart, move to Lungs]')
	vent = heart.addNode('Ventilation', states = ['Yes','No']).position(380, 200)
	vent.parentSubmodel(lungs)
	vent.parentSubmodel('Lungs')
	print('[Print Ventilation parent submodel]')
	print(vent.parentSubmodel().name())
	print('[Add Alveoli submodel to Heart, move to Lungs, add ventilation to it]')
	alveoli = heart.addSubmodel('Alveoli')
	alveoli.parentSubmodel(lungs).position(380, 200)
	alveoli.parentSubmodel('Lungs')
	vent.parentSubmodel(alveoli)
	
	print('[List submodels (all, then submodel only)]')
	print([s.name() for s in net.submodels()])
	print([s.name() for s in net.submodels(submodelOnly=True)])
	
	print('[List nodes in net (all, then submodel only)]')
	print([n.name() for n in net.nodes()])
	print([n.name() for n in net.nodes(submodelOnly=True)])
	
	print('[List nodes in submodel (all, then submodel only)]')
	print([n.name() for n in net.getSubmodel('HumanBody').nodes()])
	print([n.name() for n in net.getSubmodel('HumanBody').nodes(submodelOnly=True)])
	
	print('[Delete submodel Heart]')
	net.getSubmodel('Heart').delete()
	print('Remaining submodels:', [n.name() for n in net.submodels()])
	
	
	
	net.write('../nets/submodel1.xdsl')
	

mainTests()
#specialTests()
equationTests()
utilityTests()
submodelTests()

print('\nDone')