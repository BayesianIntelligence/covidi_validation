import _env, csv, math, random, json, time, re, traceback, sys, os, pathlib
from bni_netica import Net

toRun = None
if len(sys.argv)>1:
	toRun = sys.argv[1:]

def openNet(fn):
	return Net(fn)
	
def missing(val):
	return val == '*'
	
def readData(csvFn, shuffle = False):
	with open(csvFn) as csvFile:
		inCsv = csv.DictReader(csvFile)
		
		data = [row for row in inCsv]
		
		if shuffle:
			cols = {k:[] for k in data[0].keys()}
			for row in data:
				for k in cols.keys():
					cols[k].append(row[k])
			
			for k in cols.keys():  random.shuffle(cols[k])
			
			for i,row in enumerate(data):
				for k in cols.keys():
					row[k] = cols[k][i]
		
		return data
		
def generateTargetDistribtions(net, data, targets = [], subset = None, fixedEvidence = None):
	tm = time.time()
	
	# Filter to targets that are in both the net and the data
	targets = [t for t in targets if net.node(t) and t in data[0]]
	
	# If None, just use all variables in net
	if subset is None:
		subset = [n.name() for n in net.nodes()]
	# print(subset)
	
	distros = {t: [] for t in targets}
	
	# If None, just use all variables in net
	if subset is None:
		subset = [n.name() for n in net.nodes()]
	# Filter down to just common variables in both net and data
	subset = [n for n in subset if n in data[0] and net.node(n)]
	
	oldAu = net.autoUpdate()
	net.autoUpdate(False)
	
	for row in data:
	
		for targetI,target in enumerate(targets):
			targetNode = net.node(target)
			
			# Skip missing values for target (but not other things)
			if not targetNode.state(row[target], case=False) :  continue
			
			if fixedEvidence:
				for k,v in fixedEvidence.items():
					net.node(k).finding(v)
			else:
				fixedEvidence = {}
		
			# Set evidence nodes (except target and fixedEvidence)
			for k in subset:
				if k == target or k in fixedEvidence or missing(row[k]):  continue
				net.node(k).finding(row[k])
			
			net.update()
			targetBeliefs = targetNode.beliefs()
			#net.write('temp.dne')
			
			# Actual target value
			targetActual = row[target]
			targetActualI = net.node(target).state(targetActual, case=False).stateNum

			# Record the actual value of the target, plus the predicted distribution over the target
			# (This should allow almost any offline metric of the performance for that target under the tested scenario.)
			distros[target].append({'beliefs':[round(f,4) for f in targetBeliefs], 'trueState': targetActualI})
			
			net.retractFindings()
	
	net.autoUpdate(oldAu)
	
	#print(f'GenerateTargetDistribtions Time: {round(time.time() - tm,2)}s')
	
	return distros
	
def generateMetrics(netFn, distros, targetPosStates = None):
	tm = time.time()
	net = Net(netFn)
	
	# Filter to targets that are in both the net and the data
	# targets = [t for t in targets if net.node(t) and t in data[0]]
	
	results = {t: {'count': 0, 'accuracy': 0, 'tp': 0, 'tn': 0, 'fp': 0, 'fn': 0, 'log': 0, 'brier': 0} for t in distros}
	
	#for target in distros:
	for targetI,target in enumerate(distros):
		for res in distros[target]:
			targetBeliefs = res['beliefs']
			targetActualI = res['trueState']
			results[target]['count'] += 1
			
			# Predictive accuracy
			maxBelief = max(targetBeliefs)
			maxIndex = targetBeliefs.index(maxBelief) # Predicted state
			#print(maxIndex,targetActualI,maxBelief)
			if targetActualI == maxIndex:
				results[target]['accuracy'] += 1
			
			# Confusion matrix
			if targetPosStates:
				# What counts as the "positive" state for this node?
				posState = targetPosStates[targetI]
				posState = net.node(target).state(posState, case=False).stateNum
				
				if maxIndex == posState:
					if targetActualI == maxIndex:
						results[target]['tp'] += 1
					else:
						results[target]['fp'] += 1
				else:
					if targetActualI == posState:
						results[target]['fn'] += 1
					else:
						results[target]['tn'] += 1
				
			# Log score
			results[target]['log'] += -math.log(max(0.0000001,targetBeliefs[targetActualI]))
			
			# Brier score (multi-category)
			score = 0
			for i,b in enumerate(targetBeliefs):
				trueState = (targetActualI == i)
				score += (b - trueState)**2
				
			results[target]['brier'] += score
			
	
	# Normalise/average results
	for target,targetRes in results.items():
		for meas, measVal in targetRes.items():
			if meas in ['count','tp','fp','tn','fn']:  continue
			
			targetRes[meas] = round(measVal/targetRes['count'],4) if targetRes['count'] else '-'
	
	#print(f'GenerateMetrics Time: {round(time.time() - tm,2)}s')
	
	return results



def validate(netFn, data, targets = [], targetPosStates = None, subset = None, fixedEvidence = None):
	net = Net(netFn)
	targetDistros = generateTargetDistribtions(net, data, targets)
	results = generateMetrics(netFn, targetDistros, targetPosStates)
	return results, targetDistros
	
	
def crossValidate(netFn, data, targets = [], targetPosStates = None, subset = None, fixedEvidence = None, folds = 10, netTrainer = None):
	
	def trainNet(net, dataFn):
		net.learn(dataFn, randomize=net.nodes())
		return net
	
	if not netTrainer:
		print('using default net trainer')
		netTrainer = trainNet
	
	cvTargetDistros = {t: [] for t in targets}
	
	t = time.time()
	for fold in range(folds):
		foldSize = int(len(data)/folds)
		testStart = fold*foldSize
		testEnd = (fold+1)*foldSize
		# print({'foldSize':foldSize, 'testStart':testStart,'testEnd':testEnd,'lendata':len(data)})
		testSplit = data[testStart:testEnd]
		trainSplit = data[:testStart] + data[testEnd:]
		#trainSplit = data
		
		testData = testSplit
		trainData = trainSplit
		trainDataFn = 'data/temp_train.csv'
		with open(trainDataFn, 'w', newline='') as trainDataFile:
			outCsv = csv.DictWriter(trainDataFile, list(trainData[0].keys()), quoting=csv.QUOTE_ALL)
			outCsv.writeheader()
			outCsv.writerows(trainData)
		# print(f'{time.time()-t}s')
		
		# Do training
		net = openNet(netFn)
		net = netTrainer(net, trainDataFn)
		
		targetDistros = generateTargetDistribtions(net, testData, targets, subset, fixedEvidence)
		for target in targetDistros:
			cvTargetDistros[target] += targetDistros[target]
		#print(cvTargetDistros)
		
	cvResults = generateMetrics(netFn, cvTargetDistros, targetPosStates)
	
	print(f'CV Time: {round(time.time()-t,3)}s')
	return cvResults, cvTargetDistros

