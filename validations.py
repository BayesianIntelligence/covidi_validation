import _env, csv, math, random, json, bni_netica, time, re, traceback, sys, os, pathlib, localconfig
# from bni_smile import Net
from bni_netica import Net

paths, settings = localconfig.setup()

toRun = None
if len(sys.argv)>1:
	toRun = sys.argv[1:]

def openNet(fn):
	return Net(fn)
	# if re.search(r'\.dne', fn):
		# return bni_netica.Net(fn)
	# else:
		# return Net(fn)

def validate(net, data, targets = [], targetPosStates = None, subset = None, fixedEvidence = None):
	'''
	`net` - bni net
	`data` - array of dicts in memory (e.g. as read from csv.DictReader). vars in data not in net or vice versa are skipped.
	`targets` - list of variables to use as test class. missing will be skipped (both if the entire variable is missing from net or data, or just the value in a data row). each target is tested separately.
	`subset` - subset of variables to enter in evidence. if `None`, all variables are used.
	'''
	
	tm = time.time()
	
	# Filter to targets that are in both the net and the data
	targets = [t for t in targets if net.node(t) and t in data[0]]
	
	results = {t: {'count': 0, 'accuracy': 0, 'tp': 0, 'tn': 0, 'fp': 0, 'fn': 0, 'log': 0, 'brier': 0, 'distros': []} for t in targets}
	
	# If None, just use all variables in net
	if subset is None:
		subset = [n.name() for n in net.nodes()]
	# print('subset:',subset)
	# Filter down to just common variables in both net and data
	subset = [n for n in subset if n in data[0] and net.node(n)]
	# print('subset:',subset)
	
	oldAu = net.autoUpdate()
	net.autoUpdate(False)
	
	for row in data:
		for targetI,target in enumerate(targets):
			targetNode = net.node(target)
			# print('target:',targetNode.name(),targetNode.stateNames())
			
			# Skip missing values for target (but not other things)
			if not targetNode.state(row[target], case=False):  continue
			
			if fixedEvidence:
				for k,v in fixedEvidence.items():
					net.node(k).state(v).setTrueFinding()
			else:
				fixedEvidence = {}
		
			# Set evidence nodes (except fixedEvidence)
			for k in subset:
				if k == target or k in fixedEvidence:  continue
				# print(f'Setting: {k} to {row[k]}')
				
				node = net.node(k)
				if node:
					# Assume ints/floats are real values; throw error, if not
					if isinstance(row[k],(int,float)):
						print('hi')
						node.finding(value = row[k])
					else:
						try:
							state = node.state(row[k], case=False)
						except:
							state = None
						# If valid state, set it; otherwise, leave it as missing
						if state:
							state.setTrueFinding()
							# print('set')
			
			net.update()
			targetBeliefs = targetNode.beliefs()
			
			# Actual target value
			targetActual = row[target]
			targetActualI = net.node(target).state(targetActual, case=False).stateNum
			# print(targetActualI, targetActual)
			# print(row)

			# Record the actual value of the target, plus the predicted distribution over the target
			# (This should allow almost any offline metric of the performance for that target under the tested scenario.)
			results[target]['distros'].append({'beliefs':[round(f,4) for f in targetBeliefs], 'trueState': targetActualI})
			
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
			
			net.retractFindings()
	
	net.autoUpdate(oldAu)
	
	# Normalise/average results
	for target,targetRes in results.items():
		for meas, measVal in targetRes.items():
			if meas in ['count','tp','fp','tn','fn','distros']:  continue
			
			targetRes[meas] = measVal/targetRes['count'] if targetRes['count'] else '-'
	
	print(f'Time: {round(time.time() - tm,2)}s')
	
	# Extract the target distros, so they're separate
	targetDistros = {t:res['distros'] for t,res in results.items()}
	for res in results.values():
		del res['distros']
	
	return results, targetDistros

def crossValidate(netFn, data, targets = [], targetPosStates = None, subset = None, folds = 10, trainSize = 1, testSize = 1,
		cacheNets = True, learnParams = {}, netExperience = None, fixedEvidence = None):
	'''
	See validate for other argument definitions.
	
	NOTE: You should randomise the data *before* using this function (if needed), potentially every time (if doing nested CV)
	or just the once.
	
	`folds` - Number of folds to create/run
	`trainSize` - What proportion of the train split to actually use for training (for speed reasons)
	`testSize` - As per trainSize, but for the test set
	`cacheNets` - Cache (or re-use from the cache) learned networks
	'''
	
	cvResults = []
	cvTargetDistros = []
	
	t = time.time()
	for fold in range(folds):
		foldSize = int(len(data)/folds)
		testStart = fold*foldSize
		testEnd = (fold+1)*foldSize
		# print({'foldSize':foldSize, 'testStart':testStart,'testEnd':testEnd,'lendata':len(data)})
		testSplit = data[testStart:testEnd]
		trainSplit = data[:testStart] + data[testEnd:]
		print(f"Size of train/test split: {len(trainSplit)}:{len(testSplit)}")
		#trainSplit = data
		
		testData = testSplit
		trainData = trainSplit
		trainDataFn = paths.temp('temp_train.csv')
		with open(trainDataFn, 'w', newline='') as trainDataFile:
			# SM 2021-09: Quote non-numeric, because Netica can misinterpret final blank as too few fields in that row
			outCsv = csv.DictWriter(trainDataFile, list(trainData[0].keys()), quoting=csv.QUOTE_NONNUMERIC)
			outCsv.writeheader()
			outCsv.writerows(trainData)
		testDataFn = paths.temp('temp_test.csv')
		with open(testDataFn, 'w', newline='') as testDataFile:
			outCsv = csv.DictWriter(testDataFile, list(testData[0].keys()), quoting=csv.QUOTE_NONNUMERIC)
			outCsv.writeheader()
			outCsv.writerows(testData)
		# print(f'{time.time()-t}s')
		
		net = None
		baseNetFn,ext = os.path.splitext(netFn)
		trainNetFn = f'{baseNetFn}_fold{fold}{ext}'
		if cacheNets and os.path.exists(trainNetFn):
			net = openNet(trainNetFn)
		if net is None:
			# Do training
			net = openNet(netFn)
			if netExperience is not None:
				net.experience(netExperience, force = True)
			# print(f'{time.time()-t}s')
			print('learnParams:', learnParams)
			net.write(trainNetFn)
			net.learn(trainDataFn, removeTables=True)
			# print(f'{time.time()-t}s')
			if cacheNets:
				net.write(trainNetFn)
				# input('written train net fn')
				testNetFn = f'{baseNetFn}_testfold{fold}{ext}'
				net2 = openNet(netFn)
				net2.learn(testDataFn, removeTables=True)
				net2.write(testNetFn)
		
		# Do testing
		# print("length:",len(testData))
		res, targetDistros = validate(net, testData, targets, targetPosStates, subset = subset, fixedEvidence = fixedEvidence)
		# print(f'{time.time()-t}s')
		cvResults.append(res)
		cvTargetDistros.append(targetDistros)
		# break

	print(f'CV Time: {round(time.time()-t,3)}s')
	return cvResults, cvTargetDistros
		

def readData(csvFn, shuffle = False, columnShuffle = False):
	'''
	Read data in, ready to be used in the cross-validation.
	
	`csvFn` - The name of the data file, in CSV format
	`shuffle` - Randomly shuffle the rows of the data after reading
	`columnShuffle` - CAUTION: Likely useful for debugging only. Shuffle the column data, so that each column becomes independent
	'''
	with open(csvFn) as csvFile:
		inCsv = csv.DictReader(csvFile)
		
		data = [row for row in inCsv]
		
		if shuffle:
			random.shuffle(data)
		
		if columnShuffle:
			cols = {k:[] for k in data[0].keys()}
			for row in data:
				for k in cols.keys():
					cols[k].append(row[k])
			
			for k in cols.keys():  random.shuffle(cols[k])
			
			for i,row in enumerate(data):
				for k in cols.keys():
					row[k] = cols[k][i]
		
		return data

def validateScenarios(outJsonFn, netFns, data, subsets, targets, targetPosStates, completeOnly = False, cv = False):
	'''
	`cv` - Cross validation on/off. If False, then off. If True, then on (with defaults). If object, then on, with
	extra settings as provided in the object.
	'''
	cachedNets = {fn:openNet(fn) for fn in netFns}
	
	allResults = {}
	numAllRecords = len(data)
	for subsetName, subset in subsets.items():
		# Filter down to complete records only (relative to vars in scenario)
		if completeOnly:
			filteredData = []
			for row in data:
				if sum([row[var] == '*' for var in subset])==0:
					filteredData.append(row)
		else:
			filteredData = data
			
		for netFn in netFns:
			resultsKey = f'{netFn} - {subsetName}'
			#net = Net(netFn)
			def runValidation(data):
				net = cachedNets[netFn]
				print(resultsKey)
				overallResult = None
				try:
					if cv:
						if isinstance(cv, bool):
							cvOpts = {}
						else:
							cvOpts = cv
						# print('CROSS VALIDATING')
						res, targetDistros = crossValidate(netFn, data, targets, targetPosStates, subset = subset, **cvOpts)
					else:
						res, targetDistros = validate(net, data, targets, targetPosStates, subset = subset)
					overallResult = {'res':res,'targetDistros':targetDistros,'numAllRecords': numAllRecords,'numFilteredRecords': len(data)}
					print(json.dumps(res, indent='\t'))
				except:
					print('Error, skipping')
					traceback.print_exc()
				
				return overallResult
			
			allResults[resultsKey] = runValidation(filteredData)
			# if completeOnly:
				# allResults[resultsKey+'_unfiltered'] = runValidation(data)
	
		# Update file after every subset
		with open(paths.out(f'bns/{outJsonFn}'), 'w') as resultsFile:
			resultsFile.write(json.dumps(allResults))
			


from htm import n
import numpy, subprocess

def mergeCvs(resFn, outFn):
	with open(resFn) as resFile:
		results = json.load(resFile)
	
	for key,result in results.items():
		# Merge all the target distros into one big list (one of the recommended ways of doing a ROC for a CV)
		newTargetDistros = {}
		for target in result['targetDistros'][0]:
			newTargetDistros[target] = sum([d[target] for d in result['targetDistros']], [])
		result['targetDistros'] = newTargetDistros
		
		# Average/tally the other stats
		newRes = {}
		firstRes = result['res'][0]
		for target in firstRes:
			for metric in firstRes[target]:
				metricValues = [d[target][metric] for d in result['res']]
				newRes.setdefault(target, {})
				newRes[target][metric] = sum(metricValues)
				if metric not in ['count','tp','fp','tn','fn']:
					newRes[target][metric] /= len(metricValues)
					newRes[target][metric+'_sd'] = numpy.std(metricValues)
		result['res'] = newRes
	
	with open(outFn, 'w') as outFile:
		json.dump(results, outFile)

def convertResults(resFn, outDir):
	with open(resFn) as resFile:
		results = json.load(resFile)

	for key,result in results.items():
		resName = os.path.basename(key) + '.csv'
		
		# Target distros might have no named target (temporary issue)
		try:
			result['targetDistros'][0]
			targets = [None]
		except:
			targets = [t for t in result['targetDistros']]
		
		for target in targets:
			targetDistroSet = result['targetDistros'][target] if target is not None else result['targetDistros']
			with open(os.path.join(outDir,(target+'_' if target else '')+resName), 'w', newline='') as out:
				numStates = len(targetDistroSet[0]['beliefs'])
				fieldNames = ['trueState', *[f'state{i}' for i in range(numStates)]]
				outCsv = csv.DictWriter(out, fieldNames)
				outCsv.writeheader()
				
				rowI = 0
				for row in targetDistroSet:
					outRow = {}
					outRow['trueState'] = row['trueState']
					for i,b in enumerate(row['beliefs']):
						outRow[f'state{i}'] = row['beliefs'][i]
					
					#print(f'Row {rowI}')
					rowI += 1
				
					outCsv.writerow(outRow)

def formatMetricTable(resFn, outFn, fold = None):
	with open(resFn) as resFile:
		results = json.load(resFile)
	header = n('tr',
		n('th', 'Model'),
		n('th', 'Scenario'),
		n('th', 'Information'),
		n('th', 'Accuracy'),
		n('th', 'Log'),
		n('th', 'Brier'),
		n('th', 'PredProb'),
		n('th', 'ROC'),
	)
	tb = n('table.validationResultsTable',
		n('style', '''
			.validationResultsTable { border-collapse: collapse; }
			.validationResultsTable td, .validationResultsTable th { border: solid 1px #ccc; padding: 3px; }
		'''),
		header
	)
	for key,result in results.items():
		# print(key)
		model, scenario = re.split(r'\.(?:dne|xdsl) - ', key)
		firstRes = result['res']
		row = n('tr')
		tb.append(row)
		for target in firstRes:
			rec = result['res'][target]
			row.append([
				n('td', model),
				n('td', scenario),
				n('td',
					n('div',
						n('label', 'Number of all records: '),
						n('span', result['numAllRecords']),
					),
					n('div',
						n('label', 'Number of filtered records: '),
						n('span', result['numFilteredRecords']),
					),
				),
				n('td', f"{rec['accuracy']*100:.1f}", '%'),
				n('td', f"{rec['log']:.3f}"),
				n('td', f"{rec['brier']:.3f}"),
				n('td', f"{math.exp(-rec['log']):.3f}"),
				n('td', n('img', width=200, src='cvs/s_0.'+target+'_'+os.path.basename(key)+'.csv.roc.png')),
			])
	
	#fold = {'rows': 2}
	fold = False
	if fold:
		#cols = fold.get('cols', list(range(1, len(header.content))))
		startCol = 1
		numRows = fold['rows']
		r = 1
		dupCols = header.childNodes()[startCol:]
		for i in range(numRows-1):
			header.append(dupCols)
		r += 1
		while r < len(tb.childNodes()):
			for i in range(numRows-1):
				#print(tb.childNodes())
				row = tb.childNodes()[r+1]
				#print("r,i,t",r,i,len(tb.content), row.content)
				tb.content.remove(row)
				tb.childNodes()[r].append(row.childNodes()[startCol:])
				#tb.content[r].content.extend(row.content[startCol+1:])
		
			r += 1
		
	
	with open(outFn, 'w') as outFile:
		outFile.write(tb.str())


def formatAllResults():
	resultFns = [
		# paths.out('bns/results_dsdecod_nb.json'),
		# paths.out('bns/results_icu_nb.json'),
		# paths.out('bns/results_invvent_nb.json'),
		# paths.out('bns/results_dsdecod_tan.json'),
		# paths.out('bns/results_icu_tan.json'),
		paths.out('bns/results_invvent_tan.json'),
		# paths.out('bns/results_status_prognb.json'),
	]
	for inFn in resultFns:
		mergedInFn = os.path.join(os.path.dirname(inFn), 'merged_'+os.path.basename(inFn))
		tableFn = os.path.join(os.path.dirname(inFn), 'table_'+os.path.basename(inFn)+'.html')
		mergeCvs(inFn, mergedInFn)
		convertResults(mergedInFn, paths.out('bns/cvs', makeDir=True))
		formatMetricTable(mergedInFn,tableFn)
		subprocess.call(['Rscript', 'plotRocs.R', paths.out('bns/cvs/')])
		

def doValidations():
	data = readData(paths.out('baseline_resolvedOnly.csv'), shuffle=True)
	netFns = [
		# paths.out('bns/nb/nb_dsdecod_only.dne'),
		# paths.out('bns/nb/tan_dsdecod_only.dne'),
		# paths.out('bns/nb/tan_icu_only.dne'),
		paths.out('bns/nb/tan_invvent_only.dne'),
		#paths.out('bns/nb/age_diabetes_hypertension_dsdecod.dne'),
		#paths.out('bns/nb/age_crp_dsdecod.dne'),
	]
	subsets = {}
	subsets['None'] = []
	# subsets['AGE'] = ['AGE']
	# subsets['None'] = []
	# # subsets['All'] = None
	# #vars = ['SEX','AGE','ci_vs_bmi','ci_CRP_bl']
	# vars = ['AGE', 'ci_CRP_bl', 'HYPERTENSION', 'DIABETES']
	# for v in vars:
		# subsets[v] = [v]
	# for i,v1 in enumerate(vars):
		# for v2 in vars[i+1:]:
			# subsets[f'{v1},{v2}'] = [v1,v2]
	# subsets['HYPERTENSION'] = ['HYPERTENSION']
	# subsets['DIABETES'] = ['DIABETES']
	# subsets['AGE,DIABETES'] = ['AGE', 'DIABETES']
	# subsets['AGE,HYPERTENSION,DIABETES'] = ['AGE', 'HYPERTENSION', 'DIABETES']
	vars = ['AGE', 'ci_CRP_bl', 'ci_NLR_bl', 'ci_MAP_bl', 'ci_Diastolic_Blood_Pressure_bl', 'ci_Systolic_Blood_Pressure_bl', 'ci_LDH_bl', 'ci_CREAT_bl',
		'ci_Oxygen_Saturation_bl','HYPERTENSION', 'DIABETES', 'CHRONIC_CARDIAC_DISEASE', 'CHRONIC_KIDNEY_DISEASE', 'CHRONIC_PULMONARY_DISEASE', 'LIVER_DISEASE']
	# for v in vars:
		# subsets[v] = [v]
	# #subsets['AGE,LDH'] = ['AGE','ci_LDH_bl']
	# for v in vars[1:]:
		# subsets['AGE,'+v] = ['AGE', v]
	#subsets['AGE,+'] = ['AGE', 'ci_LDH_bl']
	# subsets['AGE,NLR,LDH,CRP'] = ['AGE', 'ci_NLR_bl', 'ci_LDH_bl', 'ci_CRP_bl']
	# vars = vars[1:]
	# for i,v1 in enumerate(vars):
		# for v2 in vars[i+1:]:
			# subsets[f'AGE,{v1},{v2}'] = ['AGE',v1,v2]
	subsets['NLR'] = ['ci_NLR_bl']
	
	# if not toRun or 'nb' in toRun:
		# icuFns = [fn for fn in netFns if '_icu' in fn]
		# ventFns = [fn for fn in netFns if '_invvent' in fn]
		# otherFns = [fn for fn in netFns if '_invvent' not in fn and '_icu' not in fn]
		# validateScenarios('results_dsdecod_nb.json', otherFns, data, subsets, ['DSDECOD'], [0],
			# completeOnly=True, cv={'folds':5, 'learnParams':{'uniformize': True}})
		# validateScenarios('results_icu_nb.json', icuFns, data, subsets, ['ci_ICU'], [1],
			# completeOnly=True, cv={'folds':5, 'learnParams':{'uniformize': True}})
		# validateScenarios('results_invvent_nb.json', ventFns, data, subsets, ['ci_InvVent'], [1],
			# completeOnly=True, cv={'folds':5, 'learnParams':{'uniformize': True}})
	
	# if not toRun or 'tan' in toRun:
		# icuFns = [fn for fn in netFns if '_icu' in fn]
		# ventFns = [fn for fn in netFns if '_invvent' in fn]
		# otherFns = [fn for fn in netFns if '_invvent' not in fn and '_icu' not in fn]
		# # validateScenarios('results_dsdecod_tan.json', otherFns, data, subsets, ['DSDECOD'], [0],
			# # completeOnly=True, cv={'folds':5, 'learnParams':{'uniformize': True}})
		# # validateScenarios('results_icu_tan.json', icuFns, data, subsets, ['ci_ICU'], [1],
			# # completeOnly=True, cv={'folds':5, 'learnParams':{'uniformize': True}})
		# validateScenarios('results_invvent_tan.json', ventFns, data, subsets, ['ci_InvVent'], [1],
			# completeOnly=True, cv={'folds':20, 'learnParams':{'uniformize': True}})
	
	netFns = [
		paths.bns('progression.trained_5.dne'),
	]
	data = readData(paths.out('progression_training.csv'), shuffle=True)
	if not toRun or 'progression' in toRun:
		validateScenarios('results_status_progression.json', netFns, data, {'AGE':['ci_age_group_bg']}, ['ci_status'], [1],
			completeOnly=True)
	
	# netFns = [
		# paths.out('bns/nb/prognb_status.dne'),
	# ]
	# data = readData(paths.out('progression_training.csv'), shuffle=True)
	# if not toRun or 'prognb' in toRun:
		# validateScenarios('results_status_prognb.json', netFns, data, {'AGE':['ci_age_group_bg']}, ['ci_status'], [1],
			# completeOnly=True, cv={'folds':5, 'learnParams':{'uniformize': True}})
	
# Remove all CV cached nets
for fn in pathlib.Path('out/bns').rglob('*_fold*.*'):
	#print(fn)
	os.unlink(fn)

doValidations()
formatAllResults()