import _env, csv, os, string, random, json, re, math, numpy
import bni_smile, bni_netica


def validName(name):
	name = re.sub('[^0-9a-zA-Z]+', '_', name).lower()
	if name[0].isnumeric(): name = 's'+name
	return name

def makeDataDict(fileName):
	dataDict = {}
	with open(fileName, encoding='utf-8') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		rows = [row for row in reader if row]
		headings = rows[0]
		for col_header in headings:
			dataDict[col_header] = set()
		for row in rows[1:]:
			try:
				for col_header, data_column in zip(headings, row):
					if data_column != '': dataDict[col_header].add(data_column)
			except Exception as e:
				print(e)
	for key in dataDict:
		dataDict[key] = list(dataDict[key])
	with open(os.path.splitext(fileName)[0]+'.json', 'w') as file:
		file.write(json.dumps(dataDict))
	return dataDict
	
def addDictNodes(net, fileName):
	with open(fileName) as file:
		dataDict = json.load(file)
	for var in dataDict.keys():
		name = validName(var)
		print(name)
		print(sorted(dataDict[var]))
		states = [validName(var) for var in sorted(dataDict[var])]
		print(states)
		new = net.addNode(name, states=states)
		new.cpt([len(new.states())*[1/len(new.states())]])
	return net
	
def replaceMissing(casefile, dataDict, missingval = '', inDelimiter = ',', skip = []):
	outfn = os.path.splitext(casefile)[0]+'_missing.csv'
	with open(dataDict) as file:
		dataDict = json.load(file)
	with open(casefile) as csv_file:
		reader = csv.reader(csv_file, delimiter=inDelimiter)
		rows = [row for row in reader if row]
		headings = [ele.lower() for ele in rows[0]]
		with open(outfn, mode='w', newline ='') as out_file:
			csv_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			csv_writer.writerow(headings)
			for row in rows[1:]:
				out = []
				for col_header, data_column in zip(headings, row):
					if col_header in skip:
						out.append(data_column)
					else:
						try:
							dataDict[col_header]['states'][data_column]
							out.append(data_column)
						except:
							out.append(missingval)
				csv_writer.writerow(out)
			out_file.close()			
	return outfn
	
	
def lowerCase(casefile, outFn = None, sepType = ','):
	outfn = outFn or os.path.splitext(casefile)[0]+'_lower.csv'
	with open(casefile) as csv_file:
		reader = csv.reader(csv_file, delimiter=sepType)
		rows = [row for row in reader if row]
		headings = [ele.lower() for ele in rows[0]]
		with open(outfn, mode='w', newline ='') as out_file:
			csv_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			csv_writer.writerow(headings)
			for row in rows[1:]:
				try:
					row = [ele.lower() for ele in row]
					csv_writer.writerow(row)
				except Exception as e:
					print(e)
			out_file.close()
	return outfn
	
def trimCI(casefile, outFn = None, sepType = ','):
	outfn = outFn or os.path.splitext(casefile)[0]+'_trim.csv'
	with open(casefile) as csv_file:
		reader = csv.reader(csv_file, delimiter=sepType)
		rows = [row for row in reader if row]
		headings = rows[0]
		newHeadings = list(filter(lambda k: 'ci_' in k, rows[0]))
		with open(outfn, mode='w', newline ='') as out_file:
			csv_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			csv_writer.writerow(newHeadings)
			for row in rows[1:]:
				out = []
				for col_header, data_column in zip(headings, row):
					if col_header in newHeadings:
						out.append(data_column)
				csv_writer.writerow(out)
			out_file.close()
	return outfn
	
	
def appendDayPhase(casefile, outFn = None, sepType = ','):
	def is_digit(n):
		try:
			float(n)
			return True
		except ValueError:
			return  False
		
	def getPhase(row, day):
		val = 'bl'
		for phase in ['uc','co','cr','re']:
			if row['bl_'+phase+'startday'] and float(row['bl_'+phase+'startday']) <= day : val = phase
		if val == 're': val = 'rc'
		return val
		
	def getDeath(row):
		try:
			last = int(row['bl_lastknownstatus'])
			if last == 3 or last == 5 or last == 6:
				return getPhase(row, int(row['bl_admission'])+int(row['bl_duration_inpatientstay']))
			else:
				return 'na'
		except Exception as e:
			#print(e)
			pass
		return '*'
	
	outfn = outFn or os.path.splitext(casefile)[0]+'_dayphase.csv'
	with open(casefile) as csv_file:
		reader = csv.DictReader(csv_file, delimiter=',')
		with open(outfn, mode='w', newline ='') as out_file:
			fieldnames = reader.fieldnames#'bl_admission bl_ucstartday bl_costartday bl_crstartday bl_restartday bl_lastknownstatus bl_duration_inpatientstay'.split(' ') # bl_duration_icustay bl_duration_ventilation bl_observationalperiod

			writer = csv.DictWriter(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames+'ci_d5 ci_d10 ci_d20 ci_death'.split(' '))
			writer.writeheader()
			
			for row in reader:
				try:
					out = {}
					for ele in fieldnames: out[ele] = row[ele]
					
					for ele in row:
						row[ele] = re.sub('[<>]+', '', row[ele])
						if not is_digit(row[ele]) or row[ele] == '-99' or row[ele] == '-66': 
							row[ele] = ''
					
					#out['ci_d0'] = getPhase(row, 0)
					#out['ci_d1'] = getPhase(row, 1)
					out['ci_d5'] = getPhase(row, 5)
					out['ci_d10'] = getPhase(row, 10)
					out['ci_d20'] = getPhase(row, 20)
					out['ci_death'] = getDeath(row)
					
					writer.writerow(out)
				except Exception as e:
					print(e)

def xdsl2dne(filename):
	filename = os.path.splitext(filename)[0]
	gNet = bni_smile.Net(filename+'.xdsl')
	
	# for node in gNet.nodes():
		# node.user().delete('weights')
	
	gNet.write(filename+'.dne')
	nNet = bni_netica.Net(filename+'.dne')
	
	for node in nNet.nodes():
		pos = gNet.node(node.name()).position()
		node.position(pos[0],pos[1])
		node.user().add('submodel', gNet.node(node.name()).parentSubmodel().name())
		
		if re.match("s[0-9]+_[0-9]+_[0-9]+_[0-9]",node.state(0).name()):
			levels = []
			for state in node.states():
				levels.append(float(state.name()[1:].split('_')[0]+'.'+state.name()[1:].split('_')[1]))
				levels.append(float(state.name()[1:].split('_')[2]+'.'+state.name()[1:].split('_')[3]))
			levels = list(dict.fromkeys(levels))
			levels.sort()
			name = node.name()
			parents = node.parents()
			cpt = node.cpt()
			node.remove()
			nNet.addNode(name, levels = levels, nodeType = bni_netica.Node.CONTINUOUS_TYPE)
			nNet.node(name).addParents(parents)
			nNet.node(name).cpt(cpt)
			nNet.node(name).user().add('submodel', gNet.node(name).parentSubmodel().name())
		
	nNet.write(filename+'.dne')
	return filename+'.dne'
	
def dne2xdsl(filename):
	filename = os.path.splitext(filename)[0]
	nNet = bni_netica.Net(filename+'.dne')
	gNet = bni_smile.Net(filename+'.dne')
    
	for node in nNet.nodes():
		pos = node.position()
		gNet.node(node.name()).position(int(pos[0]),int(pos[1]))
		try:
			gNet.addSubmodel(node.user().get('submodel'))
			gNet.node(node.name()).parentSubmodel(node.user().get('submodel'))
		except:
			pass
		

	gNet.write(filename+'.xdsl')
	return filename+'.xdsl'
	
def matchLayout(netfn, templatefn):
	net = bni_netica.Net(netfn)
	template = bni_netica.Net(templatefn)
	
	for node in net.nodes():
		try:
			pos = template.node(node.name()).position()
			node.position(pos[0],pos[1])
		except:
			print(node.name())
			print(template.node(node.name()))
			pass
	
	net.write(netfn)
	
def nextStates (states, nodes):
	for i in reversed(range(0,len(nodes))):
		states[i] = states[i] + 1
		if states[i] < nodes[i].numberStates():
			return False
		states[i] = 0
	return True
	
	
def getMeanSd(a, b):
	return [a/(a+b), math.sqrt(a*b/(math.pow(a+b,2)*(a+b+1)))]

def getVectorFromBeta(alpha, beta, length):
	prevBetaVal = 0
	vec = []
	for i in range(0, length):
		betaVal = betaCdf(((i+1)/length), alpha, beta)
		stateProb = betaVal - prevBetaVal
		vec.append(stateProb)
		prevBetaVal = betaVal
		
			
	#vec = normalise([max(x,0.01) for x in vec])
	vec = normalise(vec)
	return vec
	
def LogGamma(Z): 
	S=1+76.18009173/Z-86.50532033/(Z+1)+24.01409822/(Z+2)-1.231739516/(Z+3)+.00120858003/(Z+4)-.00000536382/(Z+5)
	LG= (Z-.5)*math.log(Z+4.5)-(Z+4.5)+math.log(S*2.50662827465)
	return LG
	
def Betinc(X,A,B):
	A0=0
	B0=1
	A1=1
	B1=1
	M9=0
	A2=0
	while abs((A1-A2)/A1)>.00001:
		A2=A1
		C9=-(A+M9)*(A+B+M9)*X/(A+2*M9)/(A+2*M9+1)
		A0=A1+C9*A0
		B0=B1+C9*B0
		M9=M9+1
		C9=M9*(B-M9)*X/(A+2*M9-1)/(A+2*M9)
		A1=A0+C9*A1
		B1=B0+C9*B1
		A0=A0/B1
		B0=B0/B1
		A1=A1/B1
		B1=1
	return A1/A
	
	
def betaCdf(Z,A,B):
	if A<=0:
		#alert("alpha must be positive")
		Betacdf = 0
	elif B<=0:
		#alert("beta must be positive")
		Betacdf = 0
	elif Z<=0:
		Betacdf=0
	elif Z>=1:
		Betacdf=1
	else:
		S=A+B
		BT=math.exp(LogGamma(S)-LogGamma(B)-LogGamma(A)+A*math.log(Z)+B*math.log(1-Z))
		if Z<(A+1)/(S+2):
			Betacdf=BT*Betinc(Z,A,B)
		else:
			Betacdf=1-BT*Betinc(1-Z,B,A)
			
	Betacdf=Betacdf+.000005
	return Betacdf


def generateCptFromBeta(node, cptsetup = None):
	if not cptsetup: cptsetup = defaultSetup(node)
	bestCase = [float(x) for x in cptsetup['topBottomParams'][0].split(',')]
	worstCase = [float(x) for x in cptsetup['topBottomParams'][1].split(',')]
	multipliers = {}
	weights = {}
	for parent in cptsetup['parents']:
		#need to add lower bound on multiplier.  better to handle this on prop = rowScore/maxRowScore line
		multipliers[parent['id']] = max(0.001, parent['multiplier'])
		weights[parent['id']] = {}
		for state in parent['states']:
			weights[parent['id']][state['name']] = state['weight']
	
	parents = node.parents()
	numChildStates = node.numberStates()
	
	if parents:
		maxRowScore = 0
		rowScores = []
		parentStates = [0]*len(parents)
		parentStates[len(parents)-1] = -1
		while not nextStates(parentStates,parents):
			rowScore = 0
			for state,parent in zip(parentStates,parents):
				rowScore += weights[parent.name()][parent.states()[state].name()]*multipliers[parent.name()]
			rowScore = rowScore/len(parents)
			maxRowScore = max(maxRowScore,rowScore)
			rowScores.append(rowScore)
	else:
		maxRowScore = 1
		rowScores = [0.5]
	
	#print(node.name(),rowScores)
	
	cpt = []
	[alpha1,beta1] = bestCase
	[alpha2,beta2] = worstCase
	# [mean1,sd1] = getMeanSd(alpha1, beta1)
	# [mean2,sd2] = getMeanSd(alpha2, beta2)
	ess1 = alpha1 + beta1
	ess2 = alpha2 + beta2
	mean1 = alpha1/ess1
	mean2 = alpha2/ess2
	for i in range(0, len(rowScores)):
		rowScore = rowScores[i]
		prop = rowScore/maxRowScore
		#alpha = (bestCase[0]-worstCase[0])*prop + worstCase[0]
		#beta = (bestCase[1]-worstCase[1])*prop + worstCase[1]
		mean = mean1*(prop) + mean2*(1-prop)
		ess = ess1*(prop) + ess2*(1-prop)
		alpha = mean*ess
		beta = ess - alpha
		
		vec = getVectorFromBeta(alpha, beta, numChildStates )
		#print('{:.3f}'.format(rowScore), '{:.3f}'.format(alpha), '{:.3f}'.format(beta))
		cpt.append( vec  )
	
	return cpt
	
def skewness(alpha, beta):
	return 2*(beta-alpha)*math.sqrt(alpha+beta+1)/((alpha+beta+2)*math.sqrt(alpha*beta))
	
def klDiv(vec, otherVec):
	try:
		sum = 0
		for x, y in zip(vec,otherVec):
			sum = sum + x*math.log(x/y)
		return max(0,sum)
	except:
		return 10000

def optimizeBetaFit(alpha, beta, vector):
	maxAlpha = alpha
	maxBeta = beta
	maxBetaVec = getVectorFromBeta(alpha, beta, len(vector))
	maxKl = klDiv(vector, maxBetaVec)
	numSteps = 1000
	
	currAlpha = maxAlpha
	currBeta = maxBeta
	for i in range(numSteps):
		currAlpha = maxAlpha + numpy.random.normal()*0.05
		currBeta = maxBeta + numpy.random.normal()*0.05
		currBetaVec = getVectorFromBeta(currAlpha, currBeta, len(vector))
		currKl = klDiv(vector, currBetaVec)
		if currKl < maxKl:
			maxBetaVec = currBetaVec
			maxKl = currKl
			maxAlpha = currAlpha
			maxBeta = currBeta
	return [maxAlpha, maxBeta]

def fitBetaToMultinomial(vector):
	vector = normalise([max(x,0.01) for x in vector])
	points = []
	for i,x in enumerate(vector):
		points.append([i/len(vector)+0.5/len(vector),x])
	
	mean = 0
	for point in points:
		mean = mean + point[0]*point[1]

	var = 0
	for point in points:
		var = var + (point[0]-mean)**2*point[1]

	complMean = 1-mean
	alpha = mean*(mean*complMean/var - 1)
	beta = complMean*(mean*complMean/var - 1)
	
	ret = optimizeBetaFit(alpha, beta, vector);
	alpha = ret[0]
	beta = ret[1]
	
	return [alpha, beta];
	
def defaultSetup(node):
	return {'parents': [{'id': parent.name(), 
						'title':parent.title(), 
						'states':[{'name':state.name(),
							'weight':10*weight/(parent.numberStates()-1)} for state, weight in zip(parent.states(),range(parent.numberStates()-1,-1,-1))], 
						'multiplier': 1} for parent in node.parents()],
			'child':{'id': node.name(), 
						'title':node.title(), 
						'states':[{'name':state.name(),
							'weight':10*weight/(node.numberStates()-1)} for state, weight in zip(node.states(),range(node.numberStates()-1,-1,-1))], 
						'multiplier': 1},
			'topBottom': ["",""],
			'topBottomParams': ["1, 4", "4, 1"],
			#'hasTable': True,
			#'sortOrder': {},
			#'combinationType': "standard",
			#'combinationFormula': "\t\tavg(s`*`)\n\t\t\t\t"
			}
	
			
def signif(x, p):
	np = numpy
	x = np.asarray(x)
	x_positive = np.where(np.isfinite(x) & (x != 0), np.abs(x), 10**(p-1))
	mags = 10 ** (p - 1 - np.floor(np.log10(x_positive)))
	return np.round(x * mags) / mags
	

def calcQuantiles(vals, interval = 0.25):
	numGroups = (1/interval)
	return [signif(numpy.quantile(vals, i*interval),6) for i in range(0,int(numpy.ceil(numGroups))+1)]


def normalise(vec):
    prob_factor = 1 / sum(vec)
    return [prob_factor * p for p in vec]
