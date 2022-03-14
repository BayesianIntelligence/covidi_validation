import math, numpy, json, time
# from bni_netica import *
	
def nextStates (states, nodes):
	for i in reversed(range(0,len(nodes))):
		states[i] = states[i] + 1
		if states[i] < len(nodes[i]['states']):
			return False
		states[i] = 0
	return True

def generateCptFromBeta(currentWeights, numStates, topRowParams, bottomRowParams):
	parents = currentWeights
	
	if parents:
		maxRowScore = 0
		rowScores = []
		parentStates = [0]*len(parents)
		parentStates[len(parents)-1] = -1
		while not nextStates(parentStates,parents):
			rowScore = 0
			for state,parent in zip(parentStates,parents):
				rowScore += parent['states'][state]['weight']*parent['multiplier']
			rowScore = rowScore/len(parents)
			maxRowScore = max(maxRowScore,rowScore)
			rowScores.append(rowScore)
	else:
		maxRowScore = 1
		rowScores = [0.5]
	
	cpt = []
	[alpha1,beta1] = [float(x) for x in topRowParams]
	[alpha2,beta2] = [float(x) for x in bottomRowParams]
	ess1 = alpha1 + beta1
	ess2 = alpha2 + beta2
	mean1 = alpha1/ess1
	mean2 = alpha2/ess2
	for i in range(0, len(rowScores)):
		rowScore = rowScores[i]
		try:
			prop = rowScore/maxRowScore
		except:
			prop = 1
		mean = mean1*(prop) + mean2*(1-prop)
		ess = ess1*(prop) + ess2*(1-prop)
		alpha = mean*ess
		beta = ess - alpha
		
		vec = getVectorFromBeta(alpha, beta, numStates )
		cpt.append( vec  )
	
	return cpt
	

def generateTable(setup):
	return generateCptFromBeta(setup['parents'], len(setup['child']['states']), setup['topBottomParams'][0].split(','), setup['topBottomParams'][1].split(','))
	

def fitBetaToMultinomial(vector):
	#make sure there's no zeros in vector
	e = 0.01
	vector = normalise([max(x,e) for x in vector])
	
	
	points = []
	for i,x in enumerate(vector):
		points.append([i/len(vector)+0.5/len(vector),x])
	
	#Calc mean
	mean = 0
	for point in points:
		mean = mean + point[0]*point[1]
	
	#Calc variance
	var = 0
	for point in points:
		var = var + (point[0]-mean)**2*point[1]
		
	#Estimate alpha/beta from mean/variance
	complMean = 1-mean
	alpha = mean*(mean*complMean/var - 1)
	beta = complMean*(mean*complMean/var - 1)
	
	ret = optimizeBetaFit(alpha, beta, vector);
	alpha = ret[0]
	beta = ret[1]
	
	return [alpha, beta]
	
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
	
def klDiv(vec, otherVec):
	try:
		sum = 0
		for x, y in zip(vec,otherVec):
			sum += x*math.log(x/y)
		return max(0,sum) #Can sometimes be negative for very small numbers
	except:
		return 10000
	
def getVectorFromBeta(alpha, beta, length):
	prevBetaVal = 0
	vec = []
	for i in range(0, length):
		betaVal = betaCdf(((i+1)/length), alpha, beta)
		stateProb = betaVal - prevBetaVal
		vec.append(stateProb)
		prevBetaVal = betaVal
		
	#make sure there's no zeros in vector
	e = 0.01
	vec = normalise([max(e,x) for x in vec])
	return vec
	
	


def compareCpts(cpt1, cpt2):
	if len(cpt1) != len(cpt2):
		raise ValueError("CPTs not same length")
	
	tot = 0
	for rowI, row in enumerate(cpt1):
		tot += klDiv(cpt1[rowI], cpt2[rowI])
	
	return tot/len(cpt1)
	
	
def mutateParentWeights(weights):
	weights = json.loads(json.dumps(weights))
	for parent in weights:
		for state in parent['states']:
			state['weight'] = min(10,max(0,round(state['weight'] + numpy.random.normal(),1)))
	return weights

def fitSetup(node):
	print('fitting setup to', node.name())
	setup = defaultSetup(node)
	parentNodes = setup['parents']
	
	mutator = mutateParentWeights
	#Get top/bottom row
	cpt = node.cpt()
	topRow = cpt[0]
	bottomRow = cpt[-1]
	
	def skew(vec): return sum([i*x for i,x in enumerate(vec)])
	
	topSkew = skew(topRow)
	bottomSkew = skew(bottomRow)
	
	for cpd in cpt:
		if skew(cpd) < topSkew:
			topRow = cpd
			topSkew = skew(cpd)
		if skew(cpd) > bottomSkew:
			bottomRow = cpd
			bottomSkew = skew(cpd)
	
	#Get their alpha/beta params
	topRowParams = fitBetaToMultinomial(topRow)
	bottomRowParams = fitBetaToMultinomial(bottomRow)
	
	
	def transFunc(newScore,currScore,T):
		if newScore < currScore:  return 1
		return math.exp(-(newScore - currScore)/T)
	
	# Setup initial parent state weights (start uniform)
	currentScore = 0
	currentWeights = parentNodes
	for parent in currentWeights:
		i = len(parent['states'])-1
		delta = 10/len(parent['states'])-1
		for state in parent['states']:
			state['weight'] = i*delta
			i -= 1
			
	newCpt = generateCptFromBeta(currentWeights, len(topRow), topRowParams, bottomRowParams)
	
	bestCpt = newCpt
	bestScore = compareCpts(newCpt, cpt)
	bestWeights = currentWeights
	iterations = 10000
	lastSwitch = 0
	
	for k in range(iterations):
		# /// Mutate weights
		newWeights = mutator(currentWeights)
		newCpt = generateCptFromBeta(newWeights, len(topRow), topRowParams, bottomRowParams)
		newScore = compareCpts(newCpt, cpt)
		# Check and keep if better, probabistically keep if worse
		T = 1-(k/iterations)
		if transFunc(newScore,currentScore,T) >= numpy.random.random():
			currentScore = newScore
			currentWeights = newWeights
			if currentScore < bestScore:
				bestScore = currentScore
				bestWeights = currentWeights
				bestCpt = newCpt
				lastSwitch = k
			# If it's been more than 10 iterations since an improvement, go back to best
			elif k-lastSwitch > 10:
				currentScore = bestScore
				currentWeights = bestWeights
				newCpt = bestCpt
				lastSwitch = k
	
	setup['topBottom'] = [','.join([str(round(x,1)) for x in topRow]),','.join([str(round(x,1)) for x in bottomRow])]
	setup['topBottomParams'] = [','.join([str(round(x,1)) for x in topRowParams]),','.join([str(round(x,1)) for x in bottomRowParams])]
	setup['parents'] = bestWeights
	return setup
	

def defaultSetup(node):
	def getNode(node):	
		return {
			'id': node.name(),
			'title': node.title(),
			'states': [{'name':state.name(), 'weight':10*weight/(node.numberStates()-1)} for state, weight in zip(node.states(),range(node.numberStates()-1,-1,-1))],
			'multiplier': 1
		}

	return {
		'parents': [getNode(parent) for parent in node.parents()],
		'child': getNode(node),
		'topBottom': ["",""],
		'topBottomParams': ["1,4","4,1"]
		}
		
	
def defaultSetups(net):
	setups = {}
	for node in net.nodes(): 
		setups[node.name()] = defaultSetup(node)	
	return {'nodeSetups': setups}
	
def fitSetups(net):
	print('Fitting Setup to BN')
	t = time.time()
	setups = {}
	for node in net.nodes(): 
		setups[node.name()] = fitSetup(node)
	print(f'Done ({round(time.time()-t,2)}s)')
	return {'nodeSetups': setups}
	
	
def loadSetup(net, setups = None):
	if not setups: setups = defaultSetups(net)
	for node in net.nodes():
		try:
			setup = setups['nodeSetups'][node.name()]
			print('loading', node.name())
			node.cpt(generateTable(setup))
		except Exception as e:
			print('failed loading', node.name(), str(e))
			node.setUniform()
	return net
	

	


	
	
######### Beta distribution code
### XXX: Copyright status of code not clear. Check or replace.
	
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
	
def normalise(vec):
    prob_factor = 1 / sum(vec)
    return [prob_factor * p for p in vec]
