import _env, csv, re, glob, os, openpyxl, functools, random
import bni_smile

# Filter (using subset) and transform (using rowAdapter) data
def filterData(inFn, outFn, subset = None, rowAdapter = None):
	with open(inFn) as inFile, open(outFn, 'w', newline='') as outFile:
		inCsv = csv.DictReader(inFile)
		
		newFieldNames = set(inCsv.fieldnames).intersection(subset)
		
		outCsv = csv.DictWriter(outFile, newFieldNames)
		outCsv.writeheader()
		
		if rowAdapter is None:
			rowAdapter = lambda x: x
		
		for row in inCsv:
			rowAdapter(row)
		
			newRow = {k:v for k,v in row.items() if k in newFieldNames}
			
			outCsv.writerow(newRow)

def dataToDict(inFn, outFn):
	with open(inFn) as file, open(outFn, 'w', newline='') as dictFile:
		inCsv = csv.reader(file)
		outCsv = csv.DictWriter(dictFile, ['Variable', 'Description', 'States'])
		outCsv.writeheader()
		
		vars = {}
		headers = None
		headerPos = None
		for row in inCsv:
			if headers is None:
				headers = row
				headerPos = dict(zip(row,range(len(row))))
				for header in headers:
					vars[header] = {'name': header, 'states': set(), 'description': ''}
			else:
				for i,value in enumerate(row):
					vars[headers[i]]['states'].add(value)
		
		# This is so we can sort numbers in the middle properly
		def makeInts(lst):
			for i,el in enumerate(lst):
				# Convert to int, and then 0-pad it for comparisons
				try: lst[i] = f'{int(el):010}'
				except: pass
			return lst
		
		for varName,var in vars.items():
			sortedStates = sorted(var['states'], key = lambda s: makeInts(re.split(r'(?<=\D)(?=[.\d])|(?<=[.\d])(?=\D)', s)))
		
			outCsv.writerow({
				'Variable': var['name'],
				'Description': var['description'],
				'States': ', '.join(sortedStates),
			})

def gridLayoutObjects(objects):
	x = 0; y = 0
	xSpace = 150; ySpace = 70
	xMax = 1200
	width = 120; height = 50
	
	for obj in objects:
		obj.size(width, height)
		obj.position(x, y)
		x += xSpace
		if x > xMax:
			x = 0
			y += ySpace
		
def dictToBn(inFn, outFn, submodelField = None, rowAdapter = None, subset = None,
		bnModule = bni_smile):
	with open(inFn) as inFile:
		inCsv = csv.DictReader(inFile)
		
		if rowAdapter is None:
			rowAdapter = lambda x: x
		
		vars = []
		for row in inCsv:
			if subset is None or row.get('Variable') in subset:
				rowAdapter(row)
				vars.append(row)
		
		try:
			vars.sort(key = lambda row: row['Category'])
		except: pass

		net = bnModule.Net()
		hasSubmodelSupport = hasattr(net, 'submodels')
		
		
		lastSubmodelField = 1 # Start with any value other than a string or None, so it doesn't match
		for row in vars:
			variable = row.get('Variable')
			addedVars = []
			if variable:
				states = re.split(r'\s*,\s*', row.get('States', 'True, False'))
				addedStates = []
				for i,state in enumerate(states):
					# Check uniqueness against all other states, as those state
					# names are also being made valid and unique...
					states[i] = net.makeValidName(state, uniqueSet = addedStates)
					addedStates.append(states[i])
				submodelTitle = row.get(submodelField)
				validName = net.makeValidName(variable, uniqueSet = addedVars)
				node = net.addNode(validName, states = states)
				#print(repr(variable))
				node.title(variable)
				if submodelTitle:
					submodelName = net.makeValidName(submodelTitle)
					submodel = net.getSubmodel(submodelName)
					if not submodel:
						submodel = net.addSubmodel(submodelName)
						submodel.title(submodelTitle)
						ns = net.submodels()
					node.submodel(submodel.name())
				for field,val in row.items():
					node.user().add(net.makeValidName(field), val)
				#net.write(outFn)
				addedVars.append(validName)
			lastSubmodelField = row.get(submodelField)
		
		netSubmodels = hasSubmodelSupport and net.submodels(submodelOnly = True)
		if netSubmodels:
			gridLayoutObjects(netSubmodels)
			for submodel in netSubmodels:
				gridLayoutObjects(submodel.nodes(submodelOnly = True))
		else:
			gridLayoutObjects(net.nodes())
		
		net.write(outFn)

def loadDict(inFn, lowerCaseKey = True):
	with open(inFn) as inFile:
		inCsv = csv.DictReader(inFile)
		fields = {}
		for fieldName in inCsv.fieldnames:  fields[re.sub(r'\s','',fieldName.lower())] = fieldName
		# Order is important
		potentialKeys = ['globalvariablename', 'bnglobalvariable', 'variable', 'name', 'title']
		varField = None
		for key in potentialKeys:
			if key in fields:
				varField = fields[key]
				break
				
		if not varField:
			print('Can\'t find variable name field')
			return
		
		dict = {}
		
		for row in inCsv:
			key = row[varField]
			if 'states' in row:
				row['statelist'] = re.split(r'\s*,\s*', row['states'])
			if lowerCaseKey:
				key = key.strip().lower()
			dict[key] = row
			
		return dict

def loadGlobalModelDict(lowerCaseKey = True):
	fns = glob.glob(os.path.join(_env.root, 'models/_global/*.csv'))
	return loadDict(fns[0], lowerCaseKey = True)

def saveDict(modelDict, outCsvFn):
	with open(outCsvFn, 'w', newline='') as outCsvFile:
		# Get fieldnames as keys from first row
		fieldNames = next(iter(modelDict.values())).keys()
		outCsv = csv.DictWriter(outCsvFile, fieldNames)
		outCsv.writeheader()
		
		# Go through dict and write out each entry
		for key, row in modelDict.items():
			outCsv.writerow(row)

def knownVariable(modelDict, node, returnAttempts = False):
	name = node.lower() if isinstance(node,str) else node.name().lower()
	
	tried = set()
	
	if name in modelDict:  return name
	tried.add(name)
	name = re.sub(r'_+t\d+$', '', name)
	if name in modelDict:  return name
	tried.add(name)
	
	if hasattr(node, 'parentSubmodel'):
		currentSubmodel = node
		while 1:
			currentSubmodel = currentSubmodel.parentSubmodel()
			if not currentSubmodel:  break
			
			currentSubmodelName = re.sub(r'_+t_?\d+$', '', currentSubmodel.name().lower())
			
			name = re.sub('_+'+re.escape(currentSubmodelName)+'$', '', name)
			if name in modelDict:  return name
			tried.add(name)
			name = re.sub(r'_+t_?\d+$', '', name)
			if name in modelDict:  return name
			tried.add(name)
	
	name = re.sub(r'_', '', name)
	if name in modelDict:  return name
	tried.add(name)

	if returnAttempts:
		return sorted(list(tried))
	return False

# https://codereview.stackexchange.com/a/126199
def levenshteinDistance(string1, string2):
	try:
		from Levenshtein import distance
		return distance(string1, string2)
	except: pass
	
	n = len(string1)
	m = len(string2)
	d = [[0 for x in range(n + 1)] for y in range(m + 1)]

	for i in range(1, m + 1):
		d[i][0] = i

	for j in range(1, n + 1):
		d[0][j] = j

	for j in range(1, n + 1):
		for i in range(1, m + 1):
			if string1[j - 1] is string2[i - 1]:
				delta = 0
			else:
				delta = 1

			d[i][j] = min(d[i - 1][j] + 1,
						  d[i][j - 1] + 1,
						  d[i - 1][j - 1] + delta)

	return d[m][n]


def similarVariables(modelDict, node):
	attempts = knownVariable(modelDict, node, returnAttempts = True)
	similar = []
	if isinstance(attempts,list):
		for varName in modelDict:
			for attempt in attempts:
				dist = levenshteinDistance(varName,attempt)/max(len(attempt),len(varName))
				if dist < 0.5:
					similar.append([varName,dist])
	
	similar.sort(key = lambda v: v[1])
	seen = set()
	similarVarNames = []
	for var in similar:
		if var[0] not in seen:
			similarVarNames.append(var[0])
		seen.add(var[0])
	
	return [*similarVarNames]

def excelToCsv(inExcelFn, outCsvFn):
	"""Converts an excel file to a CSV"""
	wb = openpyxl.load_workbook(inExcelFn)
	sh = wb.get_active_sheet()
	with open(outCsvFn, 'w') as outCsvFile:
		outCsv = csv.writer(outCsvFile)
		for row in sh.rows:
			outCsv.writerow([cell.value for cell in row])

def generateData(dataDict, outCsvFn, n = 1000, distributions = None, model = None):
	"""
	Generate data using a data dictionary. You can also specify either univariate distributions
	for the variables using `distributions` or a fuller model (as a discrete BN) using `model`.
	
	dataDict -- a data dictionary file (as a CSV, with at least the 2 columns: Variable, States) or a loaded dictionary, in the format {"variable1": {"statelist": ["state1","state2","etc"], ...}}
	distributions -- dictionary of univariate distributions
	model -- NYI! a BN. Overrides distributions if specified.
	"""
	dict = loadDict(dataDict)
	
	with open(outCsvFn, 'w', newline='') as outCsvFile:
		fieldNames = dict.keys()
		outCsv = csv.DictWriter(outCsvFile, fieldNames)
		outCsv.writeheader()
		
		for r in range(n):
			newRow = {}
			for fieldName in fieldNames:
				weights = None
				if distributions:  weights = distributions.get(fieldName)
				if fieldName == 'BL_Gender': print(fieldName, weights)
				newRow[fieldName] = random.choices(dict[fieldName]['statelist'], weights = weights)[0]
			outCsv.writerow(newRow)