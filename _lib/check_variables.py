import _env, os, re, bndict, sys, csv
from htm import n
from bni_smile import *

style = n('style', r'''
		ul li ul li { opacity: 0; margin-bottom: 5px; }
		ul li:hover ul li { opacity: 1; }
		td, th { text-align: left; vertical-align: top; }
		table.bordered { border-collapse: collapse; }
		table.bordered td, table.bordered th { padding: 2px; border: solid 1px #ccc; }
		span.tried, span.similar { display: inline-block; background: rgb(235,240,248); padding: 2px; margin: 5px; }
		span.similar { background: rgb(255,255,213); }
		tr.header th { position: sticky; top: 0; z-index: 2; background: white; border: none;
			box-shadow: -1px -1px 0 0 #ccc, 1px 1px 0 0 #ccc; }
		.ok { color: green; font-weight: bold; }
	''')

def checkBnVariables(out, fullFn, modelDict = None, modelDictFn = None):
	if modelDictFn:
		modelDict = bndict.loadDict(modelDictFn, lowerCaseKey = True)
	out.append( n('h1', 'Checking ', os.path.relpath(fullFn, _env.root)) )
	net = Net(fullFn)
	nodeNames = []
	for node in net.nodes():
		attempts = bndict.knownVariable(modelDict, node, returnAttempts = True)
		if isinstance(attempts,list):
			similarVars = bndict.similarVariables(modelDict, node)
			nodeNames.append([node.name(), attempts, similarVars])

	if nodeNames:
		out.append( n('h2', 'BN nodes not in dictionary') )
		table = n('table.bordered',
			n('tr.header', n('th', 'BN Variable'), n('th', 'Tried'), n('th', 'Similar')),
		)
		out.append(table)
		for nodeName in nodeNames:
			tr = n('tr', n('td', nodeName[0]))
			table.append(tr)
			tr.append(n('td',
				[n('span.tried', name) for name in nodeName[1]],
			))
			tr.append(n('td',
				[n('span.similar', name) for name in nodeName[2]],
			))
		return
	
	out.append(n('p.ok', 'All nodes OK'))

def checkDataVariables(out, fullFn, dict = None, dictFn = None):
	if dictFn:
		dict = bndict.loadDict(dictFn, lowerCaseKey = True)
	out.append( n('h1', 'Checking ', os.path.relpath(fullFn, _env.root)) )
	with open(fullFn) as fullFile:
		inCsv = csv.DictReader(fullFile)
		fields = inCsv.fieldnames
		fullFile.close()
		
	fieldNames = []
	for field in fields:
		attempts = bndict.knownVariable(dict, field, returnAttempts = True)
		if isinstance(attempts,list):
			similarVars = bndict.similarVariables(dict, field)
			fieldNames.append([field, attempts, similarVars])

	if fieldNames:
		out.append( n('h2', 'Data file variables not in dictionary') )
		table = n('table.bordered',
			n('tr.header', n('th', 'BN Variable'), n('th', 'Tried'), n('th', 'Similar')),
		)
		out.append(table)
		for fieldName in fieldNames:
			tr = n('tr', n('td', fieldName[0]))
			table.append(tr)
			tr.append(n('td',
				[n('span.tried', name) for name in fieldName[1]],
			))
			tr.append(n('td',
				[n('span.similar', name) for name in fieldName[2]],
			))
		return
	
	out.append(n('p.ok', 'All fields OK'))

def checkBnVariablesAll():
	modelDict = bndict.loadGlobalModelDict(lowerCaseKey = True)

	out = n('div.output', style)
	for root, dirs, files in os.walk(_env.root):
		for fileName in files:
			if re.search(r'R\d+\.xdsl', fileName):
				fullFn = os.path.join(root,fileName)
				checkVariables(out, fullFn, modelDict)
	
	return out

if len(sys.argv) == 3:
	out = n('div.output', style)
	ext = os.path.splitext(sys.argv[1])[1]
	if ext in ['.xdsl','.dne']:
		checkBnVariables(out, sys.argv[1], modelDictFn = sys.argv[2])
	else:
		checkDataVariables(out, sys.argv[1], dictFn = sys.argv[2])
	print(out)
else:
	print(checkBnVariablesAll())
