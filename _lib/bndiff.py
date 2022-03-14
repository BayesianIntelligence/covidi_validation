import _env, argparse, json
from bni_smile import Net
import bnToHtml
from htm import n

def transpose(matrix):
	rows = len(matrix)
	cols = len(matrix[0])
	
	newMatrix = [[0 for i in range(cols)] for j in range(rows)]
	
	for i in range(rows):
		for j in range(cols):
			newMatrix[i][j] = matrix[j][i]
	
	return newMatrix

def getAdjacencyMatrix(net, nodeIds = None, opts = {}):
	def getName(n):  return n.name()
	def getNode(id):  return net.node(id)
	if opts.get('title'):
		def getName(n):  return n.title()
		titleMap = {n.title(): n for n in net.nodes()}
		def getNode(id):  return titleMap.get(id)
	if nodeIds is None:
		nodeIds = [getName(n) for n in net.nodes()]
		
	nodeIdPos = {}
	for i,nodeId in enumerate(nodeIds):
		nodeIdPos[nodeId] = i
	
	
	numNodes = len(nodeIds)
	adjMatrix = [[0 for i in range(numNodes)] for j in range(numNodes)]
	
	for nodeId in nodeIds:
		node = getNode(nodeId)
		if node:
			children = node.children()
			childIds = [getName(c) for c in children]
			for childId in childIds:
				adjMatrix[ nodeIdPos[nodeId] ][ nodeIdPos[childId] ] = 1
	
	return adjMatrix

def getAllNodeIds(nets, opts = {}):
	def getName(n):  return n.name()
	if opts.get('title'):
		def getName(n):  return n.title()
	nodeIds = {}
	for net in nets:
		for node in net.nodes():
			nodeIds[getName(node)] = 1
	
	# print(list(nodeIds.keys()))
	
	return list(nodeIds.keys())
	
def editDistanceStructure(net1, net2, opts = {}):
	nodeIds = getAllNodeIds([net1, net2], opts)
	# print(nodeIds)
	
	info = {'added': [], 'omitted': [], 'reversed': []}
	def addInfo(type,r,c):
		info[type].append([nodeIds[r], nodeIds[c]])
	
	adjMatrix1 = getAdjacencyMatrix(net1, nodeIds, opts)
	adjMatrix2 = getAdjacencyMatrix(net2, nodeIds, opts)
	
	# for r in adjMatrix1:
		# print(r)
	# print('-')
	# for r in adjMatrix2:
		# print(r)
	
	rows = len(adjMatrix1)
	cols = len(adjMatrix1[0])
	
	for r in range(rows):
		for c in range(cols):
			result = adjMatrix2[r][c] - adjMatrix1[r][c]
			reverseArc = adjMatrix2[r][c] == adjMatrix1[c][r]
			if result == 1:
				if reverseArc:
					addInfo('reversed',r,c)
				else:
					addInfo('added',r,c)
			elif result == -1:
				if adjMatrix2[c][r] == 0:
					addInfo('omitted',r,c)
	
	return info

def compareNetFiles(net1Fn, net2Fn, opts = {}):
	return editDistanceStructure(Net(net1Fn), Net(net2Fn), opts)

def graphicalCompare(net1Fn, net2Fn, diffs, opts = {}):
	tmpFn = '~temp.xdsl'
	net1 = Net(args.net1)
	net2 = Net(args.net2)
	#net = net1.merge(net2)
	net = net1
	print(bnToHtml.bnToHtml(net))
	print(n('script', '''
		currentBn = new BnDetail();
		currentBn.make(document.querySelector('.bnDetail'));
		currentBn.$handleUpdate('''+json.dumps(bnToHtml.makeJsonModel(net))+''');
	'''))

if __name__ == '__main__':
	ap = argparse.ArgumentParser()
	ap.add_argument('net1', help='Original network')
	ap.add_argument('net2', help='Comparison (or updated) network')
	ap.add_argument('--titles', action='store_true', help='Compare using node titles rather than node IDs')
	ap.add_argument('--graphical', action='store_true', help='Output as BN graph')
	args = ap.parse_args()
	
	opts = {
		'title': args.titles,
	}
	
	out = compareNetFiles(args.net1, args.net2, opts)
	if args.graphical:
		graphicalCompare(args.net1, args.net2, out, opts)
	else:
		#print(json.dumps(out, indent = '\t'))
		print(n('h2', 'Added'))
		for added in out['added']:
			print('{} -> {}'.format(added[0], added[1]))
		print(n('h2', 'Omitted'))
		for omitted in out['omitted']:
			print('{} -> {}'.format(omitted[0], omitted[1]))
		print(n('h2', 'Reversed'))
		for reversed in out['reversed']:
			print('{} -> {}'.format(reversed[0], reversed[1]))