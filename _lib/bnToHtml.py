import _env, argparse, json
from htm import n
from bni_smile import Net

def sitePath(path):
	return path

def bnToHtml(net, root = None):
	root = root or n('div.bnDetail',
		n('script', src='https://code.jquery.com/jquery-3.4.1.slim.min.js'),
		n('script', src=sitePath('/_/js/arrows.js')),
		n('script', src=sitePath('/_/js/bn.component.js')),
		n('script', src=sitePath('/_/js/bn.js')),
		n('link', href=sitePath('/_/css/bn.css'), rel='stylesheet', type='text/css'),
		n('div.title'),
		n('div.bnView'),
	)
	
	return root

def makeJsonModel(net, evidence = None):
	evidence = evidence or {}
	
	for ndeName,stateI in evidence.items():
		net.node(nodeName).finding(int(stateI))
	
	net.update()
	
	bn = {'model': []}
	for node in net.nodes():
		bn['model'].append({
			'type': 'node',
			'name': node.name(),
			'pos': node.position(),
			'parents': [p.name() for p in node.parents()],
			'states': [s.name() for s in node.states()],
			'beliefs': node.beliefs(),
		})
	
	return bn

if __name__ == '__main__':
	ap = argparse.ArgumentParser()
	ap.add_argument('net')
	args = ap.parse_args()
	
	net = Net(args.net)
	print(bnToHtml(net))
	print(n('script', '''
		currentBn = new BnDetail();
		currentBn.make(document.querySelector('.bnDetail'));
		currentBn.$handleUpdate('''+json.dumps(makeJsonModel(net))+''');
	'''))