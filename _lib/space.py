import _env, argparse
from bni_smile import Net

def space(netFn, multiple = 1):
	print(netFn, repr(multiple))
	net = Net(netFn)
	for node in net.nodes():
		node.position( *[int(v*multiple) for v in node.position()] )
	net.write(netFn+'.out.xdsl')

ap = argparse.ArgumentParser(description='Space out the nodes by some multiplier in the BN')
ap.add_argument('netFn', help='Input file')
ap.add_argument('multiple', type=float, help='How much to multiply out the spacing by')
args = ap.parse_args()

space(args.netFn, args.multiple)