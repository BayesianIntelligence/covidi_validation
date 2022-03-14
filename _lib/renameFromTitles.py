import _env, argparse
from bni_smile import Net

ap = argparse.ArgumentParser()
ap.add_argument('filename')
opts = ap.parse_args()

net = Net(opts.filename)
for node in net.nodes():
	node.name(node.title(), check=True)
net.write(opts.filename+'.updated.xdsl')