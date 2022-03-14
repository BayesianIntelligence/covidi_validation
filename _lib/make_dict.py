import _env, bndict, sys

fn = sys.argv[1]
bndict.dataToDict(fn, fn+'.dict.csv')
#bndict.dictToBn(fn+'.dict.csv', fn+'.xdsl')