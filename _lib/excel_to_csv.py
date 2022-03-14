import bndict, sys, os

inFn = sys.argv[1]
outFn = os.path.splitext(inFn)[0]+'.csv'

bndict.excelToCsv(inFn, outFn)