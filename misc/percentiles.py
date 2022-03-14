import _env, bidb, json, numpy as np

db = bidb.DB('../out/iddo_sample.sqlite')
class Quantiles:
	def __init__(self):
		self.vals = []
		self.percentiles = []
	def step(self, value, *args):
		if not self.percentiles and args:
			self.percentiles = args
		try:
			self.vals.append(float(value))
		except: pass
	def finalize(self):
		#self.vals.sort()
		n = len(self.vals)
		if n == 0:
			return None
		#return ','.join([str(self.vals[int(n*p)]) for p in self.percentiles])
		return ','.join([str(np.quantile(self.vals, p)) for p in self.percentiles])
db.conn.create_aggregate('_quantiles', -1, Quantiles)

table = 'timeSeries'
quantiles = [0.01, 0.99]

cols = db.queryRows(f'select name from pragma_table_info("{table}")',oneD=True)
colStrs = [f'_quantiles({col}, {",".join(str(q) for q in quantiles)}) as {col}' for col in cols]
#colStrs = '_quantiles(ci_crp_bl, 0.05, 0.5, 0.95)'
row = db.queryRow(f'select {",".join(colStrs)} from {table}')
row = {k:(v.split(',') if v else v) for k,v in row.items()}
print(json.dumps(row,indent='\t'))