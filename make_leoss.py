import _env, json, csv
from bidb import DB

def readLeossDiscretizations():
	with open('otherData/leoss_discretizations.json') as ld:
		return json.load(ld)

def discretizeScalar(discSpec, value):
	if 'map' in discSpec:
		return discSpec['map'].get(value, '*')
	else:
		try:     float(value)
		except:  return '*'
		
		for i,level in enumerate(discSpec['levels']):
			if float(value) < level:
				return discSpec['code'][i]
		return discSpec['code'][-1]

def discretize(discSpec, row, inVars):
	if isinstance(inVars, list):
		vals = []
		if 'combine' not in discSpec:
			raise Exception(f"Multiple inVars, but no 'combine' field: {inVars}")
		combine = discSpec['combine']
		for i,inVar in enumerate(inVars):
			if 'multi' in discSpec:
				thisDiscSpec = discSpec['multi'][i]
			else:
				thisDiscSpec = discSpec
			vals.append(discretizeScalar(thisDiscSpec, row[inVar]))
		# eval/
		args = vals
		
		# Max complains if the sequence is empty :rolleyes:
		try:
			ret = eval(combine)
		except ValueError:
			ret = '*'
		return ret
	else:
		return discretizeScalar(discSpec, row[inVars])

# Computed columns are generally temporary. They go into the row, for use downstream (not for output; but they will get written to output).
def computeColumn(disc, row, key):
	return eval(disc['compute'], {'row':row,'key':key,'disc':disc})

def makeLeossPhasesTable(db):
	# Try to map out equivalent LEOSS phases
	db.query('drop table if exists tempPhases')
	def minNum(*args):
		args = [a for a in args if isinstance(a,(int,float))]
		if args:
			return min(args)
		return None
	db.conn.create_function('_minNum', -1, minNum)
	uc = db.queryRows('''create table tempPhases as
		select usubjid,
			_minNum(uc,co,cr,rc,dt) as uc,
			case when co is not null then _minNum(co,cr,rc,dt) end as co,
			case when cr is not null then _minNum(cr,rc,dt) end as cr,
			case when dt is null then _minNum(rc,rc2) end as rc,
			dt
		from (select usubjid,
			(select min(day) from timeSeries where usubjid = ts.usubjid) as uc,
			(select min(day) from timeSeries where (ci_Oxygen_Saturation < 90 or ci_PO2 < 70 or ci_AST >= 150 or ci_GGT >= 150 or ci_in_trt like '%oxygen%' or ci_in_cat like '%oxygen%') and usubjid = ts.usubjid) as co,
			(select min(day) from timeSeries where (ci_ho_type like '%intensive%') and usubjid = ts.usubjid) as cr,
			(select min(day) from timeSeries where (ci_ds_decod like '%discharged%') and usubjid = ts.usubjid) as rc,
			(select min(day) from timeSeries where (ci_ho_type like '%hospital%') and usubjid = ts.usubjid
				and day > (select min(day) from timeSeries where (ci_ho_type like '%intensive%') and usubjid = ts.usubjid)
			) as rc2,
			(select min(day) from timeSeries where (ci_ds_decod like '%death%') and usubjid = ts.usubjid) as dt
		from timeSeries as ts
		group by usubjid
		)''')


def makeLeossFormatData(db):
	data = []
	dataByUsubjid = {}
	
	makeLeossPhasesTable(db)
	
	leossDiscrete = readLeossDiscretizations()

	# Generate BL variables, based on what's in the discretisation dict
	bl = db.queryRows('''select *
		from subject
			left join baseline on subject.usubjid = baseline.usubjid
			left join tempPhases on subject.usubjid = tempPhases.usubjid''')
	headers = {} # Use keys as ordered set
	for row in bl:
		newRow = {'iddoUsubjid': row['usubjid']} # Need the IDDO usubjid for later/time series
		for key,disc in leossDiscrete.items():
			inVar = disc.get('inVar')
			if disc.get('phased'):
				key = key.format(phase='BL',worst='')
				inVar = inVar+'_bl' if not isinstance(inVar, list) else [v+'_bl' for v in inVar]
			if 'compute' in disc:
				newRow[key] = computeColumn(disc, row, key)
				row[key] = newRow[key]
				# print(key, newRow[key])
			else:
				newRow[key] = discretize(disc, row, inVar)
		data.append(newRow)
		dataByUsubjid[row['usubjid']] = newRow
		headers.update(newRow)
	
	###
	# Generate phased variables based on the phased variables listed in the discretisation dict
	###
	# # Get the average for all phased vars
	# fieldMeans = db.queryMap('select avg(resprate) resprate, ... from baseline')
	
	# SQL aggregate function to find the worst value
	class _getWorst:
		def __init__(self):
			self.chooseMethod = None
			self.outVar = None
			self.inVar = None
		def step(self, outVar, inVar, value):
			if self.chooseMethod is None:
				#print('y')
				self.outVar = outVar
				self.inVar = inVar
				# XXX: If inVar is an array and there is a 'multi' field,
				# then extract the appropriate 'inVarChosen' method from the multi instead
				inVarChosen = leossDiscrete[outVar].get('inVarChosen')
				if inVarChosen:
					args = []
					if isinstance(inVarChosen, list):
						args = inVarChosen[1:]
						inVarChosen = inVarChosen[0]
					print(inVarChosen, args)
					initMethod = getattr(self, inVarChosen+'_init', None)
					#print(inVarChosen, initMethod)
					if initMethod:
						initMethod(*args)
					self.chooseMethod = getattr(self, inVarChosen)
				else:
					self.fieldMean_init()
					#print('x')
					self.chooseMethod = self.fieldMean

			if value is None:  return
			self.chooseMethod(value)
		def furthest_init(self, referenceValue):
			self.refValue = referenceValue
			self.chosen = 0
			self.chosenDist = 0
		def furthest(self, value):
			thisDist = abs(value - self.refValue)
			# print('x')
			if thisDist > self.chosenDist:
				# print('j')
				self.chosen = value
				self.chosenDist = thisDist
		def fieldMean_init(self):
			self.fieldMean = None
			self.chosen = 0
			self.chosenDist = 0
		def fieldMean(self, value):
			# If not yet calculated, work out the mean of the field's baselines values (i.e. and treat that as "normal")
			if self.fieldMean is None:
				# print('fm')
				self.fieldMean = db.queryValue(f"select avg({self.inVar}_bl) from baseline")
				# print('fmt')
			
			# Check if this is the furthest absolute value from the mean
			# print('h', self.fieldMean)
			thisDist = abs(value - self.fieldMean)
			# print('x')
			if thisDist > self.chosenDist:
				# print('j')
				self.chosen = value
				self.chosenDist = thisDist
		def max_init(self):
			self.chosen = 0
		def max(self, value):
			if value > self.chosen:
				self.chosen = value
		def min_init(self):
			self.chosen = float('Inf')
		def min(self, value):
			if value < self.chosen:
				self.chosen = value
		def finalize(self):
			return self.chosen
	db.conn.create_aggregate('_getWorst', 3, _getWorst)
	phases = ['uc','co','cr','rc']
	phasesWithDt = phases + ['dt']
	for i,phase in enumerate(phases):
		phaseVars = ''
		sep = ''
		for key,disc in leossDiscrete.items():
			# Only include phased variables
			if disc.get('phased'):
				#key = key.format(phase=phase.upper(),worst='Worst')
				inVars = disc['inVar'] if isinstance(disc['inVar'],list) else [disc['inVar']]
				for inVar in inVars:
					field = inVar
					outVar = key
					phaseVars += sep + f"_getWorst('{outVar}', '{field}', {field}) as {field}"
					sep = ','
		print(phaseVars)
		rows = db.queryRows(f"""select timeSeries.usubjid, {phaseVars}
			from timeSeries
				left join tempPhases on timeSeries.usubjid = tempPhases.usubjid 
			where day >= {phase} and day <= _minNum({','.join(phasesWithDt[i+1:])})
			group by timeSeries.usubjid
			""")
		for row in rows:
			newRow = dataByUsubjid[row['usubjid']]
			row = {**newRow, **row}
			for key,disc in leossDiscrete.items():
				# Only include phased variables
				if disc.get('phased'):
					key = key.format(phase=phase,worst='Worst')
					if 'compute' in disc:
						newRow[key] = computeColumn(disc, row, key)
					else:
						newRow[key] = discretize(disc, row, disc['inVar'])
			headers.update(newRow)
	
	with open('iddoData/iddo_leoss_format_progression.csv', 'w', newline='') as out:
		headers = list(headers.keys())
		outCsv = csv.DictWriter(out, headers)
		outCsv.writeheader()
		# Convert blanks to *
		for row in data:
			for col in headers:
				if col not in row:
					row[col] = '*'
		outCsv.writerows(data)
	

def main():
	with DB('out/iddo_sample.sqlite') as db:
		makeLeossFormatData(db)

main()