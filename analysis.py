import _env, re, numpy
from bidb import DB
from htm import n

sampleDbLoc = 'out/iddo_original.sqlite'

def countryMeasurements():
	with DB(sampleDbLoc) as db:
		cols = db.queryColumns('pragma table_info("timeSeries")')['name']
		
		out = n('div')
		
		countries = db.queryRows('select distinct country from dm',oneD=True)
		#countriesPos = [(c,i) for i,c in countries.items()]
		freqMatrix = n('table.data', n('tr',
			n('th', 'Measure\\Country'),
			[n('th', c) for c in countries],
			n('th', "Total"),
		))
		out.append(freqMatrix)
	
		for col in cols:
			tb = db.queryMap(f'select country, count(*) from timeSeries left join subject on timeSeries.usubjid = subject.usubjid where timeSeries.{col} is not null group by country')
			
			total = db.queryMap(f'select "TOTAL" as COUNTRY, count(*) from timeSeries left join subject on timeSeries.usubjid = subject.usubjid where coalesce(timeSeries.{col},"") <> ""')
			tb.update(total)
			freqMatrix.append(
				n('tr', n('th', col), [n('td', tb.get(c)) for c in countries+["TOTAL"]])
			)
			print(tb)
			
			# out.append(n('h2', col))
			# out.append(n('table', data=tb))
	
		return out

def signif(x, p):
	np = numpy
	x = np.asarray(x)
	x_positive = np.where(np.isfinite(x) & (x != 0), np.abs(x), 10**(p-1))
	mags = 10 ** (p - 1 - np.floor(np.log10(x_positive)))
	return np.round(x * mags) / mags

def calcQuantiles(vals, interval = 0.25):
	numGroups = (1/interval)
	return [signif(numpy.quantile(vals, i/numGroups),3) for i in range(0,int(numpy.ceil(numGroups))+1)]

def unitTypes(table):
	dbTable = table
	with DB(sampleDbLoc) as db:
		cols = db.queryColumns(f'pragma table_info("{dbTable}")')['name']
		
		out = n('div')
		
		table = n('table.data', n('tr', n('th', 'Measure'), n('th', 'Unit'), n('th', 'Count'), n('th', '0.1 Quantiles')))
		out.append(table)
		
		tableRows = []
		
		for col in cols:
			if not re.search(r'_u$', col):  continue
			
			tb = db.queryRows(f'select "", {col}, count(*), group_concat({re.sub(r"_u","",col)}) as vals from {dbTable} where coalesce({col},"") <> "" group by {col}', keyType="index")
			if len(tb)>0:
				tb[0][0] = col
				
				for row in tb:
					vals = [float(v) for v in re.split(r',', row[3])]
					quantiles = calcQuantiles(vals, 0.1)
					row[3] = str(quantiles)
				
				table.append([n('tr', [n('td', c) for c in row]) for row in tb])
				table.append(n('tr.gap', n('td')))
			
			# out.append(n('table', data=tb))
		
		return out

out = n('div',
	n('style', """
		table.data th { text-align: left; }
		table.data { border-collapse: collapse; }
		table.data th, table.data td { border: solid 1px #ccc; padding: 2px 3px; }
		table.data tr:first-child th { position: sticky; background: white; top: 0; z-index: 5; background-clip: padding-box; } 
		tr.gap td { border: none; height: 6px; }
	"""),
)
#out.append(countryMeasurements())
out.append(unitTypes('subject'))
out.append(unitTypes('timeSeries'))
with open('out/analysis.html', 'w') as outFile:
	outFile.write(out.str())