from builtins import next
from builtins import strimport sqlite3, csv, re, os
gOpen = open

# Sometimes you also need access to the auto-fixed table and field names.
# They are put into extras, if you specify that.
def open(csvFns, dbLoc = ':memory:', fixHeaders = True, extras = {}, mappedTableNames = None):	db = sqlite3.connect(dbLoc)		tableNames = []
	fieldNames = []
	for i,csvFn in enumerate(csvFns):
		mappedTableName = None
		if mappedTableNames:
			if hasattr(mappedTableNames, 'get'):
				mappedTableName = mappedTableNames.get(csvFn)
			else:
				mappedTableName = mappedTableNames[i]
		#print(mappedTableName)		tableName, tableFieldNames = add(db, csvFn, tableName = mappedTableName, fixHeaders = fixHeaders)
		tableNames.append(tableName)
		fieldNames.append(tableFieldNames)
	
	db.commit()
	
	extras['tableNames'] = tableNames
	extras['fieldNames'] = fieldNames
	
	return db

def escapeIdentifier(id):
	if id.find("\x00") >= 0:
		raise "NUL characters not allowed in SQL identifiers"
	
	return "\"" + id.replace("\"", "\"\"") + "\""
	
def getTableName(csvFn):	csvFn = os.path.basename(csvFn)	tableName = re.sub(r'\.csv$', '', csvFn)	tableName = re.sub(r'^[^a-zA-Z_]', '', tableName)	tableName = re.sub(r'[^a-zA-Z0-9_]', '', tableName)	tableName = re.sub(r'^(\d)', '_$1', tableName)
	return tableName
def add(db, csvFn, tableName = None, fixHeaders = True):	with gOpen(csvFn, 'r', newline='') as csvFile:
		#rint('csvFn:',csvFn)
		
		inCsv = csv.reader(csvFile)				tableName = tableName or getTableName(csvFn)				fieldNames = next(inCsv)				for i,f in enumerate(fieldNames):			num = 0			while f in fieldNames[:i]:				f = fieldNames[i] + str(num)				num += 1			fieldNames[i] = f				fields = ''		fieldPlaces = ''		sep = ''		for field in fieldNames:
			if fixHeaders:
				field = re.sub(r'[^a-zA-Z0-9_]', '_', field)			fields += sep + escapeIdentifier(field)
			fieldPlaces += sep + '?'			sep = ','			
		#rint('fields:',fields)
				db.execute('create table '+escapeIdentifier(tableName)+' ('+fields+')')				for row in inCsv:			#rint('row:',row)
			db.execute('insert into '+escapeIdentifier(tableName)+' ('+fields+') values ('+fieldPlaces+')', row)
		return tableName, fieldNames