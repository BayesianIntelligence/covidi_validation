from builtins import next
from builtins import str
gOpen = open

# Sometimes you also need access to the auto-fixed table and field names.
# They are put into extras, if you specify that.
def open(csvFns, dbLoc = ':memory:', fixHeaders = True, extras = {}, mappedTableNames = None):
	fieldNames = []
	for i,csvFn in enumerate(csvFns):
		mappedTableName = None
		if mappedTableNames:
			if hasattr(mappedTableNames, 'get'):
				mappedTableName = mappedTableNames.get(csvFn)
			else:
				mappedTableName = mappedTableNames[i]
		#print(mappedTableName)
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
	
def getTableName(csvFn):
	return tableName
def add(db, csvFn, tableName = None, fixHeaders = True):
		#rint('csvFn:',csvFn)
		
		inCsv = csv.reader(csvFile)
			if fixHeaders:
				field = re.sub(r'[^a-zA-Z0-9_]', '_', field)
			fieldPlaces += sep + '?'
		#rint('fields:',fields)
		
			db.execute('insert into '+escapeIdentifier(tableName)+' ('+fields+') values ('+fieldPlaces+')', row)
		return tableName, fieldNames