"""

Interface layer to the database on the Java side.

"""

import sqlite3, re, math

class DB:
	# WAL = write-ahead logging. Generally, safer and faster (particularly with concurrency), but might cause issues over a network, unusual setups, etc.
	def __init__(self, path, disableWal = False):
		if hasattr(path,'execute'):
			self.conn = path
			self.conn.row_factory = sqlite3.Row
		else:
			self.conn = sqlite3.connect(path, isolation_level=None)
			self.conn.execute('pragma journal_mode=wal')
			self.conn.row_factory = sqlite3.Row
		self.debug = False
		self.autocommit = True
		
		self.addMoreFunctions()
	
	def __enter__(self):
		return self
	
	def __exit__(self, type, value, tb):
		self.conn.close()
	
	def commitOff(self):
		self.autocommit = False
		self.query('begin')
	
	def commitOn(self):
		self.autocommit = True
		self.conn.commit()
	
	# To add more functions, just go ahead and add them with self.conn.create_function.
	# They'll auto-overwrite what's there.
	# (And this seems to be relatively cheap, so don't be afraid of them.)
	# Passing '-1' for the number of arguments creates a variadic function.
	def addMoreFunctions(self):
		self.conn.create_function('_ln', 1, math.log)
		self.conn.create_function('_exp', 1, math.exp)
		self.conn.create_function('_pow', 2, math.pow)
		
		def regexp(expr, item):
			reg = re.compile(expr)
			if item is not None:
				return reg.search(item) is not None
			return None
		self.conn.create_function('REGEXP', 2, regexp)
	
		class Prod:
			def __init__(self):
				self.total = 1
			def step(self, value):
				self.total *= value
			def finalize(self):
				return self.total
		self.conn.create_aggregate('_product', 1, Prod)
		class Median:
			def __init__(self):
				self.vals = []
			def step(self, value):
				self.vals.append(value)
			def finalize(self):
				self.vals.sort()
				n = len(self.vals)
				if n == 0:
					return None
				elif n % 2 == 0:
					bottom = n // 2 - 1
					top = bottom + 1
					return (self.vals[bottom] + self.vals[top])/2
				return self.vals[n//2]
		self.conn.create_aggregate('_median', 1, Median)
		class Sd:
			def __init__(self):
				self.count = 0
				self.sum = 0
				self.sumSq = 0
			def step(self, value):
				#print(value)
				try:
					value = float(value)
					self.count += 1
					self.sum += value
					self.sumSq += value**2
				except:
					pass
			def finalize(self):
				return ((self.count*self.sumSq - self.sum**2)/self.count**2)**0.5
		self.conn.create_aggregate('_sd', 1, Sd)
	
	def escapeIdentifier(self, id):
		if id.find("\x00") >= 0:
			raise "NUL characters not allowed in SQL identifiers"
		
		return "\"" + id.replace("\"", "\"\"") + "\""
	
	def query(self, sql, params = None, ignoreErrors = False, keyType = "name"):
		if keyType == "index":
			self.conn.row_factory = None
		else:
			self.conn.row_factory = sqlite3.Row
		
		if params is None: params = []
		if self.debug:
			print("query:",sql,"<br>")
			print("params:",params,"<br>")
			
			#input('Enter to execute query')
			
		if ignoreErrors:
			rs = None
			try:
				rs = self.conn.execute(sql, params)
			except: pass
		else:
			rs = self.conn.execute(sql, params)
		
		# Default to always commit
		if self.autocommit:
			self.conn.commit()
		
		return rs
	
	def handleValType(self, val):
		return bautils.convertNumber(val)
		'''try:
			val = int(val)
		except:
			try:
				val = float(val)
			except: pass
		
		return val'''
	
	# This will be efficient, simple and protect against injections
	def intParams(self, lst):
		sqlFrag = "("
		sep = ""
		for item in lst:
			int(item) # If not int, throw error
			sqlFrag += sep + str(item)
			sep = ","
		
		sqlFrag += ")"
		
		return sqlFrag
	
	# Creates the placeholders string. More general than above function, but involves
	# duplication and is less efficient
	def placeholders(self, lst):
		return "("+("?,"*len(lst))[:-1]+")"

	def queryValue(self, sql, params = None, typeHandling = True, ignoreErrors = False):
		rs = self.query(sql, params, ignoreErrors)
		if rs is None: return None
		row = rs.fetchone()
		if row is None:  return None

		return row[0]
	
	# keyType can be "name" or "index"
	def queryRow(self, sql, params = None, keyType = "name", typeHandling = True, ignoreErrors = False):
		rs = self.query(sql, params, ignoreErrors, keyType)
		row = rs.fetchone()
		if row is None:  return None
		
		if keyType == "name":
			return dict(row)
		elif keyType == "index":
			return list(row)
	
	# keyType can be "name" or "index"
	def queryRows(self, sql, params = None, keyType = "name", typeHandling = True, oneD = False, ignoreErrors = False):
		rs = self.query(sql, params, ignoreErrors, keyType)

		if oneD:
			data = []
			for row in rs:
				data.extend(row)
			return data
		
		rows = rs.fetchall()
		if rows is None:  return None
		
		if keyType == "name":
			return [dict(r) for r in rows]
		elif keyType == "index":
			return [list(r) for r in rows]
		elif keyType == "indexWithHeaders":
			return [[list(r) for r in rows],rows[0].keys()]
	
	# Return a single column (first column)
	def queryColumn(self, sql, params = None, typeHandling = True, ignoreErrors = False):
		return self.queryColumns(sql, params=params, typeHandling=typeHandling, ignoreErrors=ignoreErrors, keyType='index')[0]
	
	# Returns a set of columns (rather than a set of rows)
	def queryColumns(self, sql, params = None, keyType = "name", typeHandling = True, ignoreErrors = False):
		rs = self.query(sql, params, ignoreErrors, keyType)
		
		if keyType == 'name':
			cols = {}
			first = True
			for row in rs:
				if first:
					first = False
					for field in row.keys():
						cols[field] = []
				for field in row.keys():
					cols[field].append(row[field])
		elif keyType == 'index':
			cols = []
			first = True
			for row in rs:
				if first:
					cols = [[] for i in row]
				for i,value in enumerate(row):
					cols[i].append(value)
		
		return cols
		
	# Returns a map of field1 to field2 (which are the first and second fields, respectively, by default)
	def queryMap(self, sql, params = None, ignoreErrors = False, field1 = 0, field2 = 1):
		rs = self.query(sql, params, ignoreErrors)

		map = {}
		for row in rs:
			map[row[field1]] = row[field2]
		
		return map

	def update(self, table, upds, condition, params, ignore = []):
		if not params: params = []

		updStr = ""
		sep = ""
		i = 0
		for field,val in upds.items():
			if field in ignore:  continue
			params.insert(i, val)
			updStr += sep + field + "= ?"
			sep = ", "
			i += 1
	
		if condition:
			condition = " where "+condition
		
		sql = "update "+table+" set "+updStr+condition
		
		if self.debug:
			print("query:",sql,"<br>")
			print("params:",params,"<br>")
		
		self.query(sql, params)
	
	# pk can be string (name of primary key), or an array of strings (names) for a "composite" key
	# replace always returns the row id, even for composite keys
	def replace(self, table, upds, pk, valid = None):
		if isinstance(pk, list):
			pks = pk
		else:
			pks = [pk]
		for pk in pks:
			if pk not in upds:
				print("Primary key '{}' not in |upds|. Use None or -1 if inserting.", pk)
		pkWheres = ' and '.join('{} = ?'.format(pk) for pk in pks)
		recordExists = self.queryValue("select 1 from {} where {}".format(table, pkWheres), [upds[pk] for pk in pks])

		# Filter down to just those that are in valid (if valid specified)
		if valid:
			oldUpds = upds
			upds = {}
			for k in valid:
				if k in oldUpds:
					v = oldUpds[k]
					upds[k] = v
		
		if not recordExists:
			insertFields = [k for k in upds.keys() if k not in pks] if (len(pks)==1 and isinstance(upds[pk],int) and upds[pk]<0) else upds.keys()
			fieldStr = ", ".join(self.escapeIdentifier(f) for f in insertFields)
			placeholdersStr = ("?,"*len(insertFields))[:-1]
			params = [upds[f] for f in insertFields]
			sql = "insert into {} ({}) values ({})".format(self.escapeIdentifier(table), fieldStr, placeholdersStr)
			if self.debug: print(sql, params)
			self.query(sql, params)
			
			return self.queryValue("select last_insert_rowid()")
		else:
			updateFields = upds.keys()
			fieldSetStr = ", ".join(self.escapeIdentifier(k)+" = ?" for k in updateFields)
			params = [upds[f] for f in updateFields] + [upds[pk] for pk in pks]
			sql = "update {} set {} where {}".format(self.escapeIdentifier(table), fieldSetStr, pkWheres)
			if self.debug: print(sql, params)
			self.query(sql, params)
			
			if len(pks)==1:
				return upds[pk]
			else:
				return self.queryValue('select rowid from {} where {}'.format(self.escapeIdentifier(table), pkWheres), [upds[pk] for pk in pks])
	
	'''
	changeSet structure should be: array(
		"changed" => array(
			<pkValue> => array("field1" => val, etc.),
			<pkValue2> etc.
		),
		"inserted" => array(
			array("field1" => val, etc.),
			array("field1" => val, etc.),
			etc.
		),
		"deleted" => array(<pkValue>, <pkValue2>, etc.)
	)
	'''
	def applyTableChanges(self, table, changeSet, pkName):
		newPkMap = {}
		
		for pk, record in changeSet["changed"].items():
			record[pkName] = int(pk)
			self.replace(table, record, pkName)

		for record in changeSet["inserted"]:
			oldPk = record[pkName]
			record[pkName] = None
			newPk = self.replace(table, record, pkName)
			newPkMap[oldPk] = newPk

		for pk in changeSet["deleted"]:
			self.query("delete from "+table+" where "+pkName+" = ?", [pk])

		return newPkMap

	def tableExists(self, table):
		return (self.queryValue("select 1 from sqlite_master where type='table' and name = ?", [table]) is not None)
		
	def fieldNames(self, rs):
		return [r[0] for r in rs.description]
	
	# inTable is either a string name of a table, or a rs
	def writeCsv(self, inTable, outCsvFn, fields = "*", where = None):
		import csv
		with open(outCsvFn, "w", newline='') as outCsvFile:
			
			if type(inTable) == str:
				rs = self.query("select "+",".join(fields)+" from "+inTable+((" where "+where) if where else ""))
			else:
				rs = inTable
			
			# Write headers
			if fields == "*":
				headers = self.fieldNames(rs)
			else:
				headers = fields
			outCsv = csv.DictWriter(outCsvFile, headers)
			outCsv.writeheader()
			
			for row in rs:
				row = dict(row)
				outCsv.writerow(dict((k,row[k]) for k in headers if k in row))

			rs.close()
