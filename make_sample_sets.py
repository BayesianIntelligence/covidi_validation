import _env, glob, re, os, numpy, csv, json, sys, localconfig, shutil
from bni_netica import Net
from bidb import DB
import csvdb, time

paths, settings = localconfig.setup()

UNIT_RETEST_SDS = 5
VALUES_TO_REMOVE = r'^(9999|9999\.0)$' # This can be any regular expression

# These databases just contain the original data, that may be filtered
originalDbLoc = paths.out('iddo_original.sqlite')
filteredDbLoc = paths.out('iddo_filtered_to_correct_treatment_times.sqlite')

# These are the databases in which data is processed
fullDbLoc = {
	'source': originalDbLoc,
	'path': paths.out('iddo_full.sqlite'),
}
sampleFromOriginalDbLoc = {
	'source': originalDbLoc,
	'path': paths.out('iddo_sample1000.sqlite'),
	'numPatients': 1000,
	'dmFilter': 'country in (select country from dm where country <> "ZAF" group by country having count(*)>100)',
}
sampleFromFilteredDbLoc = {
	'source': filteredDbLoc,
	'path': paths.out('iddo_sample_filtered1000.sqlite'),
	'numPatients': 1000,
	'dmFilter': 'country in (select country from dm group by country having count(*)>100)',
}
#sampleDbLoc = 'iddo_sample - Copy.sqlite'

# Working DB is what will be used to generate the subject and timeSeries tables
# workingDb = originalDbLoc
workingDb = sampleFromFilteredDbLoc

def makeIddoOriginalDb():
	db = DB(csvdb.open(glob.glob(paths.iddoData('*.csv')), originalDbLoc))
	
	# Add indexes
	tables = db.queryRows('select name from sqlite_master where type = "table"', oneD=True)
	
	for table in tables:
		cols = list(db.queryMap(f'pragma table_info("{table}")').values())
		if 'USUBJID' in cols:
			print(f'Creating USUBJID index for {table}')
			db.query(f'update `{table}` set USUBJID = cast(USUBJID as int)')
			db.query(f'create index {table}_usubjid on `{table}` (usubjid)')

def makeIddoFilteredDb():
	shutil.copyfile(originalDbLoc, filteredDbLoc)
	with DB(filteredDbLoc) as db:
		db.query('create temp table toKeep as select distinct usubjid from `IN` where INEVINTX like "%DAY OF ASSESS%"')
		
		tables = db.queryRows('select name from sqlite_master where type="table"', oneD=True)
		
		for table in tables:
			cols = list(db.queryMap(f'pragma table_info("{table}")').values())
			if 'USUBJID' in cols:
				db.query(f'delete from `{table}` where usubjid not in toKeep')
		
		db.query('drop table toKeep')

def makeSampleDb(sampleDbLoc, numPatients = 1000, dmFilter = None, sourceDb = originalDbLoc):
	with DB(sourceDb) as db, DB(sampleDbLoc) as sampleDb:
		sampleSubjects = db.queryRows(f'select usubjid from dm {"where "+dmFilter if dmFilter else ""} order by random() limit {numPatients}', oneD=True)
		
		tables = db.queryRows('select name from sqlite_master where type = "table"', oneD=True)
		
		sampleDb.query(f'attach "{sourceDb}" as original')
		
		for table in tables:
			cols = list(sampleDb.queryMap(f'pragma original.table_info("{table}")').values())
			if 'USUBJID' in cols:
				cols.remove('USUBJID')
				sampleDb.query(f'create table `{table}` as select cast(usubjid as integer) as USUBJID, {",".join(cols)} from original.`{table}` where cast(usubjid as integer) in '+sampleDb.intParams(sampleSubjects))
				sampleDb.query(f'create index {table}_usubjid on `{table}` (usubjid)')
			else:
				print(f'Table {table} has no USUBJID. Adding whole.')
				sampleDb.query(f'create table `{table}` as select * from original.`{table}`')

def makeSubject():
	'''
	Create a new 'Subject' table, that just collects together all the non-time series information for the
	subject.
	'''
	with DB(workingDb['path']) as db:
		db.query('drop table if exists subject')
		
		createStr = '''create table subject (id integer primary key not null,
			'''
			
		# Demographics, all fields
		columns = db.queryRows('select name from pragma_table_info("dm")', oneD=True)
		# Quite hacky: convert usubjid to integer
		createColumns = db.queryRows('select name || " " || type from pragma_table_info("dm")', oneD=True)
		createStr += ', '.join(createColumns)
		
		# Disposition, just DSDECOD (status)
		createStr += ', DSDECOD'
		
		# Signs and symptoms, "medical history" and "on admission"
		createStr += ', ci_sa_medicalHistory, ci_sa_symptomsOnAdmission'
		
		# Vital signs, Weight, Height and BMI, where available (not sure why in vital signs?)
		# include units for now (FIX: normalise this instead)
		createStr += ', ci_vs_weight, ci_vs_weight_u, ci_vs_height, ci_vs_height_u, ci_vs_bmi, ci_vs_bmi_u'
		
		# Hospitalisation, (Possible) duration, after discharge, and self care needs
		createStr += ', ci_ho_duration, ci_ho_disout, ci_ho_selfcare'
		
		# Comorbs/symptoms as separate fields
		net = Net()
		comorbSymptoms = db.queryRows('select distinct upper(coalesce(nullif(samodify,""),saterm)) from sa where (SACAT = "MEDICAL HISTORY" or SACAT = "SIGNS AND SYMPTOMS AT HOSPITAL ADMISSION") and saoccur = "Y" order by SACAT',oneD=True)
		safeComorbSymptoms = [net.makeValidName(f) for f in comorbSymptoms]
		safeComorbSymptomsMap = dict(zip(safeComorbSymptoms,comorbSymptoms))
		# symptoms = db.queryRows('select distinct upper(coalesce(nullif(samodify,""),saterm)) from sa where SACAT = "SIGNS AND SYMPTOMS AT HOSPITAL ADMISSION" and saoccur = "Y"', oneD=True)
		# safeSymptoms = [net.makeValidName(f) for f in symptoms]
		# safeSymptomMap = dict(zip(safeSymptoms,symptoms))
		
		createStr += ', ' + ', '.join(safeComorbSymptoms)

		createStr += ''')'''
		
		db.query(createStr)
		
		# Fill with data
		# demographics
		db.query(f'insert into subject ({",".join(columns)}) select {",".join(columns)} from dm')

		# Convert to int and then add index (quicker to do now)
		db.query('update subject set usubjid = cast(usubjid as int)')
		db.query('create index subject_usubjid on subject (usubjid)')
		
		# DSDECOD
		db.query('update subject set DSDECOD = (select ds.DSDECOD from ds where subject.usubjid = ds.usubjid)')
		
		# medical history
		# SAOCCUR seems to be Y when a person has the comorb. This also agrees with this:
		# https://rdrr.io/github/ISARICDataPlatform/CovidClinicalDataProcessor/src/R/ImportFunctions.R
		# XXX: Strange sqlite bug with group_concat, distinct and sep argument
		db.query('update subject set ci_sa_medicalHistory = (select group_concat(replace(distinct coalesce(nullif(samodify,""),saterm),"",""), "|") from sa where sa.usubjid = subject.usubjid and SACAT = "MEDICAL HISTORY" and saoccur = "Y")')
		
		#symptoms on admission
		db.query('update subject set ci_sa_symptomsOnAdmission = (select group_concat(replace(distinct coalesce(nullif(samodify,""),saterm),"",""), "|") from sa where sa.usubjid = subject.usubjid and SACAT = "SIGNS AND SYMPTOMS AT HOSPITAL ADMISSION" and saoccur = "Y")')
		
		# height, weight, bmi
		db.query('''update subject set
			ci_vs_height = (select vsorres from vs where vstest = 'Height' and usubjid = subject.usubjid),
			ci_vs_height_u = (select vsorresu from vs where vstest = 'Height' and usubjid = subject.usubjid),
			ci_vs_weight = (select vsorres from vs where vstest = 'Weight' and usubjid = subject.usubjid),
			ci_vs_weight_u = (select vsorresu from vs where vstest = 'Weight' and usubjid = subject.usubjid),
			ci_vs_bmi = (select vsorres from vs where vstest = 'Body Mass Index' and usubjid = subject.usubjid),
			ci_vs_bmi_u = (select vsorresu from vs where vstest = 'Body Mass Index' and usubjid = subject.usubjid)''')
		
		# duration, after discharge and self care
		db.query('''update subject set
			ci_ho_duration = (select max(hoendy)-min(hostdy)+1 from ho where usubjid = subject.usubjid),
			ci_ho_disout = (select hodisout from ho where usubjid = subject.usubjid),
			ci_ho_selfcare = (select selfcare from ho where usubjid = subject.usubjid)
			''')
		
		updates = []
		def reFunc(pattern, val):
			if val:
				return re.search(pattern, val) is not None
			return False
		db.conn.create_function('REGEXP', 2, reFunc)
		for field in safeComorbSymptoms:
			#print(safeComorbSymptomsMap[field])
			updates.append(f"{field} = iif((ci_sa_medicalHistory || '|' || ci_sa_symptomsOnAdmission) regexp '{safeComorbSymptomsMap[field]}', 'Reported', 'ND')")
		# for field in safeSymptoms:
			# updates.append(f"{field} = case when ci_sa_symptomsOnAdmission regexp '{safeSymptomMap[field]}' then 'Reported' else 'ND' end")
		db.query(f'''update subject set {",".join(updates)}''')
		

def makeTreatmentDays(db):
	# XXX: Still no pre-processing of the oxygen treatments to group them together

	# Setup a sequence of numbers for each day in a temp table
	db.query('drop table if exists tempDays')
	db.query('''create table tempDays as 
		with recursive daysTab (day) as (
		select -10
		union all
		select day+1 from daysTab limit 1000
		)
		select day from daysTab''')
	
	schema = '(usubjid integer, day integer, ci_in_trt text, ci_in_modify text, ci_in_cat text)'
	# If instdy and inendy are specified, use them and add all in between days
	inQuery1 = '''select usubjid, day, group_concat(intrt, '|') as ci_in_trt, group_concat(incat, '|') as ci_in_cat, group_concat(inmodify, '|') as ci_in_modify
	from tempDays
		left join
			(select usubjid, cast(instdy as int) instdy, cast(inendy as int) inendy, intrt, incat, inmodify from `in` where inoccur = 'Y' and `in`.instdy <> '' and `in`.inendy <> '')
			on instdy <= tempDays.day and tempDays.day <= inendy
	where usubjid is not null
	group by usubjid, day
	order by usubjid'''
	db.query('drop table if exists tempIn_sub1')
	db.query(f'create table tempIn_sub1 {schema}')
	db.query(f'insert into tempIn_sub1 {inQuery1}')
	# else if indy and indur specified, treat indy as end day and indur as duration (NOTE: no grouping done)
	inQuery2 = '''select usubjid, day, group_concat(intrt, '|') as ci_in_trt, group_concat(incat, '|') as ci_in_cat, group_concat(inmodify, '|') as ci_in_modify
	from tempDays
		left join 
			(select usubjid, cast(indy as int) indy, cast(replace(lower(indur),'p','') as int) indur, intrt, incat, inmodify
				from `in` where inoccur = 'Y' and `in`.instdy = '' and `in`.indy<>'' and `in`.indur <>'')
			on indy-indur <= tempDays.day and tempDays.day <= indy
	where usubjid is not null
	group by usubjid, day
	order by usubjid'''
	db.query('drop table if exists tempIn_sub2')
	db.query(f'create table tempIn_sub2 {schema}')
	db.query(f'insert into tempIn_sub2 {inQuery2}')
	# else if indy specified multiple times for a specific incat (not intrt), then collect sequence
	# of indy for subject, and "interpolate" binary, assuming 1 for day -1000 and 0 for day 1000
	# (Could also specify unknown) - Make use of the Y and Ns
	# (The aliases are so that things come back lower case)
	inQuery3 = '''select usubjid usubjid, cast(indy as int) indy, cast(replace(lower(indur),'p','') as int) indur, intrt intrt, incat incat, inmodify inmodify, inoccur inoccur
	from `in`
	where (inoccur = 'Y' or inoccur = 'N') and `in`.instdy = '' and `in`.indy<>'' and `in`.indur=''
	order by usubjid, indy
	'''
	# It's easier to do this particular interpolation in code than in SQL
	t = time.time()
	rows = db.queryRows(inQuery3)
	print('query time:',time.time()-t)
	
	subjects = []
	usubjid = None
	day = None
	minTreatDay = 1
	minDay = -10
	maxDay = 1000
	for row in rows:
		if row['usubjid'] != usubjid:
			usubjid = row['usubjid']
			subject = {
				'days': {k: {'trt':'','cat':'','modify':''} for k in range(minDay,maxDay)},
				'lastSeen': {},
				'usubjid': usubjid,
			}
			subjects.append(subject)
		if row['indy'] != day:
			day = row['indy']
		# The interpolation minimum day will either be the first possible treatment day
		# OR if the current day is earlier than that, then the minDay used for the data set overall
		interpMinDay = minTreatDay
		if int(day) < minTreatDay:
			interpMinDay = minDay
		# Skip days after 1000 (as don't make sense)
		if int(day) >= maxDay:
			print(f"Day > 1000 (day {day}) skipped")
			continue
		
		for fieldName in ['trt','cat','modify']:
			field = f'in{fieldName}'
			if row[field]:
				trtKey = f'{fieldName}:{row[field]}'
				
				if trtKey in subject['lastSeen']:
					lastSeenDay = subject['lastSeen'][trtKey]['day']
					lastSeenOccur = subject['lastSeen'][trtKey]['occur']
				else:
					lastSeenDay = interpMinDay-1
					lastSeenOccur = 'N'
				
				if row['inoccur'] == 'Y':
					# Spread Ys from the current day back to the last change
					startDay = max(interpMinDay,lastSeenDay+1)
					endDay = day
				elif row['inoccur'] == 'N' and lastSeenOccur == 'Y':
					# Spread Ys from the day of last change forward to the day before the current day
					startDay = max(interpMinDay,lastSeenDay+1)
					endDay = day-1
				else: # row['inoccur'] = 'N' and lastSeenOccur = 'N':
					# i.e. Don't record anything for this treatment
					startDay = 0
					endDay = -1
				for d in range(startDay, endDay+1):
					subject['days'][d][fieldName] += '|' + row[field]
				
				subject['lastSeen'][trtKey] = {'day': day, 'occur': row['inoccur']}
	
	print('process time:',time.time()-t)
	
	# Take the above, and put it into a temp table
	db.commitOff()
	db.query('drop table if exists tempIn_sub3')
	db.query(f'create table tempIn_sub3 {schema}')
	toInsert = []
	for subject in subjects:
		for day, fields in subject['days'].items():
			if fields['trt']:
				toInsert.append([subject['usubjid'], day, fields['trt'], '', ''])
	db.conn.executemany('insert into tempIn_sub3 values (?,?,?,?,?)', toInsert)
	db.commitOn()
	
	print('insert time:',time.time()-t)
	
	# Now merge them all !
	db.query('drop table if exists tempIn')
	db.query(f'create table tempIn {schema}')
	db.query('''insert into tempIn select usubjid, day, group_concat(ci_in_trt,'|') ci_in_trt, group_concat(ci_in_cat,'|') ci_in_cat, group_concat(ci_in_modify,'|') ci_in_modify
	from (select * from tempIn_sub1 union select * from tempIn_sub2 union select * from tempIn_sub3)
	group by usubjid, day''')
	print('merged:', time.time()-t)

	# while 1:
		# v = input()
		# if not v: break
		# eval(v)
	
	# sys.exit()
	# db.query('''insert into tempIn2 (usubjid,day,ci_in_trt) select usubjid, day, intrt
		# from tempDays
			# left join `in` on instdy <= tempDays.day and tempDays.day <= inendy''')
	# db.query('create table tempIn as select usubjid, coalesce(nullif(indy,""),instdy) as day, group_concat(intrt, "|") as ci_in_trt, group_concat(inmodify, "|") as ci_in_modify, group_concat(incat, "|") as ci_in_cat from `in` where inoccur = "Y" group by usubjid, coalesce(nullif(indy,""),instdy)')

def cleanDbn2Slice():
	with DB(workingDb['path']) as db:
		# Update disposition field for _t1
		db.query(r'''
		update dbn2slice as outer
			set ci_ds_decod_t1 = (select ci_ds_decod from timeSeries
				where ci_ds_decod in ('DEATH','DISCHARGED','TRANSFERRED') and outer.usubjid_t0 = usubjid order by day desc limit 1)
			where sliceStart_t1 > (select max(`day`) from timeSeries
				where ci_ds_decod in ('DEATH','DISCHARGED','TRANSFERRED') and outer.usubjid_t0 = usubjid)''')

def cleanTimeSeries():
	with DB(workingDb['path']) as db:
		# Add disposition to the last known day, when a recorded day for the disposition is missing
		db.query('''
			update timeSeries as outer
			set ci_ds_decod = (select group_concat(dsdecod, "|") from DS where outer.usubjid = usubjid)
			where day >= (select max(day) from timeSeries as inner where outer.usubjid = usubjid)
				and usubjid in (select usubjid from timeSeries group by usubjid having group_concat(ci_ds_decod) is null)
		''')

		# Delete any days that happen after discharged/death/transferred
		db.query(r'''
		delete from timeSeries as outer
			where day > (select max(`day`) from timeSeries
				where ci_ds_decod in ('DEATH','DISCHARGED','TRANSFERRED') and outer.usubjid = usubjid)''')
		
		# Replace invalid/missing values with blanks
		vars = db.queryRows('select name from pragma_table_info("timeSeries") where not name regexp "^(id|USUBJID)$"', oneD=True)
		for var in vars:
			#print(f'''update timeSeries set {var} = '' where {var} regexp '{VALUES_TO_REMOVE}' ''')
			db.query(f'''update timeSeries set {var} = NULL where cast({var} as text) regexp '{VALUES_TO_REMOVE}' ''')
	
def makeTimeSeries():
	with DB(workingDb['path']) as db:
		# Need to handle dy (day), stdy (startday) and endy (endday) in
		# creating the time series. For now, just using stdy or dy -> day, whichever
		# is available
		
		db.query('drop table if exists timeSeries')
		createStr = '''create table timeSeries (id integer primary key not null,
			usubjid integer, day integer'''
			
		# Commented out because too much variation across tables
		# tables = db.queryRows('select name from sqlite_master where type = "table"', oneD=True)
		# tables = ['HO']

		# for table in tables:
			# omitCols = ['STUDYID','DOMAIN','USUBJID']
			# columns = list(db.queryMap(f'pragma table_info("{table}")').values())
			
			# for col in columns:
				# if col in omitCols:  continue
				
				
				# createStr += ', '+col
				# omitCols.append(col)
		
		# Hospitalisation
		createStr += ', ci_ho_type'

		# Disposition/death
		createStr += ', ci_ds_decod'

		# Vital stats
		vsTests = db.queryRows('select distinct vstest from vs group by vstest', oneD=True)
		ciVsTests = ['ci_'+re.sub(r'\W', '_', t) for t in vsTests]
		ciVsTestsU = ['ci_'+re.sub(r'\W', '_', t)+'_u' for t in vsTests]
		ciVsTestsAndU = [j for i in zip(ciVsTests,ciVsTestsU) for j in i]
		createStr += ', ' + (', '.join(ciVsTestsAndU))
		
		# Labs
		lbTests = db.queryRows('select distinct lbtestcd from lb group by lbtest', oneD=True)
		ciLbTests = ['ci_'+re.sub(r'\W', '_', t) for t in lbTests]
		ciLbTestsU = ['ci_'+re.sub(r'\W', '_', t)+'_u' for t in lbTests]
		ciLbTestsAndU = [j for i in zip(ciLbTests,ciLbTestsU) for j in i]
		createStr += ', ' + (', '.join(ciLbTestsAndU))
		
		# Interventions, these look messy, so just copying across the 3 key columns
		# (Expand to separate columns?)
		createStr += ', ci_in_trt, ci_in_modify, ci_in_cat'
		
		# Complications
		# (Expand to separate columns?)
		createStr += ', ci_sa_complications'
		
		createStr += ''')'''
		
		#print(createStr)
		db.query(createStr)
		db.query('create index timeSeries_usubjid on timeSeries (usubjid)')
		
		# Fill with data. I can't think of a way to do this without temp tables (of at least some sort!)
		db.query('drop table if exists tempHo')
		# stdy (start day) is likely an accurate assessment of the first day, whereas dy is just the
		# collection day. But, if no stdy, no real alternative
		db.query('create table tempHo as select cast(usubjid as int) as usubjid, cast(coalesce(nullif(hostdy,""),hody) as int) as day, group_concat(hodecod, "|") as ci_ho_type from ho where hooccur = "Y" or (hostdy <> "") group by usubjid, coalesce(nullif(hostdy,""),hody)')
		
		db.query('drop table if exists tempDs')
		# Skip empty dsstdy entries
		# (But add them back at the end)
		db.query('''create table tempDs as select cast(usubjid as int) as usubjid, cast(coalesce(nullif(dsstdy,""),dsdy) as int) as day, group_concat(dsdecod, "|") as ci_ds_decod from ds
			where coalesce(nullif(dsstdy,""),dsdy) <> ""
			group by usubjid, coalesce(nullif(dsstdy,""),dsdy)''')
		
		# Make the treatment timings table
		# This was seriously long, so moved into its own function
		makeTreatmentDays(db)
		
		vsCols = [f'avg(case when vstest = "{t}" then vsorres end) as {ciVsTests[i]}, max(case when vstest = "{t}" then vsorresu end) as {ciVsTestsU[i]}' for i,t in enumerate(vsTests)]

		db.query('drop table if exists tempVs')
		db.query('create table tempVs as select cast(usubjid as int) as usubjid, cast(vsdy as int) as day, '+(','.join(vsCols))+' from vs group by usubjid, vsdy')
		
		
		lbCols = [f'avg(case when lbtestcd = "{t}" then lborres end) as {ciLbTests[i]}, max(case when lbtestcd = "{t}" then lborresu end) as {ciLbTestsU[i]}' for i,t in enumerate(lbTests)]

		db.query('drop table if exists tempLb')
		db.query('create table tempLb as select cast(usubjid as int) as usubjid, cast(lbdy as int) as day, '+(','.join(lbCols))+' from lb group by usubjid, lbdy')
		
		# XXX: Note that complications with no associated day are currently dropped
		db.query('drop table if exists tempCo')
		db.query("create table tempCo as select cast(usubjid as int) as usubjid, cast(sady as int) as day, group_concat(saterm, '|') as ci_sa_complications from sa where saoccur = 'Y' and SACAT = 'COMPLICATIONS' and sady <> '' group by usubjid, sady")
		
		tempTbs = re.split(r'\s+', 'tempHo tempDs tempIn tempVs tempLb tempCo')
		db.query('drop table if exists tempSubjectDayIndex')
		db.query('create table tempSubjectDayIndex as ' + (' union '.join([f"select distinct usubjid, day from {tb}" for tb in tempTbs])))
		print('indexes:')
		t = time.time()
		for tb in tempTbs+['tempSubjectDayIndex']:
			db.query(f'create index {tb}_usubjid on {tb} (usubjid)')
			db.query(f'create index {tb}_day on {tb} (day)')
			db.query(f'create index {tb}_usubjidday on {tb} (usubjid, day)')
		print('index time:', time.time()-t)
		
		

		jointQuery = f'''select tempSubjectDayIndex.usubjid, tempSubjectDayIndex.day, ci_ho_type, ci_ds_decod, ci_in_trt, ci_in_modify, ci_in_cat, ci_sa_complications, {', '.join(ciVsTestsAndU)}, {', '.join(ciLbTestsAndU)}
			from tempSubjectDayIndex
				left join tempHo on tempSubjectDayIndex.usubjid = tempHo.usubjid and tempSubjectDayIndex.day = tempHo.day
				left join tempDs on tempSubjectDayIndex.usubjid = tempDs.usubjid and tempSubjectDayIndex.day = tempDs.day
				left join tempIn on tempSubjectDayIndex.usubjid = tempIn.usubjid and tempSubjectDayIndex.day = tempIn.day
				left join tempVs on tempSubjectDayIndex.usubjid = tempVs.usubjid and tempSubjectDayIndex.day = tempVs.day
				left join tempLb on tempSubjectDayIndex.usubjid = tempLb.usubjid and tempSubjectDayIndex.day = tempLb.day
				left join tempCo on tempSubjectDayIndex.usubjid = tempCo.usubjid and tempSubjectDayIndex.day = tempCo.day'''
		
		# This hack is required because sqlite doesn't support outer joins :(
		def mod(qry, tb):
			qry = re.sub(tb, 'tempXXX', qry)
			qry = re.sub('tempHo', tb, qry)
			qry = re.sub('tempXXX', 'tempHo', qry)
			return qry
		
		print('main insert:')
		t = time.time()

		db.query(f'insert into timeSeries (usubjid, day, ci_ho_type, ci_ds_decod, ci_in_trt, ci_in_modify, ci_in_cat, ci_sa_complications, {", ".join(ciVsTestsAndU)}, {", ".join(ciLbTestsAndU)})'
			+jointQuery
			# +' UNION '
			# +mod(jointQuery, 'tempIn')
			# +' UNION '
			# +mod(jointQuery, 'tempVs')
			# +' UNION '
			# +mod(jointQuery, 'tempLb')
			# +' UNION '
			# +mod(jointQuery, 'tempCo')
			# +' UNION '
			# +mod(jointQuery, 'tempDs')
		)
		
		print('main insert done:', time.time() - t)
	
def runStep(func, msg, skipWhen = False, skipMsg = None):
	if skipWhen:
		if skipMsg:
			print(skipMsg)
	else:
		print(msg)
		t = time.time()
		func()
		print(f'Done ({round(time.time()-t,2)}s)')

# FIX: Molar units are dependent on the thing being measured when converted to mass, volume, etc.
# Most conversions use https://unitslab.com
def mapUnit(val, unit, newUnit = None, variable = None):
	unit = unit.strip() if isinstance(unit, str) else unit
	if unit == newUnit:  return val, unit
	unitMap = {
		'YEARS': {
			'MONTHS': 1/12,
			'DAYS': 1/365.25,
		},
		'C': {'F': lambda f: (f-32)/1.8}, #fahrenheit to celsius
		'cm': {
			'm': 100,
			'mm': 1/10,
		},
		'kg': {'g': 1/1000},
		'U/L': {
			'IU/L': 1, #international units/litre to units/litre, is 1:1 for AST/ALT: https://onlineconversion.vbulletin.net/forum/main-forums/convert-and-calculate/9258-help-in-conversion-for-iu-l-versus-u-l
			# The following are valid for ALT, AST, CK, LDH (and may not be valid for anything else)
			# However, I think in all cases, the ukat/L, nkat/L and mg/dL entries are actually just U/L entries with mislabelled units
			'ukat/L': 60,
			'nkat/L': 0.06,
			'mg/dL': {'AST': 1}, # XXX - I think this unit doesn't make sense for ALT, AST and CK. So just using 1, because otherwise these cases' values look OK as U/L
			# Creatine Kinase --- all errors, I think
			'MILLIGRAM PER LITRE': 1,
			'mg/L': 1,
			'ng/L': 1,
			'ng/mL': 1,
		},
		'mmol/L': {
			'mEq/L': 1, # Same, https://unitslab.com/node/42
			'mg/dL': {'GLUC': 1/18, 'LACTICAC': 0.1110, 'UREAN': 0.3571},
			'mg/dLmg/dL': {'LACTICAC': 0.1110}, # typo (XXX add a pre-filter for typos)
			'ml/dl': {'LACTICAC': 0.1110, 'UREAN': 0.3571}, # both typos
			'ml/dL': {'LACTICAC': 0.1110, 'UREAN': 0.3571}, # both typos
			'ng/dL': {'LACTICAC': 0.1110}, # typo
			'g/dL': {'ALB': 0.1505, 'UREAN': 1}, # typo for UREAN?
			'MILLIMOLE PER LITRE': 1,
			'millimol per litre': 1,
			'millimole per litre': 1,
			'mmo/L': 1,
			'mmol per litre': 1,
			'mmol/dL': 1, # typo? (K)
			'mmol/dl': 1, # typo?
			'mmol/mL': 1, # typo?
			'mmoll/l': 1,
			'mol/L': 1, # typo?
			'mol/l': 1, # typo?
			'MMOL/L': 1,
		},
		'mEq/L': {
			'mmol/L': 1,
			'MMOL/L': 1,
			'MILLIMOLE PER LITRE': 1,
			'millimol per litre': 1,
			'millimole per litre': 1,
			'mmo/L': 1,
			'mmol per litre': 1,
			'mmol/dL': 1, # typo? (K)
			'mmol/dl': 1, # typo?
			'mmol/mL': 1, # typo?
			'mmoll/l': 1,
			'mol/L': 1, # typo?
			'mol/l': 1, # typo?
		},
		'umol/L': {
			'mg/dL': {'BILI': 17.1037, 'CREAT':88.4}, # http://www.sydpath.stvincents.com.au/converter.html
			'md/dL': {'CREAT': 88.4}, # md/dL probably typo
			'umol': {'CREAT': 1}, # assuming /L is missing, as it roughly matches. Lots with this one (4875)
			'mmol/L': {'CREAT': 1}, # There may be errors here too (range much too big), so have set 1:1 with umol/L
			'mg/L': 8.84,
		}, # as above, div by 10
		'mg/L': {
			'mg/dL': 10,
			'umol/L': 1/8.84,
			'ng/L': 1/1000000,
			'ng/dL': 1/100000,
			'ng/dl': 1/100000,
			'ng/mL': 1/1000, # For CRP, there is one instance of this unit, and I think it's wrong
			'ng/mL FEU': 1/1000, 
			'ng/nl': {'DDIMER': 1/1000}, # For DDIMER, pretty sure it's type, so set to 1/1000
			'ug/L': 1/1000,
			'ug/L FEU': 1/1000,
			'ug/l FEU': 1/1000,
			'ug/mL': 1,
			'nmol/L': {'CRP': 0.105},
			'MICROGRAM PER LITRE': 1/1000,
			'micro gram per litre': 1/1000,
			'microgram per litre': 1/1000,
			'microgram/L (ug/L)': 1/1000,
			'µg/m': 1, # shrug
		},
		'ng/mL': {
			'ug/L': 1,
			'ug/mL': 1000,
			'pg/mL': {'PCT': 1, 'default': 1/1000}, # I think PCT is mislabelled
			'ng/dL': 1/100,
			'ng/dl': 1/100,
			'ng/100mL': 1/100,
			'ng/L': 1/1000,
			'MICROGRAM PER LITRE': 1,
			'micro gram per litre': 1,
			'microgram per litre': 1,
			'microgram/L (ug/L)': 1,
			'nmol/L': {'FERRITIN': 1}, # This should be 1 nmol/L = 445 ng/mL, but I believe it's mislabelled
			'mIU/L': 1, # typo?
			# Troponin specific
			'<0.05 ug/L': 1,
			'LESS THAN 0.05 ug/L': 1,
			'U/L': {'TROPONIN': 1},
			'UG/L': {'TROPONIN': 1},
			'less than 0.05 ug/L': 1,
			'mg/dL': 1, #typo?
			'mg/l': 1, #typo?
			'ng/100mL': 1/100,
		},
		'pg/mL': {
			'ng/L': 1,
		},
		'g/dL': {
			'g/L': 1/10,
			'mg/dL': 1/1000,
			'mmol/L': {'HGB': 1.6114}, #HGB monomer, specifically
		},
		'g/L': {
			'mg/dL': 10/1000,
		},
		'mg/dL': {
			'g/L': 1000/10,
		},
		'%': {
			'fraction of 1': 100,
			'L/min': lambda l: 20 + 4*l, # Not exact: https://www.biomadam.com/how-to-calculate-fio2-from-liters
			'L/L': 100,
			'proportion': 100,
		},
		'L/min': {
			'fraction of 1': lambda f: (max(f,0.2)*100-20)/4,
			'%': lambda p: (max(p,20)-20)/4, # Not exact: https://www.biomadam.com/how-to-calculate-fio2-from-liters
		},
		'mm/h': {'mm/hr': 1}, # mm per hr. I would have thought mm/hr would be more standard
		'mmHg': {'kPa': 7.5},
		# These aren't correct at all, but in the data set, they all look like the same unit (except /uL)
		'10^9/L': { # density of units
			'/mm3': 1, #1/1000,
			'mm3': 1, #1/1000,
			'10(3)/mm(3)': 1, 
			'/uL': 1/1000,
			'10^3/uL': 1,
			'cells/uL': {'WBC': 1, 'LYM': 1/1000, 'NEUT': 1/1000, 'PLAT': 1},
			'10^9 cells/L': 1,
			'10^6/L': 1/1000,
			'225': 1, # typo
			'G/L': 1,
		},
	}
	
	if newUnit in unitMap and unit in unitMap[newUnit]:
		conv = unitMap[newUnit][unit]
		# Might be specialised depending on the variable used
		if isinstance(conv, dict):
			if not conv.get(variable, conv.get('default')):
				print('No conversion or default for ', variable, unit, newUnit) 
			conv = conv.get(variable, conv.get('default'))
		try:
			val = float(val)
			if callable(conv):
				newVal = conv(val)
			else:
				newVal = val * conv
		
			return newVal,newUnit
		except ValueError as e:
			pass #print('x', val, variable)
		
	return val,unit

# targetUnits = {
	# 'ci_ALT_u': 'U/L',
	# 'ci_AST_u': 'U/L',
	# 'ci_BICARB_u': 'mmol/L',
	# 'ci_BILI_u':
	# 'ci_DDIMER': 'mg/L',
	# 'ci_TROPONI': 'mg/L',
	# 'ci_TROPONIN': 'mg/L',
# }

# SM: Should have normalised AGE field, as it requires lots of special casing here
def updateTableUnits(table):
	GUESS_UNIT = False
	print (workingDb)
	with DB(workingDb['path']) as db:
		db.conn.isolation_level = None
		cols = db.queryColumns(f'pragma table_info({table})')['name']
		
		# Build the (normal-assumed) statistics first for each unit
		if GUESS_UNIT:
			colUnitStats = {}
			for col in cols:
				if not re.search(r'_u|AGEU$', col):  continue
				unitCol = col
				valCol = re.sub(r"_u$","",col)
				valCol = re.sub(r"AGEU","AGE",valCol)
				
				unitStats = db.queryRows(f'select {unitCol} as unit, avg({valCol}) as mean, _sd({valCol}) as sd, count(*) as countForUnit from {table} where coalesce({unitCol},"")<>"" group by {unitCol}')
				
				print(unitStats)
				colUnitStats[col] = {}
				for row in unitStats:
					colUnitStats[col][row['unit']] = row
			

		for col in cols:
			if not re.search(r'_u|AGEU$', col):  continue
			unitCol = col
			valCol = re.sub(r"_u$","",col)
			valCol = re.sub(r"AGEU","AGE",valCol)

			# targetUnit is most frequent unit
			targetUnit = db.queryValue(f'select {unitCol} from {table} where coalesce({unitCol},"")<>"" group by {unitCol} order by count(*) desc limit 1')
			print(f'Target/most common unit for {unitCol} is {targetUnit}')
			
			if valCol == 'AGE':
				rs = db.queryRows(f'select id, AGE, AGEU, AGETXT from {table}')
			else:
				rs = db.queryRows(f'select id, {valCol}, {unitCol} from {table}')
			db.commitOff()
			for row in rs:
				value = row[valCol]
				unit = row[unitCol]
				if valCol == 'AGE':
					value = row['AGE'] or re.sub(r'\+', '', row['AGETXT'])
				newVal, newUnit = mapUnit(value, unit, targetUnit, re.sub(r'^ci_', '', valCol))
				
				# Fix the unit if it seems anomalous either before conversion to the standard unit or after
				if GUESS_UNIT:
					try:
						value = float(value)
						newVal = float(newVal)
					except: pass
					if isinstance(value, float) and unit:
						us = colUnitStats[col][unit]
						numSdsBefore = abs(value - us['mean'])/us['sd'] if us['sd'] else 1000 # i.e. 1000 SDs --- which is pretty abnormal
						us = colUnitStats[col][newUnit]
						numSdsAfter = abs(newVal - us['mean'])/us['sd'] if us['sd'] else 1000
						#print(numSdsBefore, numSdsAfter)
						# Greater than 5 SDs seems suspect
						if numSdsBefore > UNIT_RETEST_SDS or numSdsAfter > UNIT_RETEST_SDS:
							minUnit = unit
							minSds = numSdsBefore
							for us in colUnitStats[col].values():
								if us['sd']:
									thisNumSds = abs(value - us['mean'])/us['sd']
									if thisNumSds < minSds:
										minSds = thisNumSds
										minUnit = us['unit']
							
							if minUnit != newUnit:
								print(f'Treating {value} as {minUnit} rather than {unit}')
								newVal, newUnit = mapUnit(value, minUnit, targetUnit, re.sub(r'^ci_', '', valCol))
						
				if newUnit != row[unitCol]:
					#print(f'Converting {row[unitCol]} to {newUnit} in {table}.{valCol}')
					db.query(f'''update {table}
						set {valCol} = ?,
							{unitCol} = ?
						where id = ?''', [newVal, newUnit, row["id"]])
			db.commitOn()

def addComputedFields():
	with DB(workingDb['path']) as db:
		# NLR
		if not db.queryValue('select 1 from pragma_table_info("timeSeries") where name = "ci_NLR"'):
			db.query('alter table timeSeries add column ci_NLR float')
		db.query('update timeSeries set ci_NLR = ci_NEUT/ci_LYM')
		
		# MAP
		if not db.queryValue('select 1 from pragma_table_info("timeSeries") where name = "ci_MAP"'):
			db.query('alter table timeSeries add column ci_MAP float')
		db.query('update timeSeries set ci_MAP = (ci_Systolic_Blood_Pressure + 2*ci_Diastolic_Blood_Pressure)/3')
		
		# ICU (at any time)
		if not db.queryValue('select 1 from pragma_table_info("subject") where name = "ci_ICU"'):
			db.query('alter table subject add column ci_ICU text')
		db.query('update subject set ci_ICU = coalesce((select "Reported" from timeSeries where usubjid = subject.usubjid and ci_ho_type regexp "^(INTENSIVE CARE UNIT)$" limit 1),"ND")')
		
		# Invasive ventilation (at any time)
		if not db.queryValue('select 1 from pragma_table_info("subject") where name = "ci_InvVent"'):
			db.query('alter table subject add column ci_InvVent text')
		db.query('update subject set ci_InvVent = coalesce((select "Reported" from timeSeries where usubjid = subject.usubjid and ci_in_trt regexp "(^|\|)(INVASIVE VENTILATION|INVASIVE VENTILATION \(ANY\)|PRONE VENTILATION)" limit 1),"ND")')
		
def makeBaseline(baselineFilter = 'day >= -10 and day <=2', tableName = 'baseline'):
	with DB(workingDb['path']) as db:
		db.query(f'drop table if exists {tableName}')

		cols = db.queryColumns('pragma table_info("timeSeries")')['name']
		
		# Skip id and usubjid
		cols = cols[2:]
		
		# cols = ['ci_Oxygen_Saturation']
		
		firstVals = []
		for col in cols:
			firstVals.append(f'first_value({col}) over win1 as {col}_bl')
	
		db.query(f'''create table {tableName} as
			select distinct usubjid, {", ".join(firstVals)}
			from timeSeries
			where {baselineFilter}
			window win1 as (partition by usubjid order by day range between unbounded preceding and unbounded following)''')
		
		db.query(f'create index {tableName}_usubjid on {tableName} (usubjid)')

# minDay = first day of first slice. if windowSize = 3, minDay = -1 and refPoint = 'mid', then the first slice starts centred at day 0
# advanceBy = day|window|transition|<num days>
# transitionSize: Only need to set if different to window size
# choose = nearest|interpolated|worst|<sql agg func> (note: worst and interpolated not yet implemented)
# refPoint = mid|start|end
# specialCols: dictionary for handling special columns. Each column can have their own custom choose/refPoint.
def makeDbn2Slice(windowSize = 3, minDay = -1, advanceBy = 'day', choose = 'nearest', refPoint = 'mid', transitionSize = None,
		specialCols = {}):
	with DB(workingDb['path']) as db:
		db.query('drop table if exists dbn2Slice')

		cols = db.queryColumns('pragma table_info("timeSeries")')['name']
		
		db.query(f'''create table dbn2slice (sliceStart_t0, sliceEnd_t0, sliceStart_t1, sliceEnd_t1, {", ".join(f'{c}_t0, {c}_t1' for c in cols)})''')
		db.query('create index dbn2slice_usubjid_t0 on dbn2slice (usubjid_t0)')
		db.query('create index dbn2slice_day_t0 on dbn2slice (day_t0)')
		db.query('create index dbn2slice_usubjid_t1 on dbn2slice (usubjid_t1)')
		db.query('create index dbn2slice_day_t1 on dbn2slice (day_t1)')
		
		if transitionSize is None:  transitionSize = windowSize
		
		dayt0Start = minDay
		dayt0End = dayt0Start + windowSize-1
		dayt1Start = dayt0Start + transitionSize
		dayt1End = dayt1Start + windowSize-1
		while 1:
			# if no recs from t1 on, stop
			numRecs = db.queryValue(f'select count(*) from timeSeries where day >= {dayt1Start}')
			if not numRecs:  break
			
			if refPoint == 'start':
				refPointt0 = dayt0Start
				refPointt1 = dayt1Start
			elif refPoint == 'end':
				refPointt0 = dayt0End
				refPointt1 = dayt1End
			else: # refPoint = 'mid'
				refPointt0 = (dayt0Start+dayt0End)/2
				refPointt1 = (dayt1Start+dayt1End)/2

			if choose == 'nearest':
				aggFunc = 'first_value'
				nearestSlicet0Point = f'order by abs(day - {refPointt0})'
				nearestSlicet1Point = f'order by abs(day - {refPointt1})'
				sqlWindowt0 = f'window SUBJECTTIMEWINDOW as (partition by usubjid {nearestSlicet0Point})'
				sqlWindowt1 = f'window SUBJECTTIMEWINDOW as (partition by usubjid {nearestSlicet1Point})'
			# elif choose == 'worst':
				# pass
			else: # if choose == sql agg func
				aggFunc = choose
				nearestSlicet0Point = ''
				nearestSlicet1Point = ''
				sqlWindowt0 = ''
				sqlWindowt1 = ''
			
			# The window names are the same, because they live in different subqueries (and it's 
			# easier for everything else)
			wint0 = "over SUBJECTTIMEWINDOW" if sqlWindowt0 else ""
			wint1 = "over SUBJECTTIMEWINDOW" if sqlWindowt1 else ""
			mainColSelect = ''
			t0ColSelect = ''
			t1ColSelect = ''
			sep = ''
			for c in cols:
				if specialCols.get(c):
					expression = specialCols.get(c)
				else:
					expression = f'{aggFunc}({c}) {wint0}'
				mainColSelect += sep + f'ts1.{c}, ts2.{c}'
				t0ColSelect += sep + f'{expression} as {c}'
				t1ColSelect += sep + f'{expression} as {c}'
				sep =', '
			#db.debug = True
			#print(dayt0Start, dayt0End, ' - ', dayt1Start, dayt1End)
			db.query(f'''insert into dbn2slice
				select {dayt0Start}, {dayt0End}, {dayt1Start}, {dayt1End}, {mainColSelect}
				from (select * from (select {t0ColSelect} from timeSeries where day >= {dayt0Start} and day < {dayt0End} {sqlWindowt0}) group by usubjid) as ts1
					left join (select * from (select {t1ColSelect} from timeSeries where day >= {dayt1Start} and day < {dayt1End} {sqlWindowt1}) group by usubjid) as ts2
					on ts1.usubjid = ts2.usubjid''')
			
			# Move to next row
			if advanceBy == 'day':
				nextRowDay = dayt0Start+1
			elif advanceBy == 'window':
				nextRowDay = dayt0Start+window
			elif advanceBy == 'transition':
				nextRowDay = dayt1Start
			else: # advanceBy num days
				nextRowDay = advanceBy
			dayt0Start = nextRowDay
			dayt0End = nextRowDay + windowSize-1
			dayt1Start = dayt0Start + transitionSize
			dayt1End = dayt1Start + windowSize-1

# At the moment, just adds the +1 day information variables
def updateDbn2Slice():
	with DB(workingDb['path']) as db:
		try:  db.query('alter table dbn2slice add column ci_ds_decod_plus1day_t0 text')
		except:  pass
		try:  db.query('alter table dbn2slice add column ci_ho_type_plus1day_t0 text')
		except:  pass
		try:  db.query('alter table dbn2slice add column ci_in_trt_plus1day_t0 text')
		except:  pass
		# This only fills DSDECOD in for the following day (not any day after), but since it's +1 day, that shouldn't be a problem
		# Interventions should be OK, because they are already interpolated
		# UPDATE 2021-11-13: Now with explicit dsdecod interpolation to make this as precisely matched to the t+1 slice style as possible...
		plusNDays = 1
		dsdecodCalc = f'''iif(sliceStart_t0+{plusNDays}>(select max(day) from timeSeries where usubjid = outer.usubjid_t0),
			(select ci_ds_decod from timeSeries where usubjid = outer.usubjid_t0 and ci_ds_decod regexp 'DEATH|DISCHARGED|TRANSFERRED' order by day desc limit 1),
			(select ci_ds_decod from (select ci_ds_decod, sliceStart_t0 as ss, sliceEnd_t0 as se, day from timeSeries where day >= sliceStart_t0+{plusNDays} and day < sliceEnd_t0+{plusNDays} and usubjid = outer.usubjid_t0) order by abs(day - ((se+ss)/2+{plusNDays})) limit 1)
			)'''
		db.query(f'''
			update dbn2slice as outer
			set ci_ds_decod_plus1day_t0 = {dsdecodCalc},
				ci_in_trt_plus1day_t0 = (select ci_in_trt from (select ci_in_trt, sliceStart_t0 as ss, sliceEnd_t0 as se, day from timeSeries where day >= sliceStart_t0+{plusNDays} and day < sliceEnd_t0+{plusNDays} and usubjid = outer.usubjid_t0) order by abs(day - ((se+ss)/2+{plusNDays})) limit 1),
				ci_ho_type_plus1day_t0 = (select ci_ho_type from (select ci_ho_type, sliceStart_t0 as ss, sliceEnd_t0 as se, day from timeSeries where day >= sliceStart_t0+{plusNDays} and day < sliceEnd_t0+{plusNDays} and usubjid = outer.usubjid_t0) order by abs(day - ((se+ss)/2+{plusNDays})) limit 1)
		''')
		# The following won't work for a baseline day's +1 and a larger than 1 transition size (since there's no t1 that corresponds to it)
		# db.query(f'''
			# update dbn2slice as outer
			# set
				# ci_ds_decod_plus1day_t0 = (select ci_ds_decod_t1 from dbn2slice where sliceStart_t1 = outer.sliceStart_t0+{plusNDays} and usubjid_t0 = outer.usubjid_t0 and coalesce(ci_ds_decod_t1,'')<>'' limit 1),
				# ci_in_trt_plus1day_t0 = (select ci_in_trt_t1 from dbn2slice where sliceStart_t1 = outer.sliceStart_t0+{plusNDays} and usubjid_t0 = outer.usubjid_t0 and coalesce(ci_in_trt_t1,'')<>'' limit 1),
				# ci_ho_type_plus1day_t0 = (select ci_ho_type_t1 from dbn2slice where sliceStart_t1 = outer.sliceStart_t0+{plusNDays} and usubjid_t0 = outer.usubjid_t0 and coalesce(ci_ho_type_t1,'')<>'' limit 1)
		# ''')

def signif(x, p):
	np = numpy
	x = np.asarray(x)
	x_positive = np.where(np.isfinite(x) & (x != 0), np.abs(x), 10**(p-1))
	mags = 10 ** (p - 1 - np.floor(np.log10(x_positive)))
	return np.round(x * mags) / mags

def calcQuantiles(vals, interval = 0.25):
	numGroups = (1/interval)
	return [signif(numpy.quantile(vals, i*interval),3) for i in range(0,int(numpy.ceil(numGroups))+1)]

net = Net()
def binNumber(val, thresholds):
	prevT = '_'
	for t in thresholds:
		if val < t:
			return net.makeValidName(f's{prevT}_{t}')
		prevT = t
	
	return None

class NoExc(Exception): pass

def makeDataset(outFn, query = 'select * from subject left join baseline on subject.usubjid = baseline.usubjid'):
	with DB(workingDb['path']) as db:
		db.query('create temp table dataset as '+query)
		
		cols = db.queryColumns('pragma table_info("dataset")')['name']
		
		allCols = db.queryColumns('select * from dataset')
		
		quantileMap = {}
		for col in cols:
			if col in ['id', 'USUBJID', 'usubjid_t0', 'usubjid_t1', 'sliceStart_t0','sliceStart_t1','sliceEnd_t0','sliceEnd_t1']:  continue
			try:
				vals = allCols[col]
				# FIX: If numbers have comma, convert to decimal earlier on (assume there are commas, because
				# that's often the eu separator)
				cleanedVals = [float(re.sub(r',', '.', str(v))) for v in vals if v is not None and v not in ['','NA','None']]
				# print(f"cleanedVals {col}:", cleanedVals)
				quantiles = calcQuantiles(cleanedVals, 1/7)
				# print(quantiles)
				
				quantileMap[col] = quantiles
			except:
				pass
				#print(f"Can't quantile {col}")
		
		print(f"No quantiles for {', '.join([col for col in cols if col not in quantileMap])}")
			
		with open(outFn, 'w', newline='') as outFile:
			outCsv = csv.DictWriter(outFile, cols, quoting=csv.QUOTE_NONNUMERIC)
			outCsv.writeheader()
			
			rs = db.queryRows('select * from dataset')
			for row in rs:
				for col,val in row.items():
					if col in quantileMap:
						if val is None or val in ['', 'NA']:
							stateLabel = '*'
						else:
							stateLabel = binNumber(float(re.sub(r',', '.', str(val))), quantileMap[col])
						row[col] = stateLabel
					else:
						# escape '-' now, because otherwise stripped if at start (i.e. need to FIX)
						row[col] = re.sub(r'ZZZ', '_', net.makeValidName(re.sub(r'-','ZZZ',str(val))))
			
				outCsv.writerow(row)

	
def main(transitionSize = None, versionTag = None):
	# Set up the main databases (that don't change)
	runStep(makeIddoOriginalDb, msg = 'Making SQLite version of original data...',
		skipWhen = os.path.exists(originalDbLoc), skipMsg = 'Using existing original db')
	
	runStep(makeIddoFilteredDb, msg = 'Filtering to individuals with timed intervention information',
		skipWhen = os.path.exists(filteredDbLoc), skipMsg = 'Using existing filtered db')
	
	
	
	# Add a version tag to the working DB (and any output files) if requested
	if versionTag:
		versionTag = versionTag + '_'
		dirName,baseName = os.path.split(workingDb['path'])
		workingDb['path'] = os.path.join(dirName, versionTag + baseName)
	else:
		versionTag = ''
	
	runStep(lambda: makeSampleDb(workingDb['path'], dmFilter=workingDb['dmFilter'], numPatients = workingDb['numPatients'], sourceDb=workingDb['source']),
		msg = f'Making sample DB with (at most) {workingDb["numPatients"]} patients...',
		skipWhen = os.path.exists(workingDb['path']),
		skipMsg = 'Using existing sample db')
	
	runStep(makeSubject, 'Making subject table')
	
	runStep(makeTimeSeries, 'Making time series table')
	
	runStep(cleanTimeSeries, 'Cleaning the time series table (trimming)')
	
	runStep(lambda: updateTableUnits('subject'), msg = 'Updating to common units in subject table')
	
	runStep(lambda: updateTableUnits('timeSeries'), msg = 'Updating to common units in timeSeries table')
	
	runStep(addComputedFields, msg = 'Adding computed fields (e.g. MAP, NLR)')
	
	baselineFilter = "day >= -10 and day <=2"
	runStep(lambda: makeBaseline(baselineFilter = baselineFilter), msg = f'Making the baseline table using "{baselineFilter}"')
	
	runStep(lambda: makeDataset(paths.out(versionTag+'baseline.csv')), msg = 'Making the baseline dataset')
	
	# Version of baseline that only has DEATH or DISCHARGED
	baselineFilter = "day >= -10 and day <=2 and usubjid in (select usubjid from subject where dsdecod regexp 'DEATH|DISCHARGED')" # and ci_CRP is not null"
	runStep(lambda: makeBaseline(baselineFilter = baselineFilter, tableName = 'baseline_resolvedOnly'), msg = f'Making the baseline table using "{baselineFilter}"')
	runStep(lambda: makeDataset(paths.out(versionTag+'baseline_resolvedOnly.csv'), query='select * from subject inner join baseline_resolvedOnly on subject.usubjid = baseline_resolvedOnly.usubjid'), msg = 'Making the baseline (resolved cases only) dataset')
	
	# Uncomment to pick the worst status within the 3 day window. Otherwise, chooses the status nearest the midpoint
	runStep(lambda: makeDbn2Slice(choose = 'nearest',
		# specialCols = {'ci_ds_decod': 'iif(sum(ci_ds_decod="DEATH") over SUBJECTTIMEWINDOW, "DEATH", iif(sum(ci_ds_decod="DISCHARGED") over SUBJECTTIMEWINDOW, "DISCHARGED", min(ci_ds_decod) filter (where ci_ds_decod is not null) over SUBJECTTIMEWINDOW))'},
		transitionSize=transitionSize), msg = 'Making dbn2Slice table')
	
	runStep(cleanDbn2Slice, msg = 'Padding out the DS_DECOD field for slice t1')
	runStep(updateDbn2Slice, msg = 'Updating dbn2slice with +1 day variables')

	# Note: this is skipped by default, since progression accesses database directly
	runStep(lambda: makeDataset(paths.out(versionTag+'dbn2slice.csv'), query = 'select * from subject left join dbn2slice on subject.usubjid = dbn2slice.usubjid_t0'), msg = 'Making DBN 2 slice data set (discretized)', skipWhen = True)
	
	
if __name__ == "__main__":
	main(versionTag = '2021-12-02')