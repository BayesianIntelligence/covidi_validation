import _env, glob, csv, time
from bidb import DB
import csvdb

sampleDbLoc = 'out/iddo_sample.sqlite'

def makeCaseFile(outCsvFn):
	print('Making Casefile')
	t = time.time()
	db = DB(sampleDbLoc)
	rs = db.queryRows('select * from subject left join dbn2slice on subject.usubjid = dbn2slice.usubjid_t0')

	with open(outCsvFn, 'w', newline='') as csvfile:
		headers = 'USUBJID,id_t0,id_t1'.split(',')
		headers = headers+'ci_country,ci_icu'.split(',')
		# headers = headers+['ci_country']
		headers = headers+'ci_age_group_bg,ci_gender_bg,ci_hypertension_bg,ci_obesity_bg,ci_metabolic_syndrome_bg,ci_diabetes_bg,ci_smoking_history_bg,ci_chronic_pul_disease_bg,ci_chronic_cardiac_disease_bg,ci_chronic_kidney_disease_bg,ci_liver_disease_bg'.split(',')
	
		for step in ['0','1']:
			headers = headers+('ci_sys_immune_resp_t'+step+',ci_cardiac_output_t'+step+',ci_func_car_t'+step+',ci_hypoxaemia_t'+step+',ci_func_pul_t'+step+',ci_coagulation_t'+step+',ci_end_organ_perf_t'+step+',ci_intravas_volume_t'+step).split(',')
			headers = headers+('ci_antiviral_treat_t'+step+',ci_antiinflam_treat_t'+step+',ci_anticoag_treat_t'+step+',ci_pulmonary_support_t'+step+',ci_cardiac_support_t'+step).split(',')
			headers = headers+['ci_status_t'+step]
			headers = headers+['ci_crp_t'+step,'ci_neut_t'+step,'ci_lym_t'+step,'ci_ldh_t'+step,'ci_respiratory_rate_t'+step,'ci_pco2_t'+step,'ci_oxygen_saturation_t'+step,'ci_po2_t'+step,'ci_pulse_rate_t'+step,'ci_troponin_t'+step,'ci_systolic_blood_pressure_t'+step,'ci_diastolic_blood_pressure_t'+step,'ci_ddimer_t'+step,'ci_aptt_t'+step,'ci_plat_t'+step,'ci_lacticac_t'+step,'ci_creat_t'+step,'ci_hct_t'+step]
			
		headers = headers+['ci_status','ci_status_plus1day','ci_pulmonary_support_plus1day', 'ci_inv_vent']
		
		writer = csv.DictWriter(csvfile, fieldnames=headers)
		writer.writeheader()
	
		for row in rs:
			out = {}
			out['USUBJID'] = row['USUBJID']
			out['ci_country'] = row['COUNTRY']
			
			# if 'INTENSIVE CARE UNIT' in str(row['ci_ho_type_t0']).split('|'):
				# out['ci_icu'] = 'true'
			# else:
				# out['ci_icu'] = 'false'
				
			out['ci_inv_vent'] = row['ci_InvVent']
			out['ci_icu'] = row['ci_ICU']
			
			status = row['DSDECOD']
			if status == 'DEATH':
				out['ci_status'] = 'dead'
			elif status == 'DISCHARGED':
				out['ci_status'] = 'discharged'
			else:
				out['ci_status'] = '*'
				
			
			

			age = row['AGE']
			if not age or age == 'NA':
				out['ci_age_group_bg'] = '*'
			elif float(age) <= 45:
				out['ci_age_group_bg'] = 'young'
			elif float(age) <= 75:
				out['ci_age_group_bg'] = 'adult'
			else:
				out['ci_age_group_bg'] = 'senior'
			
			
			sex = row['SEX']
			if sex == 'M':
				out['ci_gender_bg'] = 'male'
			elif sex == 'F':
				out['ci_gender_bg'] = 'female'
			else:
				out['ci_gender_bg'] = '*'
				
				
			hist = str(row['ci_sa_medicalHistory']).split('|')
				
			if 'CHRONIC PULMONARY DISEASE' in hist:
				out['ci_chronic_pul_disease_bg'] = 'true'
			else:
				out['ci_chronic_pul_disease_bg'] = 'false'
				
				
			if 'CHRONIC CARDIAC DISEASE' in hist:
				out['ci_chronic_cardiac_disease_bg'] = 'true'
			else:
				out['ci_chronic_cardiac_disease_bg'] = 'false'
				
				
			if 'CHRONIC KIDNEY DISEASE' in hist:
				out['ci_chronic_kidney_disease_bg'] = 'true'
			else:
				out['ci_chronic_kidney_disease_bg'] = 'false'
				
				
			if 'LIVER DISEASE' in hist:
				out['ci_liver_disease_bg'] = 'true'
			else:
				out['ci_liver_disease_bg'] = 'false'
				
				
			if 'HYPERTENSION' in hist:
				out['ci_hypertension_bg'] = 'true'
			else:
				out['ci_hypertension_bg'] = 'false'
				
				
			if 'OBESITY' in hist:
				out['ci_obesity_bg'] = 'true'
			else:
				out['ci_obesity_bg'] = 'false'
				
				
			if 'OBESITY' in hist or 'HYPERTENSION' in hist:
				out['ci_metabolic_syndrome_bg'] = 'true'
			else:
				out['ci_metabolic_syndrome_bg'] = 'false'
				
				
			# if 'DIABETES' in hist or 'DIABETES - TYPE 1' in hist or 'DIABETES - TYPE 2' in hist:
				# if 'CHRONIC KIDNEY DISEASE' in hist or 'LIVER DISEASE' in hist:
					# out['ci_diabetes_bg'] = 'yescomplications'
				# else:
					# out['ci_diabetes_bg'] = 'yesnocomplications'
			# else:
				# out['ci_diabetes_bg'] = 'false'
				
			if 'DIABETES' in hist or 'DIABETES - TYPE 1' in hist or 'DIABETES - TYPE 2' in hist:
				out['ci_diabetes_bg'] = 'true'
			else:
				out['ci_diabetes_bg'] = 'false'
				
				
			if 'SMOKING' in hist:
				out['ci_smoking_history_bg'] = 'smoker'
			elif 'SMOKING - FORMER' in hist:
				out['ci_smoking_history_bg'] = 'exsmoker'
			else:
				out['ci_smoking_history_bg'] = 'never'
				
				
				
			#t+1 outputs
				
				
			# status = row['ci_ds_decod_plus1day_t0']
			# if status == 'DEATH':
				# out['ci_status_plus1day'] = 'dead'
			# elif status == 'DISCHARGED':
				# out['ci_status_plus1day'] = 'discharged'
			# else:
				# out['ci_status_plus1day'] = '*'
				
				
			#default missing to still in hospital
			status = row['ci_ds_decod_plus1day_t0']
			try:
				hosp = row['ci_ho_type_plus1day_t0'].split('|')[0]
			except:
				hosp = row['ci_ho_type_plus1day_t0']
			try:
				prev = out[f'ci_status_t0']
			except:
				prev = None
				
				
			if status == 'DEATH' or prev == 'dead':
				out['ci_status_plus1day'] = 'dead'
			elif status == 'DISCHARGED' or prev == 'discharged':
				out['ci_status_plus1day'] = 'discharged'
			elif hosp == 'INTENSIVE CARE UNIT':
				out['ci_status_plus1day'] = 'icu'
			elif hosp == 'HOSPITAL':
				out['ci_status_plus1day'] = 'hospital'
			else:
				out['ci_status_plus1day'] = '*'
				
				
			in_trt = str(row['ci_in_trt_plus1day_t0']).split('|')
			oxyTrt2 = 'OXYGEN THERAPY'.split('|')
			oxyTrt3 = 'HIGH-FLOW NASAL CANULA OXYGEN THERAPY|OXYGEN THERAPY WITH HIGH FLOW NASAL CANULA'.split('|')
			oxyTrt4 = 'BIPAP|CPAP|NON-INVASIVE MECHANICAL VENTILATION (BIPAP, CPAP, OCNAF (OPTIFLOW) ...)|NON-INVASIVE VENTILATION|NON-INVASIVE VENTILATION (E.G. BIPAP, CPAP)|OTHER NON-INVASIVE VENTILATION TYPE|UNKNOWN NON-INVASIVE VENTILATION TYPE'.split('|')
			oxyTrt5 = 'INVASIVE VENTILATION|INVASIVE VENTILATION (ANY)|PRONE VENTILATION'.split('|')
						
			if len(set(oxyTrt5)&set(in_trt)) > 0:
				out['ci_pulmonary_support_plus1day'] = 'mechanical_ventilation'
			elif len(set(oxyTrt4)&set(in_trt)) > 0:
				out['ci_pulmonary_support_plus1day'] = 'cpap'
			elif len(set(oxyTrt3)&set(in_trt)) > 0:
				out['ci_pulmonary_support_plus1day'] = 'high_flow_nasal'
			elif len(set(oxyTrt2)&set(in_trt)) > 0:
				out['ci_pulmonary_support_plus1day'] = 'supplemental_o2'
			else:
				out['ci_pulmonary_support_plus1day'] = 'none'
						
	
			for step in ['0','1']:
				out['id_t'+step] = row['id_t'+step]
				sa_comp = str(row['ci_sa_complications_t'+step]).split('|')
				in_cat = str(row['ci_in_cat_t'+step]).split('|')
				in_trt = str(row['ci_in_trt_t'+step]).split('|')

				#default missing to still in hospital
				status = row['ci_ds_decod_t'+step]
				try:
					hosp = row['ci_ho_type_t'+step].split('|')[0]
				except:
					hosp = row['ci_ho_type_t'+step]
				try:
					prev = out[f'ci_status_t{int(step)-1}']
				except:
					prev = None
					
					
				if status == 'DEATH' or prev == 'dead':
					out['ci_status_t'+step] = 'dead'
				elif status == 'DISCHARGED' or prev == 'discharged':
					out['ci_status_t'+step] = 'discharged'
				elif hosp == 'INTENSIVE CARE UNIT':
					out['ci_status_t'+step] = 'icu'
				elif hosp == 'HOSPITAL':
					out['ci_status_t'+step] = 'hospital'
				else:
					out['ci_status_t'+step] = '*'	
				# elif status == 'TRANSFERRED' or status == 'UNKNOWN':
					# out['ci_status_t'+step] = '*'			
					
					
				pulse = row['ci_Pulse_Rate_t'+step] if row['ci_Pulse_Rate_t'+step] else row['ci_Heart_Rate_t'+step]
				trop = row['ci_TROPONIN_t'+step]
				carComp = 'ACUTE CARDIAC INJURY,ACUTE MYOCARDIAL INFARCTION,CARDIAC ARREST,CARDIAC ARRHYTHMIA,CARDIAC INFLAMMATION,CARDIAC ISCHAEMIA,CARDIOMYOPATHY,CONGESTIVE HEART FAILURE,ENDOCARDITIS,MYOCARDITIS,PERICARDITIS,HIGH BNP/NT PRO BNP,HIGH TROPONIN I/T,MYOCARDIAL INFARCTION,SUPRAVENTRICULAR ARRHYTHMIA,TRANSIENT ISCHAEMIC ATTACK,VENTRICULAR ARRHYTHMIA'.split(',')
				hasComp = len(set(sa_comp)&set(carComp))>0
				carTreat = 'VASOPRESSOR/INOTROPIC,INOTROPES/VASOPRESSORS,VASOPRESSIN'.split(',')
				hasTreat = len(set(carTreat)&set(in_trt))>0
			
				out['ci_pulse_rate_t'+step] = pulse
				out['ci_troponin_t'+step] = trop
				
				#Check this
				if not pulse and not trop and not hasComp and not hasTreat:
					out['ci_func_car_t'+step] = '*'
				#29/11/21 changed from <45 and > 199
				elif pulse and (float(pulse) < 50 or float(pulse) > 120):
					out['ci_func_car_t'+step] = 'abnormal'
				elif trop and float(trop) > 0.04:
					out['ci_func_car_t'+step] = 'abnormal'
				elif hasComp or hasTreat: 
					out['ci_func_car_t'+step] = 'abnormal'
				else:
					out['ci_func_car_t'+step] = 'normal'
					
					
				sysbp = row['ci_Systolic_Blood_Pressure_t'+step]
				diabp = row['ci_Diastolic_Blood_Pressure_t'+step]
				
				out['ci_systolic_blood_pressure_t'+step] = sysbp
				out['ci_diastolic_blood_pressure_t'+step] = diabp
				
				if not sysbp and not diabp:
					out['ci_cardiac_output_t'+step] = '*'
				elif sysbp and diabp and (float(sysbp)+2*float(diabp))/3 < 70:
					out['ci_cardiac_output_t'+step] = 'low'
				elif sysbp and float(sysbp) < 90:
					out['ci_cardiac_output_t'+step] = 'low'
				elif diabp and float(diabp) < 60:
					out['ci_cardiac_output_t'+step] = 'low'
				else:
					out['ci_cardiac_output_t'+step] = 'normal'
				
					
					
				resp = row['ci_Respiratory_Rate_t'+step]
				paco2 = row['ci_PCO2_t'+step]
				oxyTreat = 'NASAL / MASK OXYGEN THERAPY,NON-INVASIVE VENTILATION,INVASIVE VENTILATION,EXTRACORPOREAL,PRONE POSITIONING'.split(',')
				hasTreat = len(set(oxyTreat)&set(in_cat))>0
				pulComp = 'PNEUMOTHORAX,PULMONARY EMBOLISM,ACUTE RESPIRATORY DISTRESS SYNDROME,PEADIATRIC ARDS (PARDS),RESPIRATORY FAILURE'.split(',')
				hasComp = len(set(sa_comp)&set(pulComp))>0
				
				out['ci_respiratory_rate_t'+step] = resp
				out['ci_pco2_t'+step] = paco2
				
				if not resp and not paco2 and not hasTreat and not hasComp:
					out['ci_func_pul_t'+step] = '*'
				elif resp and float(resp) >= 22:
					out['ci_func_pul_t'+step] = 'abnormal'
				elif paco2 and float(paco2) > 55:				
					out['ci_func_pul_t'+step] = 'abnormal'
				elif hasTreat or hasComp:
					out['ci_func_pul_t'+step] = 'abnormal'
				else:	
					out['ci_func_pul_t'+step] = 'normal'
				
								
				sao2 = row['ci_Oxygen_Saturation_t'+step]
				pao2 = row['ci_PO2_t'+step]
				
				out['ci_oxygen_saturation_t'+step] = sao2
				out['ci_po2_t'+step] = pao2
				
				if not sao2 and not pao2:
					out['ci_hypoxaemia_t'+step] = '*'
				elif sao2 and float(sao2) <=89:
					out['ci_hypoxaemia_t'+step] = 'verylow'
				elif pao2 and float(pao2) <= 49:
					out['ci_hypoxaemia_t'+step] = 'verylow'
				elif sao2 and float(sao2) <=95:
					out['ci_hypoxaemia_t'+step] = 'low'
				elif pao2 and float(pao2) <= 79:
					out['ci_hypoxaemia_t'+step] = 'low'
				else:
					out['ci_hypoxaemia_t'+step] = 'normal'
				
					
				
				crp = row['ci_CRP_t'+step]
				neut = row['ci_NEUT_t'+step]
				lym = row['ci_LYM_t'+step]
				ldh = row['ci_LDH_t'+step]
				
				
				if crp: crp = max(0,crp)
				out['ci_crp_t'+step] = crp
				out['ci_neut_t'+step] = neut
				out['ci_lym_t'+step] = lym
				out['ci_ldh_t'+step] = ldh
				
				nlr = None
				if neut and lym:
					nlr = neut/lym
					
				
				# if not crp and not neut and not lym and not ldh:
					# out['ci_sys_immune_resp_t'+step] = '*'
				# elif crp and float(crp) >= 70: #updated from 30 - 30/08/21
					# out['ci_sys_immune_resp_t'+step] = 'high'
				# elif neut and float(neut) >= 9:
					# out['ci_sys_immune_resp_t'+step] = 'high'
				# elif lym and float(lym) >= 0.8:
					# out['ci_sys_immune_resp_t'+step] = 'high'
				# elif ldh and float(ldh) > 350: #updated from 280 - 30/08/21
					# out['ci_sys_immune_resp_t'+step] = 'high'
					 # #this may be too liberal?
				# elif crp and float(crp) < 3:
					# out['ci_sys_immune_resp_t'+step] = 'low'
				# else:
					# out['ci_sys_immune_resp_t'+step] = 'moderate'				
				
				
				if not crp and not nlr and not ldh:
					out['ci_sys_immune_resp_t'+step] = '*'
				elif crp and float(crp) > 70:
					out['ci_sys_immune_resp_t'+step] = 'abnormal'
				elif nlr and float(nlr) > 5:
					out['ci_sys_immune_resp_t'+step] = 'abnormal'
				elif ldh and float(ldh) > 350:
					out['ci_sys_immune_resp_t'+step] = 'abnormal'
				else:
					out['ci_sys_immune_resp_t'+step] = 'normal'
					

				ddim = row['ci_DDIMER_t'+step]
				aptt = row['ci_APTT_t'+step]
				plat = row['ci_PLAT_t'+step]
				coagComp = 'COAGULATION DISORDER/DIC,DISSEMINATED INTRAVASCULAR COAGULATION,THROMBOEMBOLIC PHENOMENA'.split(',')
				hasComp = len(set(sa_comp)&set(coagComp))>0
				
				out['ci_ddimer_t'+step] = ddim
				out['ci_aptt_t'+step] = aptt
				out['ci_plat_t'+step] = plat
				
				if not ddim and not aptt and not plat and not hasComp:
					out['ci_coagulation_t'+step] = '*'
				elif ddim and float(ddim) > 0.5: 
					out['ci_coagulation_t'+step] = 'abnormal'
				elif aptt and float(aptt) > 25:
					out['ci_coagulation_t'+step] = 'abnormal'
				elif plat and float(plat) < 120:
					out['ci_coagulation_t'+step] = 'abnormal'
				elif hasComp:
					out['ci_coagulation_t'+step] = 'abnormal'
				else:
					out['ci_coagulation_t'+step] = 'normal'
						
					
				lac = row['ci_LACTICAC_t'+step]
				crea = row['ci_CREAT_t'+step]
				perfComp = 'ACUTE KIDNEY INJURY,ACUTE KIDNEY FAILURE,ACUTE RENAL INJURY,ACUTE RENAL FAILURE,RENAL FAILURE,'.split(',')
				hasComp = len(set(sa_comp)&set(perfComp))>0
				
				out['ci_lacticac_t'+step] = lac
				out['ci_creat_t'+step] = crea
				
				#using average for creatinine 
				#Male: 120 Female: 90
				if not lac and not crea and not hasComp:
					out['ci_end_organ_perf_t'+step] = '*'
				elif lac and float(lac) > 1:
					out['ci_end_organ_perf_t'+step] = 'low'
				elif crea and float(crea) > 105:
					out['ci_end_organ_perf_t'+step] = 'low'
				elif hasComp:
					out['ci_end_organ_perf_t'+step] = 'low'
				else:
					out['ci_end_organ_perf_t'+step] = 'normal'		
					
						
						
				crea = None if 'CHRONIC KIDNEY DISEASE' in hist else row['ci_CREAT_t'+step]
				hema = row['ci_HCT_t'+step]
				intraComp = 'BLEEDING (HAEMORRHAGE),GASTROINTESTINAL HAEMORRHAGE,HEMODYNAMIC DECOMPENSATION,HEMORRHAGIC COMPLICATIONS'.split(',')
				hasComp = len(set(sa_comp)&set(intraComp))>0
				
				out['ci_hct_t'+step] = hema
				
				#using average for creatinine 
				#Male: 100, Female: 80
				if not crea and not hema and not hasComp:
					out['ci_intravas_volume_t'+step] = '*'
				elif crea and float(crea) >= 90:
					out['ci_intravas_volume_t'+step] = 'low'
				#need to check this
				elif hema and float(hema) > 44: 
					out['ci_intravas_volume_t'+step] = 'low'
				elif hasComp:
					out['ci_intravas_volume_t'+step] = 'low'
				else:
					out['ci_intravas_volume_t'+step] = 'normal'	
					
					
				
				if 'ANTIVIRAL AGENTS' in in_cat or 'ANTIVIRALS' in in_trt:
					out['ci_antiviral_treat_t'+step] = 'true'
				else:
					out['ci_antiviral_treat_t'+step] = 'false'
					
					
				if 'CORTICOSTEROIDS' in in_cat or 'CORTICOSTEROIDS' in in_trt or 'NSAIDS' in in_cat or 'TOCILIZUMAB' in in_trt:
					out['ci_antiinflam_treat_t'+step] = 'true'
				else:
					out['ci_antiinflam_treat_t'+step] = 'false'
					
					
				if 'THERAPEUTIC ANTICOAGULANT' in in_trt:
					out['ci_anticoag_treat_t'+step] = 'true'
				else:
					out['ci_anticoag_treat_t'+step] = 'false'
					
					
				# oxyCat = 'NASAL / MASK OXYGEN THERAPY,NON-INVASIVE VENTILATION,INVASIVE VENTILATION,EXTRACORPOREAL,PRONE POSITIONING'.split(',')
				# oxyTrt = 'INVASIVE MECHANICAL LUNG VENTILATION,PRONE VENTILATION,NON-INVASIVE POSITIVE PRESSURE VENTILATION,MECHANICAL VENTILATION,SELF-PRONING WHILE NOT ON VENTILATOR,ECMO,PRONING,NON-INVASIVE MECHANICAL VENTILATION,INVASIVE MECHANICAL VENTILATION,EXTRACORPOREAL SUPPORT,OXYGEN THERAPY,PRONE POSITIONING,EXTRACORPOREAL MEMBRANE OXYGENATION (ECMO/ECLS),INVASIVE VENTILATION,NON-INVASIVE VENTILATION,EXTRACORPOREAL SUPPORT (ECMO),OXYGEN THERAPY WITH HIGH FLOW NASAL CANULA'.split(',')
				# hasTreat = (len(set(oxyCat)&set(in_cat))+len(set(oxyTrt)&set(in_trt)))>0
				# if hasTreat:
					# out['ci_pulmonary_support_t'+step] = 'true'
				# else:
					# out['ci_pulmonary_support_t'+step] = 'false'
					
				# if row['USUBJID']==96599: print(in_trt)
					
				oxyTrt2 = 'OXYGEN THERAPY'.split('|')
				oxyTrt3 = 'HIGH-FLOW NASAL CANULA OXYGEN THERAPY|OXYGEN THERAPY WITH HIGH FLOW NASAL CANULA'.split('|')
				oxyTrt4 = 'BIPAP|CPAP|NON-INVASIVE MECHANICAL VENTILATION (BIPAP, CPAP, OCNAF (OPTIFLOW) ...)|NON-INVASIVE VENTILATION|NON-INVASIVE VENTILATION (E.G. BIPAP, CPAP)|OTHER NON-INVASIVE VENTILATION TYPE|UNKNOWN NON-INVASIVE VENTILATION TYPE'.split('|')
				oxyTrt5 = 'INVASIVE VENTILATION|INVASIVE VENTILATION (ANY)|PRONE VENTILATION'.split('|')
				
				
				if len(set(oxyTrt5)&set(in_trt)) > 0:
					out['ci_pulmonary_support_t'+step] = 'mechanical_ventilation'
				elif len(set(oxyTrt4)&set(in_trt)) > 0:
					out['ci_pulmonary_support_t'+step] = 'cpap'
				elif len(set(oxyTrt3)&set(in_trt)) > 0:
					out['ci_pulmonary_support_t'+step] = 'high_flow_nasal'
				elif len(set(oxyTrt2)&set(in_trt)) > 0:
					out['ci_pulmonary_support_t'+step] = 'supplemental_o2'
				else:
					out['ci_pulmonary_support_t'+step] = 'none'
				
					
				carTrt = 'EXTRACORPOREAL SUPPORT,EXTRACORPOREAL MEMBRANE OXYGENATION (ECMO/ECLS),ECMO,EXTRACORPOREAL SUPPORT (ECMO)'.split(',')
				#incude VASOPRESSOR/INOTROPIC,INOTROPES/VASOPRESSORS,VASOPRESSIN
				hasTreat = len(set(carTrt)&set(in_trt))>0
				if 'EXTRACORPOREAL' in in_cat or hasTreat:
					out['ci_cardiac_support_t'+step] = 'true'
				else:
					out['ci_cardiac_support_t'+step] = 'false'

			for key in out: 
				if not out[key]: out[key] = '*'
			writer.writerow(out)
	print(f'Done ({round(time.time()-t,2)}s)')

def makeMissingCaseFile(inCsvFn, outCsvFn):
	with open(inCsvFn) as incsvfile, open(outCsvFn, 'w', newline='') as outcsvfile:
	
		csv_reader = csv.DictReader(incsvfile)
		csv_writer = csv.DictWriter(outcsvfile, fieldnames=csv_reader.fieldnames)
		csv_writer.writeheader()
		for row in csv_reader:
			for key in row:
				if not (key=='ci_country' or key=='ci_icu'):
					row[key] = 'missing' if row[key]=='*' else 'present'
			#print(row)
			csv_writer.writerow(row)
	
		#csv_reader = csv.reader(incsvfile)
		#csv_writer = csv.writer(outcsvfile)
		#csv_writer.writerow(next(csv_reader, None))
		#for row in csv_reader:
		#	csv_writer.writerow(['missing' if ele=='*' else 'present' for ele in row])

# with open('out/progression_training.csv') as csvfile:
	# missing = 0
	# total = 0
	# reader = csv.reader(csvfile)
	# for row in reader:
		# for item in row:
			# total += 1
			# if item == '*': missing += 1
	# print('missing: '+str(int(100*missing/total))+'%')
		

#USUBJID id_t0 id_t1 ci_country ci_icu < ci_age_group_bg ci_gender_bg ci_metabolic_syndrome_bg ci_diabetes_bg ci_smoking_history_bg ci_chronic_pul_disease_bg ci_chronic_cardiac_disease_bg < ci_sys_immune_resp_t0 ci_cardiac_output_t0 ci_func_car_t0 ci_hypoxaemia_t0 ci_func_pul_t0 ci_coagulation_t0 ci_end_organ_perf_t0 ci_intravas_volume_t0 ci_antiviral_treat_t0 ci_antiinflam_treat_t0 ci_anticoag_treat_t0 ci_pulmonary_support_t0 ci_cardiac_support_t0 ci_death_t0 < ci_sys_immune_resp_t1 ci_cardiac_output_t1 ci_func_car_t1 ci_hypoxaemia_t1 ci_func_pul_t1 ci_coagulation_t1 ci_end_organ_perf_t1 ci_intravas_volume_t1 ci_antiviral_treat_t1 ci_antiinflam_treat_t1 ci_anticoag_treat_t1 ci_pulmonary_support_t1 ci_cardiac_support_t1 ci_death_t1 ci_death




if __name__ == "__main__":
	makeCaseFile('out/progression_training.csv')
	# makeMissingCaseFile('out/progression_training.csv', 'out/progression_missing_training.csv')