import _env, time, os, csv, re, bndict, localconfig
from bni_smile import Net
import bni_netica, bni_smile

paths, settings = localconfig.setup()

print("Current directory:", os.getcwd())

## FIX! All parent nodes need to be cliqued (i.e. fully connected subnet over the parent nodes)

# selNodes = re.split(r'\s*,\s*', '''AGE, SEX, RACE, DSDECOD, ci_vs_bmi, ci_ho_disout, ci_ho_selfcare, HYPERTENSION, DIABETES, DIABETES___TYPE_2, SMOKING, AIDS_HIV, CHRONIC_CARDIAC_DISEASE, DEMENTIA, ASTHMA, MALIGNANT_NEOPLASM, CHRONIC_KIDNEY_DISEASE, SMOKING___FORMER, CHRONIC_PULMONARY_DISEASE, LIVER_DISEASE, OBESITY, CHRONIC_NEUROLOGICAL_DISORDER, RHEUMATOLOGIC_DISORDER, MALNUTRITION, HISTORY_OF_PERIPHERAL_OR_CARDI, DEPRESSION, PEPTIC_ULCER_DISEASE_EXCLUDING, HEMATOLOGIC_MALIGNANCY, DYSLIPIDEMIA_HYPERLIPIDEMIA, CORONARY_ARTERY_DISEASE, OSA__HOME_CPAP_BI_PAP_USE, OTHER_COMORBIDITIES, DIABETES___TYPE_1, TUBERCULOSIS, CHRONIC_HEMATOLOGIC_DISEASE, ASPLENIA, HISTORY_OF_FEVER, COUGH___NO_SPUTUM, WHEEZING, FATIGUE_MALAISE, SHORTNESS_OF_BREATH, CHEST_PAIN, DIARRHOEA, SKIN_RASH, HEADACHE, MUSCLE_ACHES_JOINT_PAIN, COUGH___WITH_SPUTUM, SORE_THROAT, ABDOMINAL_PAIN, VOMITING_NAUSEA, ANOREXIA, ALTERED_CONSCIOUSNESS_CONFUSIO, COUGH_BLOODY_SPUTUM, LOSS_OF_SMELL, LOSS_OF_TASTE, RUNNY_NOSE, INABILITY_TO_WALK, CONJUNCTIVITIS, LYMPHADENOPATHY, CHILLS_RIGORS, MYALGIA_OR_FATIGUE, COVID_19_SYMPTOMS, ACUTE_RENAL_INJURY__NO_HEMOFLI, ARDS, COUGH, OTHER_SIGNS_AND_SYMPTOMS, SEVERE_DEHYDRATION, SKIN_ULCERS, BLEEDING, SEIZURES, COUGH_BLOODY_SPUTUM___HAEMOPTY, EAR_PAIN, ci_ho_type_t0, ci_Body_Mass_Index_t0, ci_Diastolic_Blood_Pressure_t0, ci_Heart_Rate_t0, ci_Height_t0, ci_Mean_Arterial_Pressure_t0, ci_Oxygen_Saturation_t0, ci_Pulse_Rate_t0, ci_Respiratory_Rate_t0, ci_Systolic_Blood_Pressure_t0, ci_Temperature_t0, ci_Weight_t0, ci_APTTSTND_t0, ci_APTT_t0, ci_ALT_t0, ci_ALB_t0, ci_ALP_t0, ci_AMYLASE_t0, ci_AST_t0, ci_BASEEXCS_t0, ci_BASO_t0, ci_BASOLE_t0, ci_BICARB_t0, ci_BILI_t0, ci_CRP_t0, ci_CA_t0, ci_CAION_t0, ci_CAIONPH_t0, ci_CO2_t0, ci_CARBXHGB_t0, ci_CL_t0, ci_CHOL_t0, ci_CK_t0, ci_CREAT_t0, ci_DDIMER_t0, ci_HGBDOXY_t0, ci_BILDIR_t0, ci_EOS_t0, ci_EOSLE_t0, ci_MCHC_t0, ci_MCH_t0, ci_MCV_t0, ci_ESR_t0, ci_RBC_t0, ci_RDW_t0, ci_FERRITIN_t0, ci_FIBRINO_t0, ci_FIBRINOF_t0, ci_FIO2_t0, ci_GGT_t0, ci_GLUC_t0, ci_HCT_t0, ci_HGB_t0, ci_HBA1C_t0, ci_INTLK6_t0, ci_IRON_t0, ci_LDH_t0, ci_LACTICAC_t0, ci_WBC_t0, ci_LYM_t0, ci_LYMLE_t0, ci_MG_t0, ci_MPV_t0, ci_HGBMET_t0, ci_MONO_t0, ci_MONOLE_t0, ci_NEUT_t0, ci_NEUTLE_t0, ci_OXYSAT_t0, ci_HGBOXY_t0, ci_PO2FIO2_t0, ci_PCO2_t0, ci_PO2_t0, ci_PLATHCT_t0, ci_PLAT_t0, ci_K_t0, ci_PCT_t0, ci_PROT_t0, ci_INR_t0, ci_PT_t0, ci_PTAC_t0, ci_SODIUM_t0, ci_TT_t0, ci_TROPONIN_t0, ci_TROPONI_t0, ci_URATE_t0, ci_UREAN_t0, ci_PH_t0''')
# selNodes.extend(re.split(r'\s*,\s*', '''ci_ho_type_bl, ci_Body_Mass_Index_bl, ci_Diastolic_Blood_Pressure_bl, ci_Heart_Rate_bl, ci_Height_bl, ci_Mean_Arterial_Pressure_bl, ci_Oxygen_Saturation_bl, ci_Pulse_Rate_bl, ci_Respiratory_Rate_bl, ci_Systolic_Blood_Pressure_bl, ci_Temperature_bl, ci_Weight_bl, ci_APTTSTND_bl, ci_APTT_bl, ci_ALT_bl, ci_ALB_bl, ci_ALP_bl, ci_AMYLASE_bl, ci_AST_bl, ci_BASEEXCS_bl, ci_BASO_bl, ci_BASOLE_bl, ci_BICARB_bl, ci_BILI_bl, ci_CRP_bl, ci_CA_bl, ci_CAION_bl, ci_CAIONPH_bl, ci_CO2_bl, ci_CARBXHGB_bl, ci_CL_bl, ci_CHOL_bl, ci_CK_bl, ci_CREAT_bl, ci_DDIMER_bl, ci_HGBDOXY_bl, ci_BILDIR_bl, ci_EOS_bl, ci_EOSLE_bl, ci_MCHC_bl, ci_MCH_bl, ci_MCV_bl, ci_ESR_bl, ci_RBC_bl, ci_RDW_bl, ci_FERRITIN_bl, ci_FIBRINO_bl, ci_FIBRINOF_bl, ci_FIO2_bl, ci_GGT_bl, ci_GLUC_bl, ci_HCT_bl, ci_HGB_bl, ci_HBA1C_bl, ci_INTLK6_bl, ci_IRON_bl, ci_LDH_bl, ci_LACTICAC_bl, ci_WBC_bl, ci_LYM_bl, ci_LYMLE_bl, ci_MG_bl, ci_MPV_bl, ci_HGBMET_bl, ci_MONO_bl, ci_MONOLE_bl, ci_NEUT_bl, ci_NEUTLE_bl, ci_OXYSAT_bl, ci_HGBOXY_bl, ci_PO2FIO2_bl, ci_PCO2_bl, ci_PO2_bl, ci_PLATHCT_bl, ci_PLAT_bl, ci_K_bl, ci_PCT_bl, ci_PROT_bl, ci_INR_bl, ci_PT_bl, ci_PTAC_bl, ci_SODIUM_bl, ci_TT_bl, ci_TROPONIN_bl, ci_TROPONI_bl, ci_URATE_bl, ci_UREAN_bl, ci_PH_bl'''))

#selNodes = re.split(r'\s*,\s*', '''DSDECOD, ci_CRP_bl, AGE, SEX, ci_vs_bmi, HYPERTENSION, DIABETES''')
selNodes = re.split(r'\s*,\s*', '''DSDECOD, ci_ICU, ci_InvVent, AGE, ci_LDH_bl, ci_NLR_bl, ci_MAP_bl, ci_CRP_bl, ci_CREAT_bl, ci_DDIMER_bl, ci_PLAT_bl, ci_PH_bl,
	ci_Diastolic_Blood_Pressure_bl, ci_Systolic_Blood_Pressure_bl, ci_Oxygen_Saturation_bl,
	HYPERTENSION, DIABETES, CHRONIC_CARDIAC_DISEASE, CHRONIC_KIDNEY_DISEASE, CHRONIC_PULMONARY_DISEASE, LIVER_DISEASE''')

# SM: Note that BL_Comorb_CoronaryArteryDiseas has been shortened to meet Netica ID length limit
largeSelNodes = re.split(r'\s*,\s*', '''DSDECOD, AGE''')

parentNodes = []

def makeNb(dataFn, netFn, selNodes = None, type = 'genie', parentNodes = parentNodes):
	bnModule = bni_smile if type == 'genie' else bni_netica
	bndict.dataToDict(dataFn, paths.out('_dict.csv'))
	print(selNodes)
	bndict.dictToBn(paths.out('_dict.csv'), netFn, subset = set(selNodes), bnModule = bnModule)
	
	net = bnModule.Net(netFn)
	
	xGap = 250
	xMax = 1500
	
	x = 40
	y = 20
	prevParents = [] # Fully connect nodes, in whatever order
	for nodeName in parentNodes:
		net.node(nodeName).addParents(prevParents)
		prevParents.append(nodeName)
		net.node(nodeName).position(x = x, y = y)
		x += xGap
		if x > xMax:
			x = 40
			y += 220
	
	x = 40
	y += 220
	for nodeName in selNodes:
		net.node(nodeName)
		if nodeName not in parentNodes and net.node(nodeName):
			(net.node(nodeName).addParents(parentNodes)
				.position(x = x, y = y))
			x += xGap
			if x > xMax:
				x = 40
				y += 220

	net.learn(dataFn).write(netFn)

def convertToNb(netFn, parentNodes, type = 'genie', dataFn = None, outNetFn = None):
	bnModule = bni_smile if type == 'genie' else bni_netica
	net = bnModule.Net(netFn)
	
	for node in net.nodes():
		node.removeParents(node.parents())
	
	xGap = 250
	xMax = 1500
	
	x = 40
	y = 20
	prevParents = [] # Fully connect nodes, in whatever order
	for nodeName in parentNodes:
		net.node(nodeName).addParents(prevParents)
		prevParents.append(nodeName)
		net.node(nodeName).position(x = x, y = y)
		x += xGap
		if x > xMax:
			x = 40
			y += 220
	
	x = 40
	y += 220
	for nodeName in [n.name() for n in net.nodes()]:
		net.node(nodeName)
		if nodeName not in parentNodes and net.node(nodeName):
			(net.node(nodeName).addParents(parentNodes)
				.position(x = x, y = y))
			x += xGap
			if x > xMax:
				x = 40
				y += 220
	
	net.write('test.dne')

	if dataFn:  net.learn(dataFn, removeTables=True)
	if outNetFn:  net.write(outNetFn)

def makeTan(dataFn, netFn, selNodes = None, type = 'genie', parentNodes = parentNodes):
	# Not sure how to do TAN learning with Netica's API
	bnModule = bni_smile # if type == 'genie' else bni_netica
	# bndict.dataToDict(dataFn, 'data/_dict.csv')
	# bndict.dictToBn('data/_dict.csv', netFn, subset = set(selNodes), bnModule = bnModule)
	
	net = bnModule.Net()
	
	def clean(row):
		for k,v in row.items():
			if v == '*' or v == '':
				row[k] = 'ND'	
				
	bndict.filterData(dataFn, 'temp/tan_structure.csv', subset=selNodes, rowAdapter=clean)

	net.learnStructure('temp/tan_structure.csv', type = 'TAN', classVar = parentNodes[0])

	xGap = 250
	xMax = 1500
	
	x = 40
	y = 20
	for nodeName in parentNodes:
		print(nodeName)
		net.node(nodeName).position(x = x, y = y)
		x += xGap
		if x > xMax:
			x = 40
			y += 220
	
	x = 40
	y += 220
	for nodeName in selNodes:
		net.node(nodeName)
		if nodeName not in parentNodes and net.node(nodeName):
			net.node(nodeName).position(x = x, y = y)
			x += xGap
			if x > xMax:
				x = 40
				y += 220
	
	net.write(netFn)

def makeBnSafe(inCsvFn, outCsvFn):
	tempNet = bni_netica.Net()
	with open(inCsvFn) as inCsvFile, open(outCsvFn, 'w', newline='') as outCsvFile:
		inCsv = csv.DictReader(inCsvFile)
		outFieldNames = [tempNet.makeValidName(f) for f in inCsv.fieldnames]
		outCsv = csv.DictWriter(outCsvFile, outFieldNames + [
			'CI_DaysSincePreviousPhase', 'CI_DaysToNextPhase', 'CI_ICU', 'CI_Ventilation'])
		outCsv.writeheader()
		
		phaseDays = re.split(r'\s*,\s*', "bl_ucstartday, bl_costartday, bl_crstartday, bl_restartday")
		
		def groupDurations(dur):
			try:
				dur = float(dur)
				if dur > 0:
					return 'g1'
				elif dur < 0:
					return 'unknown'
				elif dur == 0:
					return 'g0'
			except: pass
			
			return 'unknown'
		
		for row in inCsv:
			nextPhase = float('inf')
			prevPhase = float('inf')
			nextPhaseState = None
			prevPhaseState = None
			ciIcu = groupDurations(row['bl_duration_icustay'])
			ciVentilation = groupDurations(row['bl_duration_ventilation'])
			outRow = {}
			for key,val in row.items():
				key = tempNet.makeValidName(key)
				outRow[key] = tempNet.makeValidName(val)#re.sub(r'^_', 's', val)
				if key in phaseDays:
					try:
						val = float(val)
						absVal = abs(val)
						pos = val >= 0
						if pos and absVal < abs(nextPhase):
							nextPhase = val
							# Group into 5 day periods
							nextPhaseState = 'g' + str(int(absVal/5)*5)
						if not pos and absVal < abs(prevPhase):
							prevPhase = val
							# Group into 5 day periods
							prevPhaseState = 'g' + str(int(absVal/5)*5)
					except: pass
			outCsv.writerow({**outRow,
				'CI_DaysSincePreviousPhase': None if prevPhase == float('inf') else prevPhaseState,
				'CI_DaysToNextPhase': None if nextPhase == float('inf') else nextPhaseState,
				'CI_ICU': ciIcu,
				'CI_Ventilation': ciVentilation,
			})		

def makeExample():
	csvFn = paths.out('baseline_resolvedOnly.csv')
	newCsvFn = csvFn #'data/LEOSS_encoded_data_bnsafe.csv'
	progressionCsvFn = paths.out('progression_training.csv')
	
	baseDir = paths.out('bns/nb/')
	os.makedirs(baseDir, exist_ok=True)
	
	# print('Pre-processing')
	# makeBnSafe(csvFn, newCsvFn)
	
	# XXX Remove
	#newCsvFn = 'data/LEOSS_encoded_data_bnsafe_train1.csv'
	
	num = 0
	# print(f'DSDECOD BNs')
	# num += 1; print(f'Making NB {num}: DSDECOD (Netica)')
	# makeNb(newCsvFn, f'{baseDir}nb_dsdecod_only.dne', selNodes, type = 'netica', parentNodes = ['DSDECOD'])
	# num += 1; print(f'Making TAN {num}: DSDECOD (Netica)')
	# makeTan(newCsvFn, f'{baseDir}tan_dsdecod_only.dne', selNodes, type = 'netica', parentNodes = ['DSDECOD'])
	
	# print(f'ci_ICU BNs')
	# num += 1; print(f'Making NB {num}: ci_ICU (Netica)')
	# makeNb(newCsvFn, f'{baseDir}nb_icu_only.dne', selNodes, type = 'netica', parentNodes = ['ci_ICU'])
	# num += 1; print(f'Making TAN {num}: ci_ICU (Netica)')
	# makeTan(newCsvFn, f'{baseDir}tan_icu_only.dne', selNodes, type = 'netica', parentNodes = ['ci_ICU'])
	
	# print(f'ci_InvVent BNs')
	# num += 1; print(f'Making NB {num}: ci_InvVent (Netica)')
	# makeNb(newCsvFn, f'{baseDir}nb_invvent_only.dne', selNodes, type = 'netica', parentNodes = ['ci_InvVent'])
	# num += 1; print(f'Making TAN {num}: ci_InvVent (Netica)')
	# makeTan(newCsvFn, f'{baseDir}tan_invvent_only.dne', selNodes, type = 'netica', parentNodes = ['ci_InvVent'])
	
	progNodes = ['ci_age_group_bg', 'ci_gender_bg', 'ci_hypertension_bg', 'ci_obesity_bg', 'ci_metabolic_syndrome_bg', 'ci_diabetes_bg', 'ci_smoking_history_bg', 'ci_chronic_pul_disease_bg', 'ci_chronic_cardiac_disease_bg', 'ci_chronic_kidney_disease_bg', 'ci_liver_disease_bg', 'ci_sys_immune_resp_t0', 'ci_cardiac_output_t0', 'ci_func_car_t0', 'ci_hypoxaemia_t0', 'ci_func_pul_t0', 'ci_coagulation_t0', 'ci_end_organ_perf_t0', 'ci_intravas_volume_t0', 'ci_antiviral_treat_t0', 'ci_antiinflam_treat_t0', 'ci_anticoag_treat_t0', 'ci_pulmonary_support_t0', 'ci_cardiac_support_t0', 'ci_status_t0', 'ci_crp_t0', 'ci_neut_t0', 'ci_lym_t0', 'ci_ldh_t0', 'ci_respiratory_rate_t0', 'ci_pco2_t0', 'ci_oxygen_saturation_t0', 'ci_po2_t0', 'ci_pulse_rate_t0', 'ci_troponin_t0', 'ci_systolic_blood_pressure_t0', 'ci_diastolic_blood_pressure_t0', 'ci_ddimer_t0', 'ci_aptt_t0', 'ci_plat_t0', 'ci_lacticac_t0', 'ci_creat_t0', 'ci_hct_t0', 'ci_sys_immune_resp_t1', 'ci_cardiac_output_t1', 'ci_func_car_t1', 'ci_hypoxaemia_t1', 'ci_func_pul_t1', 'ci_coagulation_t1', 'ci_end_organ_perf_t1', 'ci_intravas_volume_t1', 'ci_antiviral_treat_t1', 'ci_antiinflam_treat_t1', 'ci_anticoag_treat_t1', 'ci_pulmonary_support_t1', 'ci_cardiac_support_t1', 'ci_status_t1', 'ci_crp_t1', 'ci_neut_t1', 'ci_lym_t1', 'ci_ldh_t1', 'ci_respiratory_rate_t1', 'ci_pco2_t1', 'ci_oxygen_saturation_t1', 'ci_po2_t1', 'ci_pulse_rate_t1', 'ci_troponin_t1', 'ci_systolic_blood_pressure_t1', 'ci_diastolic_blood_pressure_t1', 'ci_ddimer_t1', 'ci_aptt_t1', 'ci_plat_t1', 'ci_lacticac_t1', 'ci_creat_t1', 'ci_hct_t1', 'ci_status']

	
	print(f'Progression BNs')
	num += 1; print(f'Making NB {num}: ci_status (Netica)')
	convertToNb('bns/progression.trained.dne', parentNodes = ['ci_status'], type = 'netica', dataFn = progressionCsvFn, outNetFn = f'{baseDir}prognb_status.dne')

	num += 1; print(f'Making NB {num}: ci_status_t1 (Netica)')
	convertToNb('bns/progression.trained.dne', parentNodes = ['ci_status_t1'], type = 'netica', dataFn = progressionCsvFn, outNetFn = f'{baseDir}prognb_status_t1.dne')
	
	print('All BNs written to {}/'.format(os.getcwd()))

if __name__ == '__main__':
	makeExample()