import _env, csv, time
from bni_netica import *
from ow_utils import *
from interpolator import *

    
def build(ts = ['na','t0','t1']):
	print('Building BN')
	t = time.time()

	def reverse(lst):
		lst.reverse()
		return lst
	
	def addParents(node, parents):
		net.node(node).addParents([parent for parent in parents if net.node(parent)])
		

	def findLevels(node, dataFn = 'out/progression_training.csv'):
		data = []
		with open(dataFn, 'r') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				data.append(row[node])
		data = [float(x) for x in data if x != '*']
				
		quan = calcQuantiles(data, 0.2)
		quan[0] = quan[0]-0.00001
		quan[-1] = quan[-1]+0.00001
		
		return quan


		

	net = Net()

	net.addNode('ci_age_group_bg',states=['senior','adult','young']).user().add('submodel','background')
	net.addNode('ci_gender_bg',states=['male','female']).user().add('submodel','background')
	net.addNode('ci_smoking_history_bg',states=['exsmoker','smoker','never']).user().add('submodel','background')
	net.addNode('ci_hypertension_bg',states=['true','false']).user().add('submodel','background')
	net.addNode('ci_obesity_bg',states=['true','false']).user().add('submodel','background')
	net.addNode('ci_metabolic_syndrome_bg',states=['true','false']).user().add('submodel','background')
	# net.addNode('ci_diabetes_bg',states=['yescomplications','yesnocomplications','false']).user().add('submodel','background')
	net.addNode('ci_diabetes_bg',states=['true','false']).user().add('submodel','background')
	net.addNode('ci_chronic_cardiac_disease_bg',states=['true','false']).user().add('submodel','background')
	net.addNode('ci_chronic_pul_disease_bg',states=['true','false']).user().add('submodel','background')
	net.addNode('ci_chronic_kidney_disease_bg',states=['true','false']).user().add('submodel','background')
	net.addNode('ci_liver_disease_bg',states=['true','false']).user().add('submodel','background')
	
	net.addNode('ci_status_plus1day',states=['dead','icu','hospital','discharged']).user().add('submodel','system')
	net.addNode('ci_pulmonary_support_plus1day',states=['high_flow_nasal','cpap','mechanical_ventilation','supplemental_o2','none']).user().add('submodel','treatment')

	for i in range(1,len(ts)):
		net.addNode('ci_anticoag_treat_'+ts[i],states=['true','false']).user().add('submodel','treatment')
		net.addNode('ci_antiinflam_treat_'+ts[i],states=['true','false']).user().add('submodel','treatment')
		# net.addNode('ci_pulmonary_support_'+ts[i],states=['true','false']).user().add('submodel','treatment')
		net.addNode('ci_pulmonary_support_'+ts[i],states=['high_flow_nasal','cpap','mechanical_ventilation','supplemental_o2','none']).user().add('submodel','treatment')
		
		net.addNode('ci_sys_immune_resp_'+ts[i],states=['abnormal','normal']).user().add('submodel','system')
		net.addNode('ci_status_'+ts[i],states=['dead','icu','hospital','discharged']).user().add('submodel','system')
		
		net.addNode('ci_coagulation_'+ts[i],states=['abnormal','normal']).user().add('submodel','vascular') 
		#29/11/21 Changed from 'low','normal'
		net.addNode('ci_end_organ_perf_'+ts[i],states=['low','normal']).user().add('submodel','vascular') #reduced, normal
		#29/11/21 Changed from 'low','normal'
		net.addNode('ci_intravas_volume_'+ts[i],states=['low','normal']).user().add('submodel','vascular') #reduced, normal 

		#29/11/21 Changed from 'abnormal','normal'
		net.addNode('ci_func_pul_'+ts[i],states=['abnormal','normal']).user().add('submodel','pulmonary')  #reduced, normal
		net.addNode('ci_hypoxaemia_'+ts[i],states=['verylow','low','normal']).user().add('submodel','pulmonary') #severe, moderate, normal


		#29/11/21 Changed from 'abnormal','normal'
		net.addNode('ci_func_car_'+ts[i],states=['abnormal','normal']).user().add('submodel','cardiac')  #reduced, normal
		
		#29/11/21 Changed from 'low','normal'
		net.addNode('ci_cardiac_output_'+ts[i],states=['low','normal']).user().add('submodel','cardiac')  #reduced, normal
	
		
	net.addNode('ci_status',states=['dead','discharged']).user().add('submodel','system')
	net.addNode('ci_icu',states=['ND','reported']).user().add('submodel','system')
	net.addNode('ci_inv_vent',states=['ND','reported']).user().add('submodel','system')
	
	
	addParents('ci_age_group_bg',[])
	addParents('ci_gender_bg',['ci_age_group_bg'])
	addParents('ci_smoking_history_bg',['ci_gender_bg','ci_age_group_bg'])
	addParents('ci_hypertension_bg',['ci_gender_bg','ci_age_group_bg'])
	addParents('ci_obesity_bg',['ci_gender_bg','ci_age_group_bg'])
	addParents('ci_metabolic_syndrome_bg',['ci_hypertension_bg','ci_obesity_bg'])
	# addParents('ci_diabetes_bg', ['ci_gender_bg','ci_age_group_bg','ci_metabolic_syndrome_bg','ci_chronic_kidney_disease_bg','ci_liver_disease_bg'])
	addParents('ci_diabetes_bg', ['ci_gender_bg','ci_age_group_bg','ci_metabolic_syndrome_bg'])
	addParents('ci_chronic_cardiac_disease_bg',['ci_gender_bg','ci_age_group_bg','ci_chronic_pul_disease_bg','ci_diabetes_bg'])
	addParents('ci_chronic_pul_disease_bg',['ci_gender_bg','ci_age_group_bg','ci_smoking_history_bg'])
	# addParents('ci_chronic_kidney_disease_bg',['ci_gender_bg','ci_age_group_bg'])
	# addParents('ci_liver_disease_bg',['ci_gender_bg','ci_age_group_bg'])
	addParents('ci_chronic_kidney_disease_bg',['ci_gender_bg','ci_age_group_bg','ci_diabetes_bg'])
	addParents('ci_liver_disease_bg',['ci_gender_bg','ci_age_group_bg','ci_diabetes_bg'])
	
	addParents('ci_status_plus1day',['ci_status_t0','ci_func_pul_t0','ci_func_car_t0','ci_end_organ_perf_t0'])
	addParents('ci_pulmonary_support_plus1day',['ci_pulmonary_support_t0', 'ci_func_pul_t0'])

	
	for i in range(1,len(ts)):
		addParents('ci_antiinflam_treat_'+ts[i],['ci_antiinflam_treat_'+ts[i-1],'ci_sys_immune_resp_'+ts[i-1]])
		addParents('ci_anticoag_treat_'+ts[i],['ci_anticoag_treat_'+ts[i-1],'ci_coagulation_'+ts[i-1]])
		addParents('ci_pulmonary_support_'+ts[i],['ci_pulmonary_support_'+ts[i-1], 'ci_func_pul_'+ts[i]])

		addParents('ci_sys_immune_resp_'+ts[i],['ci_gender_bg','ci_age_group_bg','ci_metabolic_syndrome_bg','ci_sys_immune_resp_'+ts[i-1],'ci_func_pul_'+ts[i-1],'ci_func_car_'+ts[i-1],'ci_antiinflam_treat_'+ts[i]])
		
		addParents('ci_intravas_volume_'+ts[i],['ci_hypertension_bg','ci_diabetes_bg','ci_intravas_volume_'+ts[i-1],'ci_sys_immune_resp_'+ts[i],'ci_coagulation_'+ts[i]])
		addParents('ci_coagulation_'+ts[i],['ci_diabetes_bg','ci_coagulation_'+ts[i-1],'ci_sys_immune_resp_'+ts[i],'ci_anticoag_treat_'+ts[i]])
		addParents('ci_end_organ_perf_'+ts[i],['ci_diabetes_bg','ci_end_organ_perf_'+ts[i-1],'ci_sys_immune_resp_'+ts[i],'ci_coagulation_'+ts[i], 'ci_hypoxaemia_'+ts[i], 'ci_cardiac_output_'+ts[i],'ci_chronic_kidney_disease_bg','ci_liver_disease_bg'])

		addParents('ci_func_pul_'+ts[i],['ci_age_group_bg','ci_obesity_bg','ci_chronic_pul_disease_bg','ci_func_pul_'+ts[i-1],'ci_coagulation_'+ts[i],'ci_sys_immune_resp_'+ts[i],'ci_antiinflam_treat_'+ts[i]])
		addParents('ci_hypoxaemia_'+ts[i],['ci_func_pul_'+ts[i],'ci_pulmonary_support_'+ts[i]])

		addParents('ci_func_car_'+ts[i],['ci_age_group_bg','ci_chronic_cardiac_disease_bg','ci_hypertension_bg','ci_func_car_'+ts[i-1],'ci_intravas_volume_'+ts[i],'ci_coagulation_'+ts[i],'ci_hypoxaemia_'+ts[i],'ci_end_organ_perf_'+ts[i-1],'ci_sys_immune_resp_'+ts[i],'ci_antiinflam_treat_'+ts[i]])
		addParents('ci_cardiac_output_'+ts[i],['ci_func_car_'+ts[i],'ci_intravas_volume_'+ts[i]])
		
		addParents('ci_status_'+ts[i],['ci_status_'+ts[i-1],'ci_func_pul_'+ts[i],'ci_func_car_'+ts[i],'ci_end_organ_perf_'+ts[i]])#,'ci_chronic_kidney_disease_bg','ci_liver_disease_bg'])
		
	addParents('ci_status',['ci_status_'+ts[-1],'ci_func_pul_'+ts[-1],'ci_func_car_'+ts[-1],'ci_end_organ_perf_'+ts[-1]])#,'ci_chronic_kidney_disease_bg','ci_liver_disease_bg'])
	addParents('ci_icu',['ci_status_'+ts[-1],'ci_func_pul_'+ts[-1],'ci_func_car_'+ts[-1],'ci_end_organ_perf_'+ts[-1]])#,'ci_chronic_kidney_disease_bg','ci_liver_disease_bg'])
	addParents('ci_inv_vent',['ci_pulmonary_support_t0', 'ci_pulmonary_support_t1', 'ci_func_pul_'+ts[-1]])
	
	for i in range(1,len(ts)):		
	
		net.addNode('ci_crp_'+ts[i],states=findLevels('ci_crp_'+ts[i])).user().add('submodel','system')
		net.addNode('ci_neut_'+ts[i],states=findLevels('ci_neut_'+ts[i])).user().add('submodel','system')
		net.addNode('ci_lym_'+ts[i],states=findLevels('ci_lym_'+ts[i])).user().add('submodel','system')
		net.addNode('ci_ldh_'+ts[i],states=findLevels('ci_ldh_'+ts[i])).user().add('submodel','system')
		
		net.addNode('ci_pulse_rate_'+ts[i],states=findLevels('ci_pulse_rate_'+ts[i])).user().add('submodel','cardiac')
		net.addNode('ci_troponin_'+ts[i],states=findLevels('ci_troponin_'+ts[i])).user().add('submodel','cardiac')
		net.addNode('ci_systolic_blood_pressure_'+ts[i],states=reverse(findLevels('ci_systolic_blood_pressure_'+ts[i]))).user().add('submodel','cardiac')
		net.addNode('ci_diastolic_blood_pressure_'+ts[i],states=reverse(findLevels('ci_diastolic_blood_pressure_'+ts[i]))).user().add('submodel','cardiac')
		
		net.addNode('ci_respiratory_rate_'+ts[i],states=findLevels('ci_respiratory_rate_'+ts[i])).user().add('submodel','pulmonary')		net.addNode('ci_pco2_'+ts[i],states=findLevels('ci_pco2_'+ts[i])).user().add('submodel','pulmonary')
		net.addNode('ci_oxygen_saturation_'+ts[i],states=reverse(findLevels('ci_oxygen_saturation_'+ts[i]))).user().add('submodel','pulmonary')
		net.addNode('ci_po2_'+ts[i],states=reverse(findLevels('ci_po2_'+ts[i]))).user().add('submodel','pulmonary')
		
		net.addNode('ci_ddimer_'+ts[i],states=findLevels('ci_ddimer_'+ts[i])).user().add('submodel','vascular')
		net.addNode('ci_aptt_'+ts[i],states=findLevels('ci_aptt_'+ts[i])).user().add('submodel','vascular')
		net.addNode('ci_plat_'+ts[i],states=reverse(findLevels('ci_plat_'+ts[i]))).user().add('submodel','vascular')
		net.addNode('ci_lacticac_'+ts[i],states=findLevels('ci_lacticac_'+ts[i])).user().add('submodel','vascular')
		net.addNode('ci_creat_'+ts[i],states=findLevels('ci_creat_'+ts[i])).user().add('submodel','vascular')
		net.addNode('ci_hct_'+ts[i],states=findLevels('ci_hct_'+ts[i])).user().add('submodel','vascular')
		
		addParents('ci_crp_'+ts[i], ['ci_sys_immune_resp_'+ts[i]])
		addParents('ci_neut_'+ts[i], ['ci_sys_immune_resp_'+ts[i]])
		addParents('ci_lym_'+ts[i], ['ci_sys_immune_resp_'+ts[i]])
		addParents('ci_ldh_'+ts[i], ['ci_sys_immune_resp_'+ts[i]])
		
		addParents('ci_respiratory_rate_'+ts[i], ['ci_func_pul_'+ts[i]])
		addParents('ci_pco2_'+ts[i], ['ci_func_pul_'+ts[i]])
		addParents('ci_oxygen_saturation_'+ts[i], ['ci_hypoxaemia_'+ts[i]])
		addParents('ci_po2_'+ts[i], ['ci_hypoxaemia_'+ts[i]])

		addParents('ci_pulse_rate_'+ts[i],['ci_func_car_'+ts[i]])
		addParents('ci_troponin_'+ts[i],['ci_func_car_'+ts[i]])
		addParents('ci_systolic_blood_pressure_'+ts[i],['ci_cardiac_output_'+ts[i]])
		addParents('ci_diastolic_blood_pressure_'+ts[i],['ci_cardiac_output_'+ts[i]])
		
		addParents('ci_ddimer_'+ts[i], ['ci_coagulation_'+ts[i]])
		addParents('ci_aptt_'+ts[i], ['ci_coagulation_'+ts[i]])
		addParents('ci_plat_'+ts[i], ['ci_coagulation_'+ts[i]])
		addParents('ci_lacticac_'+ts[i], ['ci_end_organ_perf_'+ts[i]])
		addParents('ci_creat_'+ts[i], ['ci_end_organ_perf_'+ts[i],'ci_intravas_volume_'+ts[i]])
		addParents('ci_hct_'+ts[i], ['ci_intravas_volume_'+ts[i]])
		
	
	for node in net.nodes():
		node.title(node.name().replace('_',' '))
		# initialiseCPT(node)
		
			
	try:
		with open('bns/cptsetup.json') as json_file:
			cptsetup = json.load(json_file)
	except:
		cptsetup = None
		
	loadSetup(net, cptsetup)
	
	net.write('bns/progression.dne')
		
	print(f'Done ({round(time.time()-t,2)}s)')
	
	return net
		
	
def listNodes(netFn):
	net = Net(netFn)
	nodes = [node.name() for node in net.nodes()]
	nodes.sort()
	print(nodes)
	

if __name__ == "__main__":
	net = build()#['na', 'bl', 'day10'])
	dne2xdsl('bns/progression.dne')

	matchLayout('bns/progression.dne', 'bns/template_iddo.dne')


