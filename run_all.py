import subprocess, os, json, time

def runPipeline(scripts, outDir = 'out'):
	runTimesFn = os.path.join(outDir,'runTimes.json')
	runTimes = {}
	try:
		with open(runTimesFn) as rt:
			runTimes = json.load(rt)
	except: pass
	for i,script in enumerate(scripts):
		if callable(script):
			print(f'Calling "{script.__name__}" (Script {i+1} of {len(scripts)})')
			if script.__name__ in runTimes:
				print(f'(Previous run took {round(runTimes[script.__name__],2)}s)')
			try:
				t = time.time()
				script()
				runTimes[script.__name__] = time.time() - t
				print(f'This run took {round(runTimes[script.__name__],2)}s')
				with open(runTimesFn, 'w') as rt:
					json.dump(runTimes, rt)
			except Exception as e:
				traceback.print_exc()
				print(f'\nScript {i+1} failed. (See above for error.)')
			print()
		else:
			print(f'Running "{script}" (Script {i+1} of {len(scripts)})')
			if str(script) in runTimes:
				print(f'(Previous run took {round(runTimes[str(script)],2)}s)')
			try:
				t = time.time()
				args = script if isinstance(script, list) else [script]
				subprocess.run(['python','-u',*args], check=True)
				runTimes[str(script)] = time.time() - t
				print(f'This run took {round(runTimes[str(script)],2)}s')
				with open(runTimesFn, 'w') as rt:
					json.dump(runTimes, rt)
			except Exception as e:
				print(e)
				print(f'\nScript {i+1} failed. (See above for error.)')
			print()

	with open(runTimesFn, 'w') as rt:
		json.dump(runTimes, rt)
		
# NOTE: Script pipeline. Comment/uncomment (or delete/add) to turn things on and off
# (It's possible to add both functions and scripts, but you should prefer scripts.)
runPipeline([
	'make_sample_sets.py',
	'make_nb.py',
	'validations.py',
])