import json, os

class Paths:
	def __init__(self, dirs):
		self.dirs = dirs
	
	def __getattr__(self, name):
		if name not in self.dirs:  raise AttributeError
		
		def getPath(*files, makeDir=False):
			newPath = os.path.join(self.dirs[name], *files)
			if makeDir:
				os.makedirs(newPath, exist_ok=True)
			return newPath
		return getPath

def setup():
	with open('settings.json') as s:  settings = json.load(s)

	# Before we start, check the directories specified in settings
	def checkDirs():
		for dir in settings['dirs']:
			if not os.path.isdir(dir):
				print(f'Directory {dir} from the settings file not found, attempting to create')
				os.makedirs(dir)

	checkDirs()

	# dataDir = settings['dirs']['iddoData']

	paths = Paths(settings['dirs'])
	
	return paths, settings