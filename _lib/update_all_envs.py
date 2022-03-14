import _env
import os, shutil

for root, dirs, files in os.walk(_env.root):
	for fileName in files:
		if fileName == "_env.py":
			targetFn = os.path.join(root, fileName)
			if os.path.abspath("_env.py") != targetFn:
				newFileName = "_env.py"
				print("Copying to ", os.path.join(root, newFileName))
				shutil.copyfile("_env.py", os.path.join(root, newFileName))

print("Hit 'enter' to close")
input()