# IMPORTANT: Import this first, before anything else
#
# (Specifically, it needs to be imported before anything in _lib is accessed)
#

# Find the nearest _lib folder for imports
import sys, os
d = os.path.abspath('./_lib'); d2 = None
while not os.path.exists(d):
	d2 = d; d = os.path.join(os.path.dirname(os.path.dirname(d)), '_lib')
	if d == d2: raise SystemExit('Could not find "_lib" folder in any parent folder')
sys.path.append(d); os.environ['PATH'] += ';'+d

# Point to the root dir
root = os.path.dirname(d)

if __name__ == "__main__":
	import subprocess
	subprocess.check_call('start powershell', shell=True)