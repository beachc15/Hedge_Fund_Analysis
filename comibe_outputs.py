import json
from os import listdir
from os.path import isfile, join

uri = 'outputs/'

onlyfiles = [f for f in listdir(uri) if isfile(join(uri, f))]
i = 0
x = {}
for file in onlyfiles:
	print(i)
	i += 1
	with open(f'outputs/{file}', 'r') as inp:
		x[i] = inp.read()

with open('outputs/export.json', 'w') as out:
	json.dump(x, out)
