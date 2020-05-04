import json
from os import listdir
from os.path import isfile, join

import pandas as pd

uri = 'outputs/'

onlyfiles = [f for f in listdir(uri) if isfile(join(uri, f))]
i = 0
dfs = {}
tickers = []
nan_names = []
for file in onlyfiles:
	i += 1
	with open(f'data_acquisition/outputs/{file}', 'r') as inp:
		x = (json.load(inp))
		for y in x:
			z = json.loads(x[y])
			for p in z:
				dfs[y] = (pd.DataFrame(z))
			# find the errors in the ticker value and add them to a list to fix later
			nans = dfs[y][(dfs[y]['ticker'].isna()) == True].index
			drops = dfs[y]['ticker'].str.endswith("*")
			drop_index = drops[drops == True].index
			dfs[y] = dfs[y].drop(drop_index)
			unique = dfs[y][(dfs[y]['ticker'].isna()) != True]['ticker']

			# drop values without a ticker
			dfs[y] = dfs[y].drop(nans)

			for ind in nans:
				nan_names.append(ind)
			for unq in unique:
				tickers.append(unq)
			dfs[y] = dfs[y].to_json()

# save error stats into a stats file to keep track of errors
nan_names = pd.Series(nan_names).drop_duplicates()
ticker_lookup_error_count = len(nan_names)
nan_names = nan_names.to_json()

unique = pd.Series(unique).drop_duplicates().to_json()

with open('inputs/stats.txt', 'w') as out:
	out.write(f'ticker Lookup Error Count: {ticker_lookup_error_count}')
	out.write(nan_names)

with open('inputs/unique.json', 'w') as out:
	out.write(unique)

with open('inputs/export.json', 'w') as out:
	json.dump(dfs, out)
