from os import listdir
from os.path import isfile, join

import pandas as pd

"""just converts my .csv files into json files under the orig_url directory"""

orig_url = 'historical_prices'
output_url = 'historical_prices/jsons/'
onlyfiles = [f for f in listdir(orig_url) if isfile(join(orig_url, f))]
for file in onlyfiles:
	with open(f"{orig_url}/{file}", "r") as inp:
		df = pd.read_csv(inp)
		df.to_json(f"{output_url}{file[:-4]}.json", orient="table")
