"""This python file processess the unique values to find the info from yfinance such as Dividend information,
portfolio recommendations"""
import json

import yfinance as yf
from tqdm import tqdm


def main(portfolio):
	export = meat(portfolio)
	return export


def meat(portfolio):
	out = {}
	for symbol in tqdm(portfolio):
		tick = yf.Ticker(portfolio[symbol])
		keep_cols = ['dividendRate', 'heldPercentInsiders']
		save = {}
		try:
			info = tick.info
			for col in keep_cols:
				save[col] = info[col]
			out[symbol] = save
		except IndexError:
			pass
		except KeyError:
			pass
		except ValueError:
			pass
	return out


if __name__ == '__main__':
	# todo filter out values ending in *: I could do this inside the main method though
	with open('inputs/unique.json', 'r') as file:
		selection = json.load(file)
	z = main(selection)
	with open('inputs/uniquestats.json', 'w') as file:
		json.dump(z, file)
