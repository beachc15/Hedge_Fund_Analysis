from datetime import date
from os import listdir
from os.path import isfile, join

import pandas as pd
import yfinance as yf

import portfolio_stats_main


def main():
	mypath = 'data_acquisition/historical_prices/jsons'
	onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
	for file in onlyfiles:
		output = {}
		uri = f'{mypath}/{file}'
		df = pd.read_json(uri, orient='table')
		this_date = file[-15:-5]
		returns, gspc = get_portfolio_daily_return(df, this_date)
		output = {"portfolio": returns, "SP500": gspc}
		print(type(returns))
		output = pd.DataFrame({"portfolio": returns, "SP500": gspc})
		output.to_csv(f'Final_Data/data_analysis/portfolio_returns/{this_date}.csv')


def get_portfolio_daily_return(portfolio, dates1):
	gspc = []

	dates = list(map(int, dates1.split('-')))
	df1 = portfolio
	df1 = df1.set_index(['Date'])
	# print(df1)
	wts = portfolio_stats_main.get_weights(
		date(year=int(dates[0]), month=int(dates[1]),
		     day=int(dates[2])), df1.columns)

	start = df1.index[0]
	end = dates1

	# initialize my S&P 500 Series
	gspc = pd.Series(yf.download('^GSPC', start=start, end=end, interval='1mo')['Adj Close'])

	returns = df1.multiply(wts).sum(axis=1)
	return returns, gspc


if __name__ == '__main__':
	main()
