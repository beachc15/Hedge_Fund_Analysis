import json
import multiprocessing
from datetime import date, timedelta

import pandas as pd
import yfinance as yf

"""find the 10 year historic monthly data for each stock in a portfolio at a given time"""


def main(portfolio_tup):
	x = []
	for items in portfolio_tup:
		this_portfolio = items[0]
		this_date = items[1]
		df = pd.read_json(this_portfolio)
		pf = df['ticker'].values
		x.append(lookup(pf, this_date))
	return x


def lookup(portfolio, my_date):
	"""pass in a numpy.ndarray of stock tickers and will return a dataframe of the monthly closing prices
	 of the past 5 years of stock returns"""
	tickers = ' '.join(portfolio)
	time_d = timedelta(weeks=260)
	this_date = my_date - time_d
	df = yf.download(tickers,
	                 start=this_date.strftime("%Y-%m-%d"),
	                 end=my_date.strftime("%Y-%m-%d"),
	                 interval="1mo",
	                 period="5y",
	                 threads=False)
	df = df.dropna(how="all", axis=1)
	df = df.dropna(how="all", axis=0)
	df = df['Adj Close']
	df.to_csv(f'historical_prices/historical_prices_{my_date}.csv')
	return my_date


if __name__ == '__main__':
	portfolios = []
	with open('inputs/export.json') as file:
		reader = json.load(file)
		for port in reader:
			dt = list(map(int, (port.split('/'))))
			# NOTE: year function only works for dates after the year 2000 at this point as it's hardcoded
			portfolio = (reader[port], date(month=dt[0], day=dt[1], year=int(f"20{dt[2]}")))
			portfolios.append(portfolio)
	# HARDCODED initialize all of my processes and assign them their tasks
	one = [portfolios[x] for x in range(len(portfolios)) if 0 <= x <= 2]
	two = [portfolios[x] for x in range(len(portfolios)) if 3 <= x <= 5]
	three = [portfolios[x] for x in range(len(portfolios)) if 6 <= x <= 8]
	four = [portfolios[x] for x in range(len(portfolios)) if 9 <= x <= 10]
	five = [portfolios[x] for x in range(len(portfolios)) if 11 <= x <= 13]
	six = [portfolios[x] for x in range(len(portfolios)) if 14 <= x <= 16]
	seven = [portfolios[x] for x in range(len(portfolios)) if 17 <= x <= 18]
	eight = [portfolios[x] for x in range(len(portfolios)) if 19 <= x <= 20]

	# one = portfolios[0:2]
	# two = portfolios[3:5]
	# three = portfolios[6:8]
	# four = portfolios[9:10]
	# five = portfolios[11:13]
	# six = portfolios[14:16]
	# seven = portfolios[17:18]
	# eight = portfolios[19:20]
	processes = [one, two, three, four, five, six, seven, eight]
	with multiprocessing.Pool(8) as p:
		print(p.map(main, processes))
