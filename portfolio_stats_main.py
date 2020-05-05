import json
import math
from datetime import date
from os import listdir
from os.path import isfile, join

import numpy as np
import pandas as pd
import scipy.stats as s
import yfinance as yf
from tqdm import tqdm


def main(portfolio_uri):
	datez = list(map(int, portfolio_uri[-15:-5].split('-')))
	dates = date(year=datez[0], month=datez[1], day=datez[2])
	portfolio_prices = pd.read_json(portfolio_uri, orient="table").set_index('Date')
	clean_prices = clean(portfolio_prices)
	returns = get_ln_returns(clean_prices)
	means, adjusted_returns = get_return_over_mean(returns)
	weights = get_weights(dates, adjusted_returns.columns)
	means = means.drop_duplicates(keep="first")
	# TODO fix difference in between the index of weights and means
	adjusted_mean = {}
	weighted_sigmas = {}
	sigmas = adjusted_returns.std()
	for col in weights.index:
		adjusted_mean[col] = weights[col] * means[col]
		weighted_sigmas[col] = sigmas[col] * weights[col]
	adjusted_mean = pd.Series(adjusted_mean)
	weighted_sigmas = pd.Series(weighted_sigmas)
	compound_weighted_month_return = adjusted_mean.sum()
	weighted_portfolio_sigma = weighted_sigmas.sum()

	weighted_port_stats = portfolio_regression(returns, portfolio_prices.index[0], weights)

	return weighted_port_stats


def clean(prices):
	"""removes all columns with null values"""
	# TODO count number of deleted values
	return prices.dropna(how="any", axis=1)


def get_weights(this_date, this_ind):
	"""accepts the date as a date object and returns the weight of total portfolio in a pandas series that each asset
	has based on the fair market value as listed in the export.json file for that year, note: this is done off of the FMV"""
	this_date = this_date.strftime("%m/%d/%y")
	with open("data_acquisition/inputs/export.json", "r") as file:
		reader = json.load(file)
	temp_df = pd.read_json(reader[this_date])
	drop_rows = temp_df[temp_df['ticker'].isin([x for x in temp_df['ticker'].values if x not in this_ind])].index
	temp_df = temp_df.drop(drop_rows).set_index(['ticker'])
	# I didn't account for duplicates
	FMVs = temp_df['fair_value']

	FMVs = FMVs.groupby(by=FMVs.index).sum()

	total = np.sum(FMVs)
	weights = FMVs.divide(total)
	return weights


def get_ln_returns(portfolio_prices):
	output_df = portfolio_prices.copy()
	for col in portfolio_prices:
		for d in range(1, len(portfolio_prices[col])):
			output_df[col][d] = math.log(portfolio_prices[col][d] / portfolio_prices[col][d - 1])
	output_df = (output_df.drop(output_df.index[0]))
	return output_df


def get_return_over_mean(this_ln_returns):
	"returns mean for each security and a dataframe of its daily ln returns"
	means = {}
	this_ln_returns_copy = this_ln_returns.copy()
	for col in this_ln_returns:
		this_mean = (np.average(this_ln_returns[col]))
		means[col] = this_mean
		for row in this_ln_returns[col].index:
			this_ln_returns_copy[col][row] = this_ln_returns[col][row] - means[col]
		means = pd.Series(means)
	return means, this_ln_returns_copy


def portfolio_regression(log_returns, dates, weights):
	"""returns a pandas pataframe of each assets and its alpha, beta, RSQ and intercept/slope t-test"""
	# Need to get ^GSPC from the date listed on the portfolio - 5 years and run it through the return over mean function
	start = dates
	end = log_returns.index[-1]
	GSPC = ""

	# initialize my S&P 500 Series
	GSPC = pd.DataFrame({"GSPC": yf.download('^GSPC', start=start, end=end, interval='1mo')['Adj Close']})
	GSPC = get_ln_returns(GSPC)
	mean, _ = get_return_over_mean(GSPC)
	GSPC = GSPC['GSPC']
	_, _, GSPCreturn, GSPCvar, GSPCskew, GSPCkurt = s.describe(GSPC)

	# find stats
	all_stats_month = {}
	for col in log_returns:
		instance = log_returns[col]

		_, _, mean, var, skew, kurt = (s.describe(instance))
		# print(GSPC)
		# print(end)
		try:
			slope, intercept, r_value, p_value, std_err = s.linregress(instance, GSPC)
		except ValueError:
			instance1 = instance.drop(instance.index[-1])
			slope, intercept, r_value, p_value, std_err = s.linregress(instance1, GSPC)

		all_stats_month[col] = {
			"return": mean,
			"std": math.sqrt(var),
			"skew": skew,
			"kurt": kurt,
			"beta": slope,
			"alpha": intercept,
			"RSQ": r_value ** 2,
			"p_value": p_value,
			"std_err": std_err
		}

	all_stats_month = pd.DataFrame(all_stats_month).transpose()
	export_cols = ['return', 'std', 'skew', 'kurt', 'alpha', 'beta', 'RSQ', 'p_value', 'std_err']
	all_stats_month = all_stats_month[export_cols]
	# for ind in all_stats_month.index:
	num = len(all_stats_month.index)
	weighted_data = (all_stats_month.multiply(weights[all_stats_month.index], axis=0).sum(axis=0))
	weighted_data["index_return"] = GSPCreturn
	weighted_data["index_std"] = math.sqrt(GSPCvar)
	weighted_data["index_skew"] = GSPCskew
	weighted_data["index_kurt"] = GSPCkurt
	weighted_data["no_stocks"] = num
	# get sum of column after it's multiplied by its weights
	name = end.strftime("%Y-%m-%d")
	all_stats_month.to_csv(f'Final_data/ind_portfolios/{name}.csv')
	all_stats_month.to_json(f'Final_Data/ind_portfolios/jsons/{name}.json', indent=4)

	return weighted_data


if __name__ == '__main__':
	base_url = r"data_acquisition/historical_prices/jsons/"
	mypath = r"data_acquisition/historical_prices/jsons/"
	uris = [f for f in listdir(mypath) if isfile(join(mypath, f))]
	stats = {}
	for portfolio_uri in tqdm(uris):
		# print(f"{base_url}{portfolio_uri}")
		stats[f'portfolio: {portfolio_uri[-15:-5]}'] = main(f"{base_url}{portfolio_uri}")
	stats = pd.DataFrame(stats).transpose()
	stats.to_csv("Final_Data/final_csv.csv")
	stats.to_json("Final_Data/final_json.json", indent=4)
