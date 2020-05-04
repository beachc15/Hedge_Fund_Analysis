import math

import numpy as np
import pandas as pd
from tqdm import tqdm


def main(portfolio_uri):
	portfolio_prices = pd.read_json(portfolio_uri, orient="table").set_index('Date')
	clean_prices = clean(portfolio_prices)
	returns = get_ln_returns(clean_prices)
	adjusted_returns, df = get_return_over_mean(returns)
	# print(adjusted_returns)
	print(df)

	return ""


def clean(prices):
	"""removes all columns with null values"""
	# TODO count number of deleted values
	return prices.dropna(how="any", axis=1)


def get_weights():
	"""returns the weight of total portfolio in a pandas series that each asset has based on the fair
	market value as listed	in the export.json file for that year"""
	series = []
	return series


def get_ln_returns(portfolio_prices):
	output_df = portfolio_prices.copy()
	for col in tqdm(portfolio_prices):
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
	return means, this_ln_returns_copy


def get_variance(adjusted_returns):
	"""returns pandas series of the variance of each asset"""
	return None


def portfolio_regression(adjusted_returns, date):
	"""returns a pandas pataframe of each assets and its alpha, beta, RSQ and intercept/slope t-test"""
	# Need to get ^GSPC from the date listed on the portfolio - 5 years and run it through the return over mean function
	return None


if __name__ == '__main__':
	this_uri = "data_acquisition\historical_prices\jsons\historical_prices_2013-11-14.json"
	main(this_uri)
