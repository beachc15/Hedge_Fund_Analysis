import json
import re
from datetime import date as date
from multiprocessing import Pool

import pandas as pd
import requests
from bs4 import BeautifulSoup
from numpy import nan
from tqdm import tqdm

import scraper as s


def main():
	"""Method: go to URL for hedge fund's EDGAR search with a filter for "13F-HR" then target the URL of the
	"Documents" section of the table. Add these to a tuple to keep track of what has been accessed and what hasn't
	Access those URLs. If a link ends in ".xml" then access that file otherwise look for document in the table next to
	the "description" value named "FORM 13F-HR".

	NOTE: as of right now, this only goes back to around 2008
	"""
	urls = reroute_urls()
	ind = [x for x in urls]
	print(len(ind))
	one = [{x: urls[x]} for x in ind[0: 3]]
	two = [{x: urls[x]} for x in ind[4: 6]]
	three = [{x: urls[x]} for x in ind[7: 9]]
	four = [{x: urls[x]} for x in ind[10: 13]]
	five = [{x: urls[x]} for x in ind[14: 17]]
	six = [{x: urls[x]} for x in ind[18: 21]]
	seven = [{x: urls[x]} for x in ind[22: 24]]
	eight = [{x: urls[x]} for x in ind[24:26]]

	with Pool(8) as p:
		output = (p.map(get_data, [one, two, three, four, five, six, seven, eight]))
	for ex in range(len(output)):
		with open(f"output{ex}.json", 'w') as file:
			json.dump(output[ex], file)
	return "finished"


def reroute_urls():
	"""looks through table and returns a dictionary with the title of filing date and the value of the url"""

	# TODO convert this system to get all 13H filings

	# initialize return variable and the original URL where we will get our data from

	def conv_to_datetime(this_date):
		date_parts = list(map(int, this_date.split('-')))
		exp_date = date(date_parts[0], date_parts[1], date_parts[2])
		return exp_date

	orig_url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000923093&type=13F-HR&dateb=&owner=include&count=100'
	url_dict = {}
	r = requests.get(orig_url)
	soup = BeautifulSoup(r.text, features="lxml")
	r.close()
	this_table = soup.find("table", {"class": "tableFile2"})
	rows = (this_table.findAll("tr"))

	for row in rows:
		# only save the data if the document is the 13F-HR. This filters out the amended statements
		if str(row.contents[1].contents[0]) != '13F-HR':
			pass
		else:
			# as the href tag is always the first thing in the line with quotations around it I split the row by
			# quotations and chose the first element after the first quotation before the second
			url = ('http://sec.gov{}'.format(str(row.contents[3].contents[0]).split("\"")[1]))
			dt = conv_to_datetime(str(row.contents[7].contents[0]))
			url_dict[dt] = url
	# print(len(url_dict))
	return url_dict


def get_data(urls):
	""" Accepts a list of URL strings of the filing detail page of the hedge funds' 13F-HR filing and returns a json
	formatted pandas DataFrame of all holdings of the Hedge Fund with the columns "Name", "Class", "CUSIP", "Fair Market
	Value", "type of position (put/call/nan)\""""

	def change_url(z, inp):
		z = z.split('/')
		z[-1] = inp
		return '/'.join(z)

	def get_req(this_url):
		# in this case I am HARDCODING the different outputs.
		# TODO make this function dynamically coded
		possible_end = {'xml': 'form13fInfoTable.xml',
		                'text1': 'd13fhr.txt',
		                'text2': 'd352933d13fhr.txt',
		                'text3': 'd256242d13fhr.txt',
		                'text4': 'd300436d13fhr.txt',
		                'text5': 'd396131d13fhr.txt',
		                'text6': 'd439517d13fhr.txt',
		                'text7': 'd487753d13fhr.txt',
		                'text8': 'd539741d13fhr.txt'}
		for ending in possible_end:
			new_url = change_url(this_url, possible_end[ending])
			r = requests.get(new_url)
			if len(r.text) > 10000:
				return r, ending

		else:
			print(f"error {this_url}")
			return 'error', this_url

	def xml_parse(res):
		soup = BeautifulSoup(res.text, "lxml-xml")
		data = (soup.contents[0].findAll('infoTable'))
		export = {}
		global errors, my_date
		errors = []
		for row in tqdm(data):
			name = row.find("nameOfIssuer").contents[0]
			try:
				put_call = row.find("putCall").contents[0]
			except AttributeError:
				put_call = nan
			# get stock symbol by CUSIP
			try:
				ticker = s.meat(row.find("cusip").contents[0])
			except:
				errors.append(row.find("cusip").contents[0])
				ticker = None
			export[name] = {
				"ticker": ticker,
				"class": row.find("titleOfClass").contents[0],
				"CUSIP": row.find("cusip").contents[0],
				"fair_value": row.find("value").contents[0],
				"type_of_position": put_call,
				"no_of_shares": row.find("sshPrnamt").contents[0],
			}
		return export

	def txt_parse_1(res):
		soup = BeautifulSoup(res.text, "html5lib")
		table = str(soup.table.caption.contents[1]).split('\n')
		export = {}
		# next_line will act as a cache for name values which are word wrapped to the next line
		next_line = None
		for row in table:
			name = ""
			# If there is something in our cache, we re-instantiate our name variable to take the cache value
			if next_line is not None:
				name = next_line
				next_line = None
			# regex expression to split the row by whenever there are more than 2 whitespaces
			temp = re.split(r"(\s{2,})", row)

			# the name will always be the first value so we add the name from our row to the name string which at this
			# point is either 0 or it is defined by the next_line cache value
			name = name + str(''.join(temp[0]))
			# if the number of values in the list of items in the row is less than 3, it must be a word-wrap row and
			# therefore we should add the name to our cache
			if len(temp) < 3:
				next_line = name
			export[name] = {
				"class": None,
				"CUSIP": None,
				"fair_value": None,
				"type_of_position": None,
				"no_of_shares": None,
				"5-year_price": None
			}

		return export

	# First step: request the webpage and find the file
	final_data = {}
	global not_done
	for data in urls:
		for my_date in data:
			url = data[my_date]
		response, kind = get_req(url)
		if kind == 'xml':
			final_data[my_date.strftime("%m/%d/%y")] = pd.DataFrame(xml_parse(response)).transpose().to_json()
		else:
			not_done = response

	# what else do I want?
	# Varcovar, correlation matrix, average log returns, 5 year historic returns (no index, series),
	return final_data


if __name__ == '__main__':
	errors = []
	not_done = {}
	x = main()
	print(x)
