import re

import requests
from bs4 import BeautifulSoup

base_url = 'https://finance.yahoo.com/lookup?s='
ex_dest = '/output/'
ex = []


def meat(CUSIP):
	formdata = {"tickersymbol": CUSIP, "sopt": "cusip"}
	page = requests.post('https://www.quantumonline.com/search.cfm', data=formdata)
	soup = BeautifulSoup(page.text, 'html5lib')
	text = str(soup.get_text)
	old_span = re.compile("Ticker Symbol:").search(text).span()
	output = text[int(old_span[0]): int(old_span[1] + 8)]
	ticker = output.split()[-1]
	return ticker


def main(name):
	return meat(name)


if __name__ == '__main__':
	print(meat('90214J101'))
