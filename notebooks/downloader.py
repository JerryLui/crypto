## File updaters for coinmarketcap and blockchain.info
import pandas as pd
import requests
import re, sys, os
import numpy as np
from bs4 import BeautifulSoup	# To scrap web data
import concurrent.futures		# For concurrent downloads

## TODO: Add progress bar for downloads.
## Notice: In this file I have used different methods to merge old data with new to try out different solutions

### COINMARKETCAP ###
def update_market_cap(file = 'data/total_market_cap.csv', base_url = 'https://coinmarketcap.com/historical/'):
	'''
	Downloads/updates market cap data from coinmarketcap's historical snapshots (which are taken at a 7 day interval) 
	
	Parameters
	----------
	file : str
		Filename and path to file
	base_url : str
		Url to page with all historical snapshots
		
	'''
	print('Updating market cap...')
	## Read existing data, create new if none found
	try:
		df = pd.read_csv(file, parse_dates=True, index_col='Date')
		latest_date = df.index[-1]

		# Check if latest registered data is up to date
		if latest_date + pd.offsets.Week() >= pd.to_datetime('today'):
			print('Data is already up to date!') 
			return # Interrupt if nothing is running
		else:
			print('Latest data point at: ' + latest_date.strftime('%d-%m-%Y'))
			
	except FileNotFoundError:
		print('File Not Found!\nWriting to ' + file + '...')
		# Create empty dataframe
		df = pd.DataFrame([], columns=['Date', 'Total Market Cap'])
		df = df.set_index('Date')
		# Set first data point at 20130421
		latest_date = pd.to_datetime('20130421')

	## Create date range for historical snapshots from latest date to today-1 day since data uploads after day
	Date = pd.date_range(start=latest_date+pd.offsets.Week(),
						end=pd.to_datetime('today')-pd.offsets.Day(), freq='7D').strftime('%Y%m%d')

	def get_market_cap(date):
		''' Retrieve historical snapshot data from given date, returns marketcap as int. '''
		page = requests.get(base_url + date)
		soup = BeautifulSoup(page.content, 'html.parser')
		body = soup.find('body')
		container = body.find('div', {'class':'container'}, recursive=False)
		try:		# Handles if no page hasn't been updated yet
			mcap = container.find('span', {'id' : 'total-marketcap'}).text.strip()
		except AttributeError:
			print('Error: Data not available yet!\n')
			raise
			
		# Extract marketcap value from span
		return int(re.sub(r',|\$', '', mcap))

	## Retrieve market cap value in dollars
	
	print('Parsing data from {} to {}'.format(Date[0], Date[-1]))
	print('-'*40)
	with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor: # 2 Threads seems to be optimal in jupyter
		futures = [executor.submit(get_market_cap, date) for date in Date]
		market_cap = [future.result() for future in futures]

	## Create data frame of date
	market_cap_df = pd.DataFrame({'Date':Date, 'Total Market Cap':market_cap})
	market_cap_df.Date = pd.to_datetime(market_cap_df.Date)
	market_cap_df = market_cap_df.set_index('Date')

	## Write to file
	df.append(market_cap_df).to_csv(file)
	print('\nTotal Market Cap data has been successfully updated to ' +
		market_cap_df.index[-1].strftime('%d-%m-%Y') + '!\n')

def update_coins(folder = 'price/', tail = '.csv'):
	'''
	Updates and writes top 9 coins to folder 
	
	Parameters
	----------
	folder : str
		Folder path to read and write files from, default = 'price/'
	tail : str
		File extension, default = '.csv'
		
	TODO:
	----------
	Rearrange for coin as input argument. Current arrangement not extendable.
	'''
	
			
	def get_coin_dict(nmbr_of_coins = 9):
		'''
		Get top 9 (or whatever specified by nmbr_of_coins) coins from coinmarketcap homepage.
		
		Parameters
		----------
		nmbr_of_coins : int
			Number of coins to get
		
		Returns
		----------
		dict
			Dictionary with paired coin {ticker:name} sorted by ticker.
		'''
		
		# Get main ranking table
		page = requests.get('https://coinmarketcap.com/')
		soup = BeautifulSoup(page.content, 'html.parser')
		body = soup.find('body')
		container = body.find('div', {'class':'container'}, recursive=False)
		table = container.find('table', {'id':'currencies'})
		
		# Table body
		tbody = table.find('tbody')
		rows = tbody.find_all('tr')[:nmbr_of_coins]

		# Get the coin name and ticker from each row in table
		coins = []
		coin_names = []
		for row in rows:
			a = row.find('span', {'class':'currency-symbol'}).find('a')
			coins.append(a.get_text())                  # Get coin ticker
			coin_names.append(a['href'].split('/')[-2]) # Get coin name

		# Return dictionary sorted by coin ticker
		return dict([(coin, coin_name) for coin, coin_name in sorted(zip(coins, coin_names))])
	
	# Check for files in given folder, creates folder if it doesn't exist
	try:
		filenames = os.listdir(folder)
		folder_not_found = False
	except:
		print('Folder not found, creating \'' + folder + '\'')
		os.makedirs(folder)
		folder_not_found = True

	# Get coin name and ticker from files/web if file not found
	if folder_not_found or len(filenames) < 1:
		coin_dict = get_coin_dict()      # Scrap top 9 coins from coinmarketcap
		coins = list(coin_dict.keys())
		coin_names = list(coin_dict.values())
	else:
		coins = list(map(lambda x: re.sub(tail, '', x).upper(), filenames)) # Get existing coin names
		
		# Manual method
		#coin_names = ['cardano', 'bitcoin-cash', 'bitcoin', 'dash', 'ethereum', 'iota', 'litecoin', 'nem', 'monero', 'ripple']
		#coin_dict = dict(zip(coins, coin_names))
		
		# Programmer method
		coin_dict = get_coin_dict(nmbr_of_coins = 20)
		coin_dict['IOTA'] = coin_dict.pop('MIOTA')
		coin_names = [coin_dict[coin] for coin in coins]
		
	# Data constants
	header = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market Cap']
	base_url = 'https://coinmarketcap.com/currencies/'
	tail_url = '/historical-data/'

	print('\nUpdating coins...')
	def download_data(coin, folder='price/', tail='.csv'):
		'''
		Retrieves coin historical data upto today of the given coin from coinmarketcap and writes it to file.
		
		Parameters
		----------
		coin : str
			Coin ticker, ex. 'BTC, XRP, ETH...'
			
		Returns
		----------
		str  
			File written response.
		'''
		
		
		# Load stored data, if none found create new
		file = folder + coin.lower() + tail
		try:
			original_df = pd.read_csv(file, delimiter='\t', index_col='Date', parse_dates=True, 
								dtype={'Open':str, 'High':str, 'Low':str, 'Close':str})
			file_not_found = False
			latest_date = original_df.index[0]
			
			# Check if data is up to date
			if latest_date + pd.offsets.Day() >= pd.to_datetime('today'):
				return (coin + ' data already up to date!')
				
		except FileNotFoundError:
			file_not_found = True
		
		# Get html data
		url = base_url + coin_dict[coin] + tail_url
		if not file_not_found: # Only request data from date before last date
			url += (r'?start=' + (latest_date + pd.offsets.Day()).strftime('%Y%m%d') + 
					r'&end=' + pd.to_datetime('today').strftime('%Y%m%d'))
		page = requests.get(url)
		soup = BeautifulSoup(page.content, 'html.parser')

		# Extract table data from html
		table = soup.find('div', {'class':'table-responsive'})
		table_body = table.find('tbody')
		rows = table_body.find_all('tr')

		data = []
		for row in rows:
			cols = row.find_all('td')
			cols = [e.text.strip() for e in cols]
			data.append(cols)
		
		# Convert parsed data into data frame
		parsed_df = pd.DataFrame(data, columns=header)
		parsed_df.Date = pd.to_datetime(parsed_df.Date)
		parsed_df = parsed_df.set_index('Date')
		
		# If no original file
		if file_not_found:
			parsed_df.to_csv(file, sep='\t')
			return (coin + ' data from ' + parsed_df.index[0].strftime('%d-%B-%Y') + 
						' to ' + parsed_df.index[-1].strftime('%d-%B-%Y') + 
						' has been successfully written to ' + file)
		# Concat new and original dataframe and write to file
		else:
			pd.concat((parsed_df, original_df)).to_csv(file, sep='\t')
			return (coin + ' data from ' + latest_date.strftime('%d-%B-%Y') + 
						' has been successfully updated to ' + parsed_df.index[0].strftime('%d-%B-%Y') + 
						' and written to ' + file)
		
	# Download all coin data in coins concurrently
	with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
		futures = [executor.submit(download_data, coin) for coin in coins]
		[print(future.result()) for future in concurrent.futures.as_completed(futures)]

	print('All downloads finished!\n')
	
## BLOCKCHIAN.INFO ###

		
def update_blockchain_info():
	'''
	Updates files from blockchain.info
	
	TODO: Make more general with filenames and urls.
	'''
	
	def update_file(filename, url, folder='data/'):
		'''
		Updates given file with new data from blockchain.info.
		
		TODO: 
			- Allow for different timespans depending on existing data.
			- Parse filename & url as dict/tuple to ensure pairing?
		'''
	
		# Load current existing data
		df = pd.read_csv(folder + filename, names=['Date', 'Data'], index_col='Date', parse_dates=True)
		if pd.to_datetime('today') - df.iloc[-1].name < pd.Timedelta(1, unit='D'):
			print(filename + ' already up to date!')
			return
			
		# Load data from url
		url_df = pd.read_csv(url, names=['Date', 'Data'], index_col='Date', parse_dates=True)
		url_df = url_df.resample('D').mean()	# Resample data into daily averages
		url_df = url_df.combine_first(df)		# Merge the two dataframes overwriting exisitng df values
		url_df.to_csv(folder + filename, header=False)	# Write to file
		print(filename + ' has been successfully updated!')
		return
	
	filename = ['wallet_users.csv',
			'hash_rate_raw.csv']
	url = ['https://blockchain.info/charts/my-wallet-n-users?timespan=1year&format=csv',
	'https://api.blockchain.info/charts/hash-rate?timespan=1year&format=csv']

	# Update each file
	for f, u in zip(filename, url):
		update_file(f, u)