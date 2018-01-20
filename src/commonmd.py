import requests
import time
import datetime
import logging
import logging.handlers


def getLogger( log_fpath, log_fname, logger_name, stream_mode = False ):
	logger = logging.getLogger( logger_name )


	file_max_bytes = 1024 * 1024
	file_handler = logging.handlers.RotatingFileHandler( log_fpath + log_fname, 
	                                                     maxBytes = file_max_bytes,
																											 backupCount=2 )

	formatter = logging.Formatter( '[%(levelname)s] - %(message)s' )
	file_handler.setFormatter( formatter )
	logger.addHandler( file_handler )

	if stream_mode:
		stream_handler = logging.StreamHandler()
		stream_handler.setFormatter( formatter )
		logger.addHandler( stream_handler )


	logger.setLevel( logging.DEBUG )

	return logger
###/getLogger


def printLogger( logger, levelname, log_date, message ):
	if levelname == 'info':
		new_message = '{0} > {1}'.format( log_date, message )
		logger.info( new_message )
	elif levelname == 'warn':
		new_message = '{0} > {1}'.format( log_date, message )
		logger.warn( new_message )
	elif levelname == 'error':
		new_message = '{0} > {1}'.format( log_date, message )
		logger.error( new_message )
	else:
		new_message = '{0} > {1}'.format( log_date, message )
		logger.debug( new_message )
###/printLogger


def getTicker(currency):
	resource_url = 'https://api.bithumb.com/public/ticker/'
	resource_uri = resource_url + currency

	response = requests.get(resource_uri).json()

	return response
###/getTicker


def getOrderbook(currency):
	resource_url = 'https://api.bithumb.com/public/orderbook/'
	resource_uri = resource_url + currency
	
	response = requests.get(resource_uri).json()
	
	return response
###/getOrderbook


def getRecentTransactions(currency, offset, count):
	resource_url = 'https://api.bithumb.com/public/recent_transactions/'
	resource_uri = resource_url + currency + '?offset={0}&count={1}'.format( offset, count )
	
	response = requests.get(resource_uri).json()
	
	return response
###/getRecentTransactions


def getReadableDate( unixtime ):
	unixtime += 32400 # 9시간 추가, 한국 시간으로 변경

	file_date = datetime.datetime.fromtimestamp( int( unixtime ) ).strftime( '%Y%m%d' )[ 2: ]
	log_date = datetime.datetime.fromtimestamp( int( unixtime ) ).strftime( '%Y%m%d %H:%M:%S' )[ 2: ]

	return file_date, log_date
###/getRecentDate
