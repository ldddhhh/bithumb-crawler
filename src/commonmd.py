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


def getOrderbook( currency, group_orders=1, count=20 ):
	resource_url = 'https://api.bithumb.com/public/orderbook/'
	resource_uri = resource_url + currency + '?group_orders={0}&count={1}'.format( group_orders, count )
	
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


def resetTransDatas( trans_datas ):
	new_trans_datas  = list() # 시간 순으로 정렬된 transactions data
	has_date_update  = False  # 일자가 넘어가는지 유무
	prev_ymd_date    = -1  # 일자가 넘어가는지 비교하기 위해 전 루프에서의 ymd 날짜 정보
	updated_ymd_date = -1  # 일자가 넘어 간 경우 새로 변경된 일자

	for loop_idx, trans_data in enumerate( trans_datas ):
		req_type     = trans_data[ 'type' ]
		price        = trans_data[ 'price' ]
		units_traded = trans_data[ 'units_traded' ]
		total        = trans_data[ 'total' ]
		trans_date   = trans_data[ 'transaction_date' ]

		cur_ymd_date, compare_date = getSplitedDate( trans_date )

		new_trans_data = [ compare_date, trans_date, req_type, price, units_traded, total ]
		new_trans_datas.append( new_trans_data )

		if loop_idx == 0:
			prev_ymd_date = cur_ymd_date
			updated_ymd_date = cur_ymd_date
		elif prev_ymd_date != cur_ymd_date:
			has_date_update = True
			updated_ymd_date = max( prev_ymd_date, cur_ymd_date )
	
	new_trans_datas = sorted( new_trans_datas, key=lambda k: k[ 0 ] )

	return new_trans_datas, has_date_update, updated_ymd_date
###/resetTransDatas


def getSplitedDate( trans_date ):
	splt_trans_date = trans_date.strip().split( ' ' )

	ymd_date = splt_trans_date[ 0 ].strip().split( '-' )
	y        = ymd_date[ 0 ][ 2: ]
	m        = ymd_date[ 1 ]
	d        = ymd_date[ 2 ]
	ymd_date = int( '{0}{1}{2}'.format( y, m, d ) )

	hms_date = trans_date.strip().split( ' ' )[ 1 ].strip().split( ':' )
	h        = int( hms_date[ 0 ] )
	m        = int( hms_date[ 1 ] )
	s        = int( hms_date[ 2 ] )

	compare_date = ( h * 3600 ) + ( m * 60 ) + s

	return ymd_date, compare_date
###/getSplitedDate
