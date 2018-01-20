import commonmd as cmd
import time
import os
import ujson


def getFileWriters( file_date, coin_names ):
	ticker_fpath       = '../ticker-data/{0}/'.format( file_date )
	orderbook_fpath    = '../orderbook-data/{0}/'.format( file_date )
	transactions_fpath = '../transactions-data/{0}/'.format( file_date )

	if not os.path.isdir( ticker_fpath ):
		os.mkdir( ticker_fpath )

	if not os.path.isdir( orderbook_fpath ):
		os.mkdir( orderbook_fpath )

	if not os.path.isdir( transactions_fpath ):
		os.mkdir( transactions_fpath )

	ticker_fws       = dict()
	orderbook_fws    = dict()
	transactions_fws = dict()

	for coin_name in coin_names:	
		fname = '{0}.json'.format( coin_name )
		ticker_fws[ coin_name ]       = open( ticker_fpath+fname, 'w', encoding='utf-8' )
		orderbook_fws[ coin_name ]    = open( orderbook_fpath+fname, 'w', encoding='utf-8' )
		transactions_fws[ coin_name ] = open( transactions_fpath+fname, 'w', encoding='utf-8' )

	return ticker_fws, orderbook_fws, transactions_fws
###/getFileWriters


def closeFileWriters( ticker_fws, orderbook_fws, transactions_fws, coin_names ):
	for coin_name in coin_names:
		ticker_fws[ coin_name ].close()
		orderbook_fws[ coin_name ].close()
		transactions_fws[ coin_name ].close()
#/closeFileWriters


def main():
	logger_name = 'crawler'
	log_fpath   = '../logs/'
	log_fname   = 'crawler.log'
	stream_mode = True
	logger = cmd.getLogger( log_fpath, log_fname, logger_name, stream_mode )

	coin_names = [ 'BTC', 'ETH', 'DASH', 'LTC', 'ETC', 'XRP', 'BCH', 
	               'XMR', 'ZEC', 'QTUM', 'BTG', 'EOS' ]

	trans_last_timestamp = dict()
	for coin_name in coin_names:
		trans_last_timestamp[ coin_name ] = -1
	
	file_date, log_date = cmd.getReadableDate( time.time() )
	cmd.printLogger( logger, 'info', log_date, 'Process start' )

	#ticker_fws, orderbook_fws, transactions_fws = getFileWriters( file_date, coin_names )

	is_first_loop = True
	loop_cnt = 0
	while True:
		start_time = time.time()

		loop_cnt += 1
	
		cur_file_date, log_date = cmd.getReadableDate( start_time )
		"""
		if file_date != cur_file_date:
			cmd.printLogger( logger, 'info', log_date, 
			                 'Date updated({0} to {1})'.format( file_date, cur_file_date ) )
			file_date = cur_file_date
			closeFileWriters( ticker_fws, orderbook_fws, transactions_fws, coin_names )
			ticker_fws, orderbook_fws, transactions_fws = getFileWriters( file_date, coin_names )
		"""

		try: 
			"""
			ticker_data    = cmd.getTicker( 'ALL' )
			orderbook_data = cmd.getOrderbook( 'ALL' )
			"""

			for coin_name in coin_names:
				response = cmd.getRecentTransactions( coin_name, 0, 3 )
				status = response.get( 'status', '-1' )

				print( response )
				print( response[ 'data' ][ 0 ].keys() )
				"""
				if 'status' in response.keys():
					status = response
					if response
				"""
				trans_datas = list()
				if 'data' in response.keys() and len( response[ 'data' ] ) > 0 :
					trans_datas = response[ 'data' ]
				else:
					pass

				new_trans_datas = cmd.resetTransDatas( trans_datas )
				for new_trans_data in new_trans_datas:
					print( new_trans_data )

				break

				#transactions_data = ujson.dumps( transactions_data, ensure_ascii=False )
				#transactions_fws[ coin_name ].write( '{0}\n'.format( transactions_data ) )

			"""
			for coin_name in coin_names:
				sub_ticker_data = { 'status': ticker_data.get( 'status', -1 ),
				                    'date': ticker_data.get( 'data', {} ).get( 'date', -1 ),
														'data': ticker_data.get( 'data', {} ).get( coin_name, {} ) }
				sub_ticker_data = ujson.dumps( sub_ticker_data, ensure_ascii=False )
				ticker_fws[ coin_name ].write( '{0}\n'.format( sub_ticker_data ) )

				sub_orderbook_data = { 'status': orderbook_data.get( 'status', -1 ),
				                       'date': orderbook_data.get( 'data', {} ).get( 'timestamp', -1 ),
														   'data': orderbook_data.get( 'data', {} ).get( coin_name, {} ) }
				sub_orderbook_data = ujson.dumps( sub_orderbook_data, ensure_ascii=False )
				orderbook_fws[ coin_name ].write( '{0}\n'.format( sub_orderbook_data ) )
			"""
		except Exception as e:
			cmd.printLogger( logger, 'error', log_date, e )

		end_time = time.time()
		time_gap = max( end_time - start_time, 0 )
		if time_gap < 1:
			time.sleep( 1 - time_gap )

		if loop_cnt % 60 == 0:
			cmd.printLogger( logger, 'info', log_date, 'Normal processing' )
			loop_cnt = 0

		break

	#closeFileWriters( ticker_fws, orderbook_fws, transactions_fws, coin_names )
###/main


if __name__ == '__main__':
	main()
