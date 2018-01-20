import commonmd as cmd
import time
import os
import ujson


def getFileWriters( file_date, coin_names ):
	fws = dict()

	for coin_name in coin_names:
		fpath = '../deal-data/{0}/'.format( coin_name )
		fname = '{0}.json'.format( file_date )

		if not os.path.isdir( fpath ):
			os.mkdir( fpath )

		fws[ coin_name ] = open( fpath+fname, 'w', encoding='utf-8' )

	return fws
###/getFileWriters


def closeFileWriters( fws ):
	for coin_name in fws.keys():
		fws[ coin_name ].close()
#/closeFileWriters


def main():
	logger_name = 'crawler'
	log_fpath   = '../logs/'
	log_fname   = 'crawler.log'
	stream_mode = True
	logger = cmd.getLogger( log_fpath, log_fname, logger_name, stream_mode )

	coin_names = [ 'BTC', 'ETH', 'DASH', 'LTC', 'ETC', 'XRP', 'BCH', 
	               'XMR', 'ZEC', 'QTUM', 'BTG', 'EOS' ]

	prev_trans_datas = dict()
	for coin_name in coin_names:
		prev_trans_datas[ coin_name ] = list()
	
	file_date, log_date = cmd.getReadableDate( time.time() )
	cmd.printLogger( logger, 'info', log_date, 'Process start' )

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
			"""

			order_datas  = cmd.getOrderbook( 'ALL', 1, 20 ) 
			order_status = order_datas.get( 'status', '-1' )
			order_datas  = order_datas.get( 'data', {} )

			for coin_idx, coin_name in enumerate( coin_names ):
				response     = cmd.getRecentTransactions( coin_name, 1, 3 )
				trans_status = response.get( 'status', '-1' )
				print( response )
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

				new_trans_datas, has_date_update, updated_ymd_date = cmd.resetTransDatas( trans_datas )
				for new_trans_data in new_trans_datas:
					print( new_trans_data )
				"""
				cmd.deleteDupDeal( prev_trans_datas[ coin_name ], new_trans_datas )

				cur_order_datas = order_datas.get( coin_name, {} )
				cur_order_datas = { 'bids': cur_order_datas.get( 'bids', [] ),
				                    'asks': cur_order_datas.get( 'asks', [] ) }
				fw_data = { 'trans_status': trans_status, 'trans_datas': new_trans_datas,
				            'order_status': order_status, 'order_datas': cur_order_datas }

				print( fw_data )
				"""

				"""
				if is_first_loop:
					fws = getFileWriters( updated_ymd_date, coin_names )
					is_first_loop = False
				elif has_date_update:
					closeFileWriters( fws )
					fws = getFileWriters( updated_ymd_date, coin_names )
				"""

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

	#closeFileWriters( fws )
###/main

if __name__ == '__main__':
	main()
