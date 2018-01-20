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
	stream_mode = False 
	logger = cmd.getLogger( log_fpath, log_fname, logger_name, stream_mode )

	logger.info( 'Process start' )

	coin_names = [ 'BTC', 'ETH', 'DASH', 'LTC', 'ETC', 'XRP', 'BCH', 
	               'XMR', 'ZEC', 'QTUM', 'BTG', 'EOS' ]

	prev_trans_datas = dict()
	for coin_name in coin_names:
		prev_trans_datas[ coin_name ] = list()

	prev_hms_date = -1
	
	is_first_loop = True
	loop_cnt = 0

	while True:
		start_time = time.time()

		loop_cnt += 1
	
		try: 
			sub_stime = time.time()

			order_datas  = cmd.getOrderbook( 'ALL', 1, 5 ) 
			order_status = order_datas.get( 'status', '-1' )
			order_datas  = order_datas.get( 'data', {} )

			sub_etime = time.time()
			time_gap = max( sub_etime - sub_stime, 0 )
			if time_gap < 1:
				time.sleep( 1 - time_gap )

			for coin_idx, coin_name in enumerate( coin_names ):
				sub_stime = time.time()

				response     = cmd.getRecentTransactions( coin_name, 1, 30 )
				trans_status = response.get( 'status', '-1' )
				trans_datas  = response.get( 'data', [] )

				if trans_status != '0000':
					logger.warn( 'Invalid Transactions status: {0}'.format( trans_status ) )	

				new_trans_datas, has_date_update, updated_ymd_date = cmd.resetTransDatas( trans_datas, prev_hms_date )
				if len( new_trans_datas ) > 0:
					prev_hms_date = new_trans_datas[ -1 ][ 0 ]

				cur_order_datas = order_datas.get( coin_name, {} )
				cur_order_datas = { 'bids': cur_order_datas.get( 'bids', [] ),
				                    'asks': cur_order_datas.get( 'asks', [] ) }
				fw_data = { 'trans_status': trans_status, 'trans_datas': new_trans_datas,
				            'order_status': order_status, 'order_datas': cur_order_datas }

				if is_first_loop:
					fws = getFileWriters( updated_ymd_date, coin_names )
					is_first_loop = False
				elif has_date_update:
					closeFileWriters( fws )
					fws = getFileWriters( updated_ymd_date, coin_names )

				fw_data = ujson.dumps( fw_data, ensure_ascii=False )
				fws[ coin_name ].write( '{0}\n'.format( fw_data ) )

				sub_etime = time.time()
				time_gap = max( sub_etime - sub_stime, 0 )
				if time_gap < 0.2:
					time.sleep( 0.2 - time_gap )
		except Exception as e:
			logger.error( e )

		end_time = time.time()
		time_gap = max( end_time - start_time, 0 )
		if time_gap < 5:
			time.sleep( 5 - time_gap )

		#if loop_cnt % 150 == 0:
		if loop_cnt % 120 == 0:
			logger.info( 'Normal processing' )
			loop_cnt = 0
	
	closeFileWriters( fws )
###/main

if __name__ == '__main__':
	main()
