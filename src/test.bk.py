import ujson
import time

# 캔들 차트 인덱스
start_price_idx = 0 # 시가 인덱스
end_price_idx   = 1 # 종가 인덱스
min_price_idx   = 2 # 저가 인덱스
max_price_idx   = 3 # 고가 인덱스

volume_data_type2idx = { 'bid': 0, 'ask': 1 } # 볼륨 차트 인덱스, bid:매수, ask:매도

def setCandleData( candle_datas, trans_datas, start_time, interval_time ):
	"""
	특정 구간 내 trans datas 내에서 시가, 
	"""
	global start_price_idx
	global end_price_idx 
	global min_price_idx
	global max_price_idx
	time_updated = False

	min_price = int()
	max_price = int()
	for data_idx, trans_data in enumerate( trans_datas ):
		cur_price = int( trans_data[ 3 ] )
		if data_idx == 0:
			min_price = cur_price
			max_price = cur_price
		else:
			min_price = min( min_price, cur_price )
			max_price = max( max_price, cur_price )

		cur_time = trans_data[ 0 ]
		if cur_time - start_time > interval_time:
			candle_datas.append( [ cur_price, cur_price, cur_price, cur_price ] )
			time_updated = True
			start_time += interval_time
		else:
			candle_datas[ -1 ][ min_price_idx ] = min( candle_datas[ -1 ][ min_price_idx ], min_price ) # 저가 갱신
			candle_datas[ -1 ][ max_price_idx ] = max( candle_datas[ -1 ][ max_price_idx ], max_price ) # 고가 갱신
			candle_datas[ -1 ][ end_price_idx ] = cur_price # 종가 갱신

	return time_updated			
###/setCandleData


def setVolumeData( volume_datas, trans_datas, start_time, interval_time ):
	"""
	특정 구간(interval_time, sec) 내에서의 매도채결량, 매수채결량
	"""
	global volume_data_type2idx 

	time_updated = False

	min_price = int()
	max_price = int()
	for data_idx, trans_data in enumerate( trans_datas ):
		volume_type = trans_data[ 2 ]
		volume      = float( trans_data[ 4 ] )

		cur_time = trans_data[ 0 ]
		if cur_time - start_time > interval_time:
			new_volume_data = [ 0, 0 ]
			new_volume_data[ volume_data_type2idx[ volume_type ] ] += volume
			volume_datas.append( new_volume_data.copy() )
			time_updated = True
			start_time += interval_time
		else:
			volume_datas[ -1 ][ volume_data_type2idx[ volume_type ] ] += volume

	return time_updated
###/setVolumeData


def getRSI( candle_datas, period ):
	global end_price_idx 

	acc_inc_val = 0
	acc_dec_val = 0

	prev_end_price = 0
	for data_idx, candle_data in enumerate( candle_datas[ -period: ] ):
		cur_end_price = candle_data[ end_price_idx ]
		if data_idx == 0:
			prev_end_price = cur_end_price
			continue

		if cur_end_price > prev_end_price:
			acc_inc_val = cur_end_price - prev_end_price
		else:
			acc_dec_val = prev_end_price - cur_end_price

	if acc_dec_val == 0:
		rsi = -1
	else:	
		rs  = ( acc_inc_val / period ) / ( acc_dec_val / period )
		rsi = 100 - ( 100 / ( 1 + rs ) )

	return rsi
###/getRSI


def getVP( volume_datas, period ):
	global volume_data_type2idx 

	acc_bid_volume = 0.0
	acc_ask_volume = 0.0

	for volume_data in volume_datas[ -period: ]:
		acc_bid_volume += volume_data[ volume_data_type2idx[ 'bid' ] ]
		acc_ask_volume += volume_data[ volume_data_type2idx[ 'ask' ] ]

	vp = ( acc_bid_volume / acc_ask_volume ) * 100

	return vp
###/getVP


def getMRI( candle_datas, period ):
	global end_price_idx 

	acc_val = 0

	for data_idx, candle_data in enumerate( candle_datas[ -period: ] ):
		cur_end_price = candle_data[ end_price_idx ]
		acc_val += cur_end_price

	mri = acc_val / period

	return mri
###/getMSI


def main():
	global start_price_idx
	global end_price_idx 
	global min_price_idx
	global max_price_idx
	global volume_data_type2idx 

	data_fpath = '../deal-data/XRP/'
	data_fname = '180121.json'

	candle_datas = list()
	volume_datas = [ [ 0, 0 ] ]

	rs_inc_5m = list() # 5분봉 rs 상승폭 리스트
	rs_dec_5m = list() # 5분봉 rs 하락폭 리스트
	has_meet_first_trans_data = False
	start_time_5m = 0

	invalid_trans_status_cnt = 0
	empty_trans_datas_cnt    = 0 # empty trans datas가 연속으로 일어나는 횟수, 서버가 죽은거 체크
	with open( data_fpath+data_fname, 'r', encoding='utf-8' ) as fr:
		for line_idx, line in enumerate( fr ):
			if line_idx == 0:
				continue
			line = ujson.loads( line )

			trans_status = line[ 'trans_status' ]
			trans_datas  = line[ 'trans_datas' ]

			if trans_status != '0000':
				invalid_trans_status_cnt += 1
				continue

			if not has_meet_first_trans_data and len( trans_datas ) <= 0:
				continue
			elif not has_meet_first_trans_data and len( trans_datas ) > 0:
				start_time_5m = trans_datas[ 0 ][ 0 ]
				init_price = int( trans_datas[ 0 ][ 3 ] )
				candle_datas.append( [ init_price, init_price, init_price, init_price ] )

				has_meet_first_trans_data = True
			
			if len( trans_datas ) <= 0:
				empty_trans_datas_cnt += 1
				continue
			else:
				empty_trans_datas_cnt = 0

			time_updated = setCandleData( candle_datas, trans_datas, start_time_5m, 300 )
			time_updated = setVolumeData( volume_datas, trans_datas, start_time_5m, 300 )

			if time_updated:
				start_time_5m += 300

			if len( candle_datas ) >= 50:
				rsi = getRSI( candle_datas, 50 )
				mri50 = getMRI( candle_datas, 50 )
				mri20 = getMRI( candle_datas, 20 )
				mri15 = getMRI( candle_datas, 15 )
				vp50  = getVP( volume_datas, 50 )
				vp20  = getVP( volume_datas, 20 )
				vp15  = getVP( volume_datas, 15 )

				print( trans_datas[ 0 ][ 3 ] )
				print( 'vp50 = {0}'.format( vp50 ) )
				print( 'vp20 = {0}'.format( vp20 ) )
				print( 'vp15 = {0}'.format( vp15 ) )
				print( 'mri50 = {0}'.format( mri50 ) )
				print( 'mri20 = {0}'.format( mri20 ) )
				print( 'mri15 = {0}'.format( mri15 ) )
				print( 'rsi = {0}'.format( rsi ) )
				print()
				time.sleep( 1.0 )


				"""
				if mri15 > mri20 and mri20 > mri50:
					print( '정배열' )
					print( trans_datas[ 0 ] )
					print( ( candle_datas[ -1 ][ start_price_idx ] + candle_datas[ -1 ][ end_price_idx ] ) / 2 )
					print( rsi )
					print( mri50 )
					print( mri20 )
					print( mri15 )
					print()
					#time.sleep( 0.1 )
				elif mri50 > mri20 and mri20 > mri15:
					print( '역배열' )
					print( ( candle_datas[ -1 ][ start_price_idx ] + candle_datas[ -1 ][ end_price_idx ] ) / 2 )
					print( rsi )
					print( mri50 )
					print( mri20 )
					print( mri15 )
					print()
					time.sleep( 0.1 )
				"""
###/main


if __name__ == '__main__':
	main()
