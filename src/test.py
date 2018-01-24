import ujson
import time


time_idx     = { '1m': 0, '3m': 1, '5m': 2, '10m': 3 } # 시간 단위 to 인덱스
interval_sec = { '1m': 60, '3m': 180, '5m': 300, '10m': 600 }
trans_idx    = { 'cmpr_time': 0, 'trans_date': 1, 'deal_type': 2, 
                 'price': 3, 'units_traded': 4, 'total': 5 }

# 캔들 차트 인덱스, start_price: 시가, end_price: 종가, min_price: 저가, max_price: 고가
candle_idx   = { 'start_price': 0, 'end_price': 1, 'min_price': 2, 'max_price': 3 }
data_periods = [ 15, 20, 50 ]

volume_idx = { 'bid': 0, 'ask': 1 } # 볼륨 차트 인덱스, bid:매수, ask:매도

def setCandleData( candle_datas, trans_datas, start_time, interval_time ):
	"""
	캔들 데이터 세팅
	candle_datas: 캔들 데이터 변수
	trans_datas: 거래소 트랜잭션(거래) 데이터
	start_time: 데이터 축적 시작 시간
	interval_time: 데이터 분할 시간, 초단위
	"""
	global candle_idx

	time_updated = False

	min_price = int() 
	max_price = int()

	for data_idx, trans_data in enumerate( trans_datas ):
		cur_price = int( trans_data[ 3 ] ) # 현재 거래가격

		if data_idx == 0:
			min_price = cur_price
			max_price = cur_price
		else:
			min_price = min( min_price, cur_price )
			max_price = max( max_price, cur_price )

		cur_time = trans_data[ 0 ]
		if cur_time - start_time > interval_time: # 데이터 분할 시간 초과시
			if len( candle_datas ) >= 54:
				candle_datas = candle_datas[ :53 ]
			candle_datas.append( [ cur_price, cur_price, cur_price, cur_price ] )
			time_updated = True
			start_time += interval_time
		else:
			prev_min_price = candle_datas[ -1 ][ candle_idx[ 'min_price' ] ]
			prev_max_price = candle_datas[ -1 ][ candle_idx[ 'max_price' ] ]
			candle_datas[ -1 ][ candle_idx[ 'min_price' ] ] = min( prev_min_price, min_price ) # 저가 갱신
			candle_datas[ -1 ][ candle_idx[ 'max_price' ] ] = max( prev_max_price, max_price ) # 고가 갱신
			candle_datas[ -1 ][ candle_idx[ 'end_price' ] ] = cur_price # 종가 갱신

	return time_updated			
###/setCandleData


def setVolumeData( volume_datas, trans_datas, start_time, interval_time ):
	"""
	특정 구간(interval_time, sec) 내에서의 매도채결량, 매수채결량
	"""
	global volume_idx 

	time_updated = False

	min_price = int()
	max_price = int()
	for data_idx, trans_data in enumerate( trans_datas ):
		volume_type = trans_data[ 2 ]
		volume      = float( trans_data[ 4 ] )

		cur_time = trans_data[ 0 ]
		if cur_time - start_time > interval_time:
			new_volume_data = [ 0, 0 ]
			new_volume_data[ volume_idx[ volume_type ] ] += volume
			volume_datas.append( new_volume_data.copy() )
			time_updated = True
			start_time += interval_time
		else:
			volume_datas[ -1 ][ volume_idx[ volume_type ] ] += volume

	return time_updated
###/setVolumeData


def getRSI( candle_datas, period ):
	global candle_idx 

	end_price_idx = candle_idx[ 'end_price' ]

	acc_inc_val = 0 # 누적 상승 가격분
	acc_dec_val = 0 # 누적 하락 가격분

	prev_end_price = 0 # 이전 봉 종가

	rsi = float()

	for data_idx, candle_data in enumerate( candle_datas[ -period: ] ):
		cur_end_price = candle_data[ end_price_idx ]

		if data_idx == 0:
			prev_end_price = cur_end_price
			continue

		if cur_end_price > prev_end_price:
			acc_inc_val += cur_end_price - prev_end_price
		else:
			acc_dec_val += prev_end_price - cur_end_price

		prev_end_price = cur_end_price

	if acc_dec_val == 0:
		rsi = -1
	else:	
		rs  = ( acc_inc_val / period ) / ( acc_dec_val / period )
		rsi = 100 - ( 100 / ( 1 + rs ) )

	return rsi
###/getRSI


def getVP( volume_datas, period ):
	global volume_idx 

	acc_bid_volume = 0.0
	acc_ask_volume = 0.0

	for volume_data in volume_datas[ -period: ]:
		acc_bid_volume += volume_data[ volume_idx[ 'bid' ] ]
		acc_ask_volume += volume_data[ volume_idx[ 'ask' ] ]

	vp = ( acc_bid_volume / acc_ask_volume ) * 100

	return vp
###/getVP


def getMRI( candle_datas, period ):
	global candle_idx 
	end_price_idx = candle_idx[ 'end_price' ]

	acc_val = 0

	for data_idx, candle_data in enumerate( candle_datas[ -period: ] ):
		cur_end_price = candle_data[ end_price_idx ]
		acc_val += cur_end_price

	mri = acc_val / period

	return mri
###/getMSI


def getStochastics( candle_datas, period ):
	global candle_idx 
	end_price_idx = candle_idx[ 'end_price' ]
	min_price_idx = candle_idx[ 'min_price' ]
	max_price_idx = candle_idx[ 'max_price' ]
	
	min_prices = [ 0, 0, 0, 0, 0 ] # [ +0일 최저가, +1일 최저가, +2일 최저가, +3일 최저가, +4일 최저가 ]
	max_prices = [ 0, 0, 0, 0, 0 ] # [ +0일 최고가, +1일 최고가, +2일 최고가, +3일 최고가, +4일 최고가 ]
	for data_idx, candle_data in enumerate( candle_datas[ -(period+4): ] ):
		min_price = candle_data[ min_price_idx ]
		max_price = candle_data[ max_price_idx ]
		
		if data_idx <= 4:
			for sub_data_idx in range( data_idx ):
				min_prices[ 4 - sub_data_idx ] = min( min_prices[ 4 - sub_data_idx ], min_price )
				max_prices[ 4 - sub_data_idx ] = max( max_prices[ 4 - sub_data_idx ], max_price )

			min_prices[ 4 - data_idx ] = min_price
			max_prices[ 4 - data_idx ] = max_price
		elif data_idx >= period:
			for sub_data_idx in range( ( period+4 ) - data_idx ):
				min_prices[ sub_data_idx ] = min( min_prices[ sub_data_idx ], min_price )
				max_prices[ sub_data_idx ] = max( max_prices[ sub_data_idx ], max_price )
		else:
			for sub_data_idx in range( len( min_prices ) ):
				min_prices[ sub_data_idx ] = min( min_prices[ sub_data_idx ], min_price )
				max_prices[ sub_data_idx ] = max( max_prices[ sub_data_idx ], max_price )

	# [ +0일 종가, +1일 종가, +2일 종가, +3일 종가, +4일 종가 ]
	end_prices = [ candle_datas[ -1 ][ end_price_idx ], candle_datas[ -2 ][ end_price_idx ],
	               candle_datas[ -3 ][ end_price_idx ], candle_datas[ -4 ][ end_price_idx ],
								 candle_datas[ -5 ][ end_price_idx ] ] 

	fask_k  = ( end_prices[ 0 ] - min_prices[ 0 ] ) / ( max_prices[ 0 ] - min_prices[ 0 ] ) * 100
	slow_k1 = ( ( ( end_prices[ 0 ] - min_prices[ 0 ] ) +
	              ( end_prices[ 1 ] - min_prices[ 1 ] ) +
					 	    ( end_prices[ 2 ] - min_prices[ 2 ] ) ) /
					    ( ( max_prices[ 0 ] - min_prices[ 0 ] ) +
						 	  ( max_prices[ 1 ] - min_prices[ 1 ] ) +
						 	  ( max_prices[ 2 ] - min_prices[ 2 ] ) ) ) * 100
	slow_k2 = ( ( ( end_prices[ 1 ] - min_prices[ 1 ] ) +
	              ( end_prices[ 2 ] - min_prices[ 2 ] ) +
						 	  ( end_prices[ 3 ] - min_prices[ 3 ] ) ) /
						  ( ( max_prices[ 1 ] - min_prices[ 1 ] ) +
						 	  ( max_prices[ 2 ] - min_prices[ 2 ] ) +
						 	  ( max_prices[ 3 ] - min_prices[ 3 ] ) ) ) * 100
	slow_k3 = ( ( ( end_prices[ 2 ] - min_prices[ 2 ] ) +
	              ( end_prices[ 3 ] - min_prices[ 3 ] ) +
						 	  ( end_prices[ 4 ] - min_prices[ 4 ] ) ) /
						  ( ( max_prices[ 2 ] - min_prices[ 2 ] ) +
						 	  ( max_prices[ 3 ] - min_prices[ 3 ] ) +
						 	  ( max_prices[ 4 ] - min_prices[ 4 ] ) ) ) * 100
	slow_d = ( slow_k1 + slow_k2 + slow_k3 ) / 3	

	return fask_k, slow_d
###/getStochastics


def main():
	global time_idx 
	global interval_sec
	global candle_idx
	global volume_idx 
	global trans_idx
	global data_periods

	data_fpath = '../deal-data/XRP/'
	data_fname = '180121.json'

	start_time   = dict() 
	candle_datas = dict()
	volume_datas = dict()
	for time_base in time_idx.keys():
		candle_datas[ time_base ] = list()
		start_time[ time_base ] = 0
		volume_datas[ time_base ] = [ [ 0, 0 ] ]

	has_meet_first_trans_data = False

	tmp_time = time.time()
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
				init_price = int( trans_datas[ 0 ][ trans_idx[ 'price' ] ] )
				for time_base in start_time.keys():
					start_time[ time_base ] = trans_datas[ 0 ][ trans_idx[ 'cmpr_time' ] ]
					candle_datas[ time_base ].append( [ init_price, init_price, init_price, init_price ] )
				has_meet_first_trans_data = True
			
			if len( trans_datas ) <= 0:
				empty_trans_datas_cnt += 1
				continue
			else:
				empty_trans_datas_cnt = 0

			for time_base in start_time.keys():
				interval = interval_sec[ time_base ]
				time_updated = setCandleData( candle_datas[ time_base ], trans_datas, 
				                              start_time[ time_base ], interval )
				time_updated = setVolumeData( volume_datas[ time_base ], trans_datas, 
				                              start_time[ time_base ], interval )
				
				if time_updated:
					start_time[ time_base ] += interval


			### 데이터 축적 형태로 변경 필요
			rsis = dict()
			mris = dict()
			vps  = dict()
			stks  = dict() # 스토캐스틱 fask k 데이터
			stds  = dict() # 스토캐스틱 slow d 데이터
			for time_base in start_time.keys():
				sub_rsis = dict()
				sub_mris = dict()
				sub_vps  = dict()
				sub_stks = dict()
				sub_stds = dict()
				for data_period in data_periods:
					sub_rsis[ data_period ] = 0
					sub_mris[ data_period ] = 0
					sub_vps[ data_period ]  = 0
					sub_stks[ data_period ] = 0
					sub_stds[ data_period ] = 0

				rsis[ time_base ] = sub_rsis
				mris[ time_base ] = sub_mris
				vps[ time_base ]  = sub_vps
				stks[ time_base ] = sub_stks
				stds[ time_base ] = sub_stds

			if len( candle_datas[ '10m' ] ) >= 54:	# 처음 시작 데이터 축적 완료 시점
				for time_base in start_time.keys():
					sub_candle_datas = candle_datas[ time_base ]
					sub_volume_datas = volume_datas[ time_base ]
					for data_period in data_periods:
						rsis[ time_base ][ data_period ] = getRSI( sub_candle_datas, data_period )	
						mris[ time_base ][ data_period ] = getMRI( sub_candle_datas, data_period )	
						vps[ time_base ][ data_period ]  = getVP( sub_volume_datas, data_period )	
						fask_k, slow_d  = getStochastics( sub_candle_datas, data_period )
						stks[ time_base ][ data_period ] = fask_k
						stds[ time_base ][ data_period ] = slow_d

				print( '###########################################################' )
				print( trans_datas[ -1 ][ 3 ] )
				"""
				for data_period in data_periods:
					print( 'rsi{0}: {1}'.format( data_period, rsis[ '10m' ][ data_period ] ) )
				print()
				for data_period in data_periods:
					print( 'mri{0}: {1}'.format( data_period, mris[ '10m' ][ data_period ] ) )
				print()
				for data_period in data_periods:
					print( 'vp{0}:  {1}'.format( data_period, vps[ '10m' ][ data_period ] ) )
				print()
				"""
				for data_period in data_periods:
					print( 'stk{0}: {1}'.format( data_period, stks[ '10m' ][ data_period ] ) )
				print()
				for data_period in data_periods:
					print( 'std{0}: {1}'.format( data_period, stds[ '10m' ][ data_period ] ) )
				print()
				time.sleep( 0.5 )
				

			"""
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
