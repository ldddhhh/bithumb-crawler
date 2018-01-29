import ujson
import time
import sys

time_idx     = { '1m': 0, '3m': 1, '5m': 2, '10m': 3 } # 시간 단위 to 인덱스
trans_idx    = { 'sec_time': 0, 'trans_date': 1, 'deal_type': 2,
                 'price': 3, 'units_traded': 4, 'total': 5 } # 트랜스 데이터 인덱스
interval_sec = { '1m': 60, '3m': 180, '5m': 300, '10m': 600 } # 분 단위 별 초

# 캔들 차트 인덱스, start_price: 시가, end_price: 종가, min_price: 저가, max_price: 종가, timestamp: 시가 시간
candle_idx = { 'start_price': 0, 'end_price': 1, 'min_price': 2, 'max_price': 3, 
               'deal_cnt': 4, 'timestamp': 5 }
volume_idx   = { 'bid': 0, 'ask': 1 } # 볼륨 차트 인덱스, bid:매수, ask:매도

max_candles_len = 100 # 캔들 데이터 최대 저장 개수
max_volumes_len = 100 # 볼륨 데이터 최대 저장 개수

def getCandleData( candle_datas, trans_datas, start_time, interval_time, max_len=100 ):
	"""
	캔들 데이터 세팅
	candle_datas: 캔들 데이터 변수
	trans_datas: 거래소 트랜잭션(거래) 데이터
	start_time: 데이터 축적 시작 시간
	interval_time: 데이터 분할 시간, 초단위
	"""
	global candle_idx
	global trans_idx

	time_updated = False

	min_price = int()
	max_price = int()

	for data_idx, trans_data in enumerate( trans_datas ):
		prev_min_price = candle_datas[ -1 ][ candle_idx[ 'min_price' ] ]
		prev_max_price = candle_datas[ -1 ][ candle_idx[ 'max_price' ] ]

		cur_price     = int( trans_data[ trans_idx[ 'price' ] ] ) # 현재 트랜스 데이터 평단가
		cur_timestamp = trans_data[ trans_idx[ 'trans_date' ] ]   # 현재 트랜스 시간

		### 저가, 고가 업데이트
		cur_min_price = min( prev_min_price, cur_price )
		cur_max_price = max( prev_max_price, cur_price )

		cur_time = trans_data[ trans_idx[ 'sec_time' ] ] # 현재 초 시간

		if cur_time >= start_time + interval_time: # 데이터 분할 시간 초과시
			if len( candle_datas ) >= max_len: 
				candle_datas = candle_datas[ 1: ]

			candle_datas.append( [ cur_price, cur_price, cur_price, cur_price, 1, cur_timestamp ] )
			time_updated = True
			start_time += interval_time
		else:
			candle_datas[ -1 ][ candle_idx[ 'min_price' ] ] = cur_min_price # 저가 갱신
			candle_datas[ -1 ][ candle_idx[ 'max_price' ] ] = cur_max_price # 고가 갱신
			candle_datas[ -1 ][ candle_idx[ 'end_price' ] ] = cur_price     # 종가 갱신
			candle_datas[ -1 ][ candle_idx[ 'deal_cnt' ] ] += 1             # 분봉 내 거래 건수 1추가

	return candle_datas, time_updated, start_time
###/getCandleData


def getVolumeData( volume_datas, trans_datas, start_time, interval_time, max_len ):
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
			if len( volume_datas ) >= max_len:
				volume_datas = volume_datas[ 1: ]

			new_volume_data = [ 0, 0 ]
			new_volume_data[ volume_idx[ volume_type ] ] += volume
			volume_datas.append( new_volume_data.copy() )
			time_updated = True
			start_time += interval_time
		else:
			volume_datas[ -1 ][ volume_idx[ volume_type ] ] += volume

	return volume_datas, time_updated, start_time
###/getVolumeData


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

	fast_k = float()
	try:
		fast_k  = ( ( end_prices[ 0 ] - min_prices[ 0 ] ) / ( max_prices[ 0 ] - min_prices[ 0 ] ) ) * 100
	except ZeroDivisionError as zde:
		fast_k = -1000

	slow_k = [ 0, 0, 0 ]
	for i in range( 3 ):
		for j in range( i, i+3 ):
			try:
				slow_k[ i ] += ( ( end_prices[ j ] - min_prices[ j ] ) / ( max_prices[ j ] - min_prices[ j ] ) ) * 100
			except ZeroDivisionError as zde:
				slow_k[ i ] = -1000
		slow_k[ i ] /= 3

	slow_d = sum( slow_k ) / 3

	return fast_k, slow_k[ 0 ], slow_d
###/getStochastics


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



def buyOrNot( fast_ks, slow_ks, slow_ds, rsis ):
	'''
	현재 살지 지켜볼지 결정
	'''
	buy_or_not = False

	stoch_using_time_base = '1m' # 스토캐스틱 지표에서 사용할 분봉 단위
	stoch_using_period    = 10   # 스토캐스틱 지표에서 사용할 기간 단위
	using_fast_ks = fast_ks[ stoch_using_time_base ][ stoch_using_period ][ :-1 ]
	using_slow_ks = slow_ks[ stoch_using_time_base ][ stoch_using_period ][ :-1 ]
	using_slow_ds = slow_ds[ stoch_using_time_base ][ stoch_using_period ][ :-1 ]

	"""
	if len( using_slow_ks ) > 4 and len( using_slow_ds ) > 4:
		# 포맷: [ 전 단계 slow_k 증가치, 현재 단계 slow_k 증가치 ]
		slow_k_inc_val = [ using_slow_ks[ -2 ] - using_slow_ks[ -3 ], using_slow_ks[ -1 ] - using_slow_ks[ -2 ] ]

		if using_slow_ks[ -1 ] < 30:
			#if slow_k_inc_val[ 1 ] > 2: # fast_K_inc_val[ 1 ] > 매직넘버
			if slow_k_inc_val[ 0 ] <= 0 and slow_k_inc_val[ 1 ] > 0: # fast_K_inc_val[ 1 ] > 매직넘버
				if using_slow_ks[ -3 ] < using_slow_ds[ -3 ] and using_slow_ks[ -2 ] < using_slow_ds[ -2 ] and using_slow_ks[ -1 ] > using_slow_ds[ -1 ]:
					buy_or_not = True
	"""
	
	if len( using_fast_ks ) > 3 and len( using_slow_ds ) > 3:
		# 포맷: [ 전 단계 fast_k 증가치, 현재 단계 fast_k 증가치 ]
		fast_k_inc_val = [ using_fast_ks[ -2 ] - using_fast_ks[ -3 ], using_fast_ks[ -1 ] - using_fast_ks[ -2 ] ]

		if using_fast_ks[ -1 ] < 30:
			#if fast_k_inc_val[ 1 ] > 0: # fast_K_inc_val[ 1 ] > 매직넘버
			if fast_k_inc_val[ 0 ] <= 0 and fast_k_inc_val[ 1 ] > 0: # fast_K_inc_val[ 1 ] > 매직넘버
				if using_fast_ks[ -2 ] < using_slow_ds[ -2 ] and using_fast_ks[ -1 ] > using_slow_ds[ -1 ]:
					buy_or_not = True

	if buy_or_not:
		print( 'fast ks: {0}'.format( using_fast_ks ) )
		print( 'slow ds: {0}'.format( using_slow_ds ) )
		print()
	
	return buy_or_not
###/buyOrNot


def sellOrNot( fast_ks, slow_ks, slow_ds, rsis ):
	sell_or_not = False

	stoch_using_time_base = '1m' # 스토캐스틱 지표에서 사용할 분봉 단위
	stoch_using_period    = 10   # 스토캐스틱 지표에서 사용할 기간 단위
	using_fast_ks = fast_ks[ stoch_using_time_base ][ stoch_using_period ][ :-1 ]
	using_slow_ks = slow_ks[ stoch_using_time_base ][ stoch_using_period ][ :-1 ]
	using_slow_ds = slow_ds[ stoch_using_time_base ][ stoch_using_period ][ :-1 ]

	# 포맷: [ 전 단계 fast_k 증가치, 현재 단계 fast_k 증가치 ]
	fast_k_inc_val = [ using_fast_ks[ -2 ] - using_fast_ks[ -3 ], using_fast_ks[ -1 ] - using_fast_ks[ -2 ] ]
	# 포맷: [ 전 단계 slow_k 증가치, 현재 단계 slow_k 증가치 ]
	slow_k_inc_val = [ using_slow_ks[ -2 ] - using_slow_ks[ -3 ], using_slow_ks[ -1 ] - using_slow_ks[ -2 ] ]
	# 포맷: [ 전 단계 slow_d 증가치, 현재 단계 slow_d 증가치 ]
	slow_d_inc_val = [ using_slow_ds[ -2 ] - using_slow_ds[ -3 ], using_slow_ds[ -1 ] - using_slow_ds[ -2 ] ]

	"""
	if slow_k_inc_val[ 1 ] < 0: # fast_K_inc_val[ 1 ] < 매직넘버
		sell_or_not = True
	elif using_slow_ks[ -1 ] <= using_slow_ds[ -1 ]:
		sell_or_not = True
	elif using_slow_ks[ -1 ] >= 80:
		sell_or_not = True
	"""

	print( 'fast ks: {0}'.format( using_fast_ks ) )
	print( 'slow ds: {0}'.format( using_slow_ds ) )
	print()
	if fast_k_inc_val[ 1 ] < -10: # fast_K_inc_val[ 1 ] < 매직넘버
		sell_or_not = True
	elif using_fast_ks[ -1 ] <= using_slow_ds[ -1 ]-10 and slow_d_inc_val[ 1 ] < 5:
		sell_or_not = True
	elif using_fast_ks[ -1 ] >= 80:
		sell_or_not = True

	"""
	if fast_k_inc_val[ 1 ] < 0: # fast_K_inc_val[ 1 ] < 매직넘버
		sell_or_not = True
	elif using_fast_ks[ -1 ] <= using_slow_ds[ -1 ]:
		sell_or_not = True
	elif using_fast_ks[ -1 ] >= 80:
		sell_or_not = True
	"""

	"""
	print( '* fast ks: {0}'.format( fast_ks[ '5m' ][ 10 ] ) )
	print( '* slow ds: {0}'.format( slow_ds[ '5m' ][ 10 ] ) )
	"""

	return sell_or_not
###/sellOrNot


def letsBuy( price ):
	bought_price = price
	has_bought   = True

	return has_bought, bought_price
###/letsBuy


def letsSell( price, bought_price, investment ):
	sold_price = price
	income = ( investment * ( sold_price / bought_price ) ) - investment
	income_rate = ( income / investment ) * 100
	has_sold = True

	return has_sold, sold_price, income, income_rate
###/letsSell


def main():
	global time_idx
	global trans_idx
	global interval_sec
	global max_candles_len
	global max_volumes_len 


	### 차트 데이터 파일 라이터 초기화
	chart_fpath  = '../test-output/'
	chart_fnames = list()
	chart_fws    = dict()
	for time_base in time_idx.keys():
		chart_fname = '{0}.tsv'.format( time_base )
		chart_fws[ time_base ] = open( chart_fpath+chart_fname, 'w', encoding='utf-8' )
	###/차트 데이터 파일 라이터 초기화


	### 운영과 관련된 변수 초기화
	empty_trans_datas_cnt     = 0     # 신규 트랜스 데이터가 연속적으로 없는 경우의 회수
	invalid_trans_status_cnt  = 0     # 신규 트랜스 데이터 답변 상태가 연속적으로 에러인 경우의 회수
	has_meet_first_trans_data = False # 유효한 트랜스 데이터를 처음으로 만났는지 유/무
	###/운영과 관련된 변수 초기화


	### 지표 관련된 변수 초기화
	start_times  = dict() # 데이터 축적 기준 시간, 포맷: { 시간단위: 데이터 축적 시작 시간 }
	candle_datas = dict() # 캔들 데이터, 포맷: { 시간단위: [ 단위별 캔들봉, ... ] }
	volume_datas = dict() # 볼륨 데이터, 포맷: { 시간단위: [ [ 단위별 매수 볼륨, 단위별 매도 볼륨 ], ... ] }
	for time_base in time_idx.keys():
		candle_datas[ time_base ] = list()
		start_times[ time_base ] = 0
		volume_datas[ time_base ] = [ [ 0, 0 ] ]

	# 스토캐스틱 지표 변수 초기화
	fast_ks = dict()
	slow_ks = dict()
	slow_ds = dict()
	stoch_periods = [ 5, 10, 15, 20 ]
	for time_base in time_idx.keys():
		fast_ks[ time_base ] = dict()
		slow_ks[ time_base ] = dict()
		slow_ds[ time_base ] = dict()
		for stoch_period in stoch_periods:
			fast_ks[ time_base ][ stoch_period ] = [ 0.0 ]
			slow_ks[ time_base ][ stoch_period ] = [ 0.0 ]
			slow_ds[ time_base ][ stoch_period ] = [ 0.0 ]
	#/스토캐스틱 지표 변수 초기화
	###/지표 관련된 변수 초기화

	data_fpath  = '../deal-data/XRP/'
	data_fnames = [ '180128.json' ]
	#data_fnames = [ '180127.json', '180128.json' ]

	for data_fname in data_fnames:
		with open( data_fpath+data_fname, 'r', encoding='utf-8' ) as fr:
			for line_idx, line in enumerate( fr ):
				if line_idx == 0:
					continue

				### 거래 데이터 로드
				line = ujson.loads( line.strip() )

				time_stamp      = line[ 'timestamp' ]
				trans_timestamp = line[ 'trans_timestamp' ]

				order_status = line[ 'order_status' ]
				order_datas  = line[ 'order_datas' ]

				trans_status = line[ 'trans_status' ]
				trans_datas  = line[ 'trans_datas' ]
				###/거래 데이터 로드


				### 거래 데이터 정합성 판단
				if len( trans_datas ) <= 0: # 신규 거래 데이터가 없는 경우
					empty_trans_datas_cnt += 1
					continue
				else: # 신규 거래 데이터가 다시 생겨난 경우
					empty_trans_datas_cnt = 0

				if trans_status != '0000': # 트랜스 데이터 답변 에러
					invalid_trans_status_cnt += 1
					continue
				else: # 트랜스 데이터 정상 답변
					invalid_trans_status_cnt = 0
				###/거래 데이터 정합성 판단

				
				### 지표 생성 시점 체크
				if not has_meet_first_trans_data:
					if len( trans_datas ) > 0: # 처음으로 유효한 트랜스 데이터를 만난 경우
						init_price     = int( trans_datas[ 0 ][ trans_idx[ 'price' ] ] ) # 첫 트랜스(거래) 평단가
						init_timestamp = trans_datas[ 0 ][ trans_idx[ 'trans_date' ] ] # 첫 트랜스(거래) 시간
						for time_base in time_idx.keys():
							start_times[ time_base ] = trans_datas[ 0 ][ trans_idx[ 'sec_time' ] ]
							candle_datas[ time_base ].append( [ init_price, init_price, init_price, init_price,
							                                    0, init_timestamp ] )
						has_meet_first_trans_data = True
						print( '첫 유요한 트랜스 데이터({0})'.format( trans_datas[ 0 ] ) )
					else: # 유효한 트랜스 데이터를 한번도 만나지 않은 경우
						continue
				###/지표 생성 시점 체크

				### 분봉 별 시간 변경 체크 변수 초기화
				time_updated = dict() # 분봉 기준으로 시간이 변경 됐는지 유무
				for time_base in time_idx.keys():
					time_updated[ time_base ] = False
				###/분봉 별 시간 변경 체크 변수 초기화


				### 캔들 데이터 업데이트
				for time_base in time_idx.keys():
					interval = interval_sec[ time_base ] # 각 분봉에 해당하는 초

					candle_datas[ time_base ], time_updated[ time_base ], cur_start_time = getCandleData( candle_datas[ time_base ], trans_datas, start_times[ time_base ], interval, max_candles_len )

					if time_updated[ time_base ]:
						start_times[ time_base ] = cur_start_time
				###/캔들 데이터 업데이트


				### 스토캐스틱 지표 업데이트
				if len( candle_datas[ '10m' ] ) >= stoch_periods[ -1 ]:
					for time_base in time_idx.keys():
						for stoch_period in stoch_periods:
							fast_k, slow_k, slow_d = getStochastics( candle_datas[ time_base ], stoch_period )

							if time_updated[ time_base ]:
								if len( fast_ks[ time_base ][ stoch_period ] ) > 4:
									fast_ks[ time_base ][ stoch_period ] = fast_ks[ time_base ][ stoch_period ][ 1: ]
									slow_ks[ time_base ][ stoch_period ] = slow_ks[ time_base ][ stoch_period ][ 1: ]
									slow_ds[ time_base ][ stoch_period ] = slow_ds[ time_base ][ stoch_period ][ 1: ]
								fast_ks[ time_base ][ stoch_period ].append( fast_k )
								slow_ks[ time_base ][ stoch_period ].append( slow_k )
								slow_ds[ time_base ][ stoch_period ].append( slow_d )
							else:
								fast_ks[ time_base ][ stoch_period ][ -1 ] = fast_k
								slow_ks[ time_base ][ stoch_period ][ -1 ] = slow_k
								slow_ds[ time_base ][ stoch_period ][ -1 ] = slow_d
				###/스토캐스틱 지표 업데이트

				
				if len( candle_datas[ '10m' ] ) >= stoch_periods[ -1 ]:
					for time_base in time_idx.keys():
						if time_updated[ time_base ]:
							out_line = str()
							candle_cols = [ 'timestamp', 'start_price', 'end_price', 'deal_cnt' ]
							for candle_col in candle_cols:
								out_line += '{0}\t'.format( candle_datas[ time_base ][ -2 ][ candle_idx[ candle_col ] ] )
							for stoch_period in stoch_periods:
								fast_k = round( fast_ks[ time_base ][ stoch_period ][ -2 ] )
								slow_k = round( slow_ks[ time_base ][ stoch_period ][ -2 ] )
								slow_d = round( slow_ds[ time_base ][ stoch_period ][ -2 ] )
								out_line += '{0}\t{1}\t{2}\t'.format( fast_k, slow_k, slow_d )
							out_line.strip()
							out_line += '\n'
							chart_fws[ time_base ].write( out_line )

	for time_base in time_idx.keys():
		chart_fws[ time_base ].close()
###/main


if __name__ == '__main__':
	main()
