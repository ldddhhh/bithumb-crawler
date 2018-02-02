import ujson
import time

time_idx     = { '1m': 0, '3m': 1, '5m': 2, '10m': 3 } # 시간 단위 to 인덱스
trans_idx    = { 'sec_time': 0, 'trans_date': 1, 'deal_type': 2,
                 'price': 3, 'units_traded': 4, 'total': 5 } # 트랜스 데이터 인덱스
interval_sec = { '1m': 60, '3m': 180, '5m': 300, '10m': 600 } # 분 단위 별 초

# 캔들 차트 인덱스, start_price: 시가, end_price: 종가, min_price: 저가, max_price: 종가, timestamp: 시가 시간
candle_idx = { 'start_price': 0, 'end_price': 1, 'min_price': 2, 'max_price': 3, 
               'deal_cnt': 4, 'timestamp': 5 }
volume_idx   = { 'bid': 0, 'ask': 1, 'all': 2 } # 볼륨 차트 인덱스, bid:매수, ask:매도

max_candles_len = 100 # 캔들 데이터 최대 저장 개수
max_volumes_len = 100 # 볼륨 데이터 최대 저장 개수

sell_flag_cnt = [ 0, 0 ]

def getCandleData( candle_datas, trans_datas, start_time, interval_time, max_len ):
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
		if cur_time >= start_time + interval_time: # 데이터 분할 시간 초과시
			if len( volume_datas ) >= max_len:
				volume_datas = volume_datas[ 1: ]

			new_volume_data = [ 0, 0, volume ]
			new_volume_data[ volume_idx[ volume_type ] ] += volume
			volume_datas.append( new_volume_data.copy() )
			time_updated = True
			start_time += interval_time
		else:
			volume_datas[ -1 ][ volume_idx[ volume_type ] ] += volume
			volume_datas[ -1 ][ volume_idx[ 'all' ] ] += volume

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


def getVolumePower( volume_datas, period ):
	global volume_idx 

	acc_bid_volume = 0.0
	acc_ask_volume = 0.0

	for volume_data in volume_datas[ -period: ]:
		acc_bid_volume += volume_data[ volume_idx[ 'bid' ] ]
		acc_ask_volume += volume_data[ volume_idx[ 'ask' ] ]

	vp = ( acc_bid_volume / acc_ask_volume ) * 100

	return vp
###/getVolumePoser


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



def buyOrNot( fast_ks, slow_ks, slow_ds, vps ):
	'''
	현재 살지 지켜볼지 결정
	'''
	global time_idx # { '1m': 0, '3m': 1, '5m': 2, '10m': 3 } 

	buy_or_not = False

	buy_votes_cnt = 0

	stoch_using_period = 10   # 스토캐스틱 지표에서 사용할 기간 단위
	for time_base in time_idx.keys():
		using_fast_ks = fast_ks[ time_base ][ stoch_using_period ]
		using_slow_ks = slow_ks[ time_base ][ stoch_using_period ]
		using_slow_ds = slow_ds[ time_base ][ stoch_using_period ]

		if len( using_slow_ks ) > 4 and len( using_slow_ds ) > 4:
			# 포맷: [ 전 단계 slow_k 증가치, 현재 단계 slow_k 증가치 ]

			if time_base in [ '1m' ]:
				slow_k_inc_val = [ using_slow_ks[ -3 ] - using_slow_ks[ -4 ], 
				                   using_slow_ks[ -2 ] - using_slow_ks[ -3 ] ]
				if using_slow_ks[ -2 ] <= 30:
					if slow_k_inc_val[ 0 ] < 0 and slow_k_inc_val[ 1 ] > 0: 
						if hasStochUpCrossed( using_slow_ks, using_slow_ds ):
							buy_votes_cnt += 1
			else:
				slow_k_inc_val = [ using_slow_ks[ -2 ] - using_slow_ks[ -3 ], 
				                   using_slow_ks[ -1 ] - using_slow_ks[ -2 ] ]
				if using_slow_ks[ -1 ] <= 30:
					if slow_k_inc_val[ 0 ] < 0 and slow_k_inc_val[ 1 ] > 0: 
						if time_base == '1m' and hasStochUpCrossed( using_slow_ks, using_slow_ds ):
							buy_votes_cnt += 1
						elif time_base == '3m' and hasStochUpCrossed( using_slow_ks[ -4: ], using_slow_ds[ -4: ] ):
							buy_votes_cnt += 1
						elif time_base == '5m' and hasStochUpCrossed( using_slow_ks[ -2: ], using_slow_ds[ -2: ] ):
							buy_votes_cnt += 1
						elif time_base == '10m':
							buy_votes_cnt += 1

	if buy_votes_cnt > 1:
		buy_or_not = True

	return buy_or_not
###/buyOrNot


def hasStochUpCrossed( slow_ks, slow_ds ):
	k_lower_than_d  = False
	has_crossed = False
	for arr_idx in range( len( slow_ks ) ):
		slow_k = slow_ks[ arr_idx ]
		slow_d = slow_ds[ arr_idx ]
		if not k_lower_than_d and slow_k < slow_d:
			k_lower_than_d = True
		elif k_lower_than_d and not has_crossed and slow_k > slow_d:
			has_crossed = True
			break

	return has_crossed
###/hasStochUpCrossed


def sellOrNot( bought_price, cur_price, fast_ks, slow_ks, slow_ds, vps ):
	global time_idx # { '1m': 0, '3m': 1, '5m': 2, '10m': 3 } 

	sell_or_not    = False
	sell_votes_cnt = 0

	stoch_using_period = 10 # 스토캐스틱 지표에서 사용할 기간 단위
	for time_base in time_idx.keys():
		using_fast_ks = fast_ks[ time_base ][ stoch_using_period ]
		using_slow_ks = slow_ks[ time_base ][ stoch_using_period ]
		using_slow_ds = slow_ds[ time_base ][ stoch_using_period ]

		# 포맷: [ 전 단계 fast_k 증가치, 현재 단계 fast_k 증가치 ]
		fast_k_inc_val = [ using_fast_ks[ -2 ] - using_fast_ks[ -3 ], using_fast_ks[ -1 ] - using_fast_ks[ -2 ] ]

		# 포맷: [ 전 단계 slow_k 증가치, 현재 단계 slow_k 증가치 ]
		slow_k_inc_val = [ using_slow_ks[ -2 ] - using_slow_ks[ -3 ], using_slow_ks[ -1 ] - using_slow_ks[ -2 ] ]

		# 포맷: [ 전 단계 slow_d 증가치, 현재 단계 slow_d 증가치 ]
		slow_d_inc_val = [ using_slow_ds[ -2 ] - using_slow_ds[ -3 ], using_slow_ds[ -1 ] - using_slow_ds[ -2 ] ]

		if slow_k_inc_val[ 1 ] < -3:
			sell_votes_cnt += 1
		elif slow_k_inc_val[ 1 ] < 0 and using_slow_ks[ -1 ] <= using_slow_ds[ -1 ]:
			sell_votes_cnt += 1

	if sell_votes_cnt > 1:
		sell_or_not = True
		
	return sell_or_not
###/sellOrNot


def letsBuy( trans_datas, investment ):
	cur_price    = int( trans_datas[ -1 ][ 3 ] )
	bought_price = cur_price
	has_coin_cnt = ( investment / cur_price ) * 0.9985 # 보유 코인 개수
	has_bought   = True

	return has_bought, bought_price, has_coin_cnt
###/letsBuy


def letsSell( trans_datas, bought_price, investment, has_coin_cnt ):
	cur_price  = int( trans_datas[ -1 ][ 3 ] )
	sold_price = cur_price

	new_investment = ( sold_price * has_coin_cnt ) * 0.9985 # 매도 후 투자금
	income         = new_investment - investment # 수익금
	income_rate    = ( income / investment ) * 100 # 수익륙

	has_sold = True

	return has_sold, sold_price, new_investment, income, income_rate
###/letsSell


def main():
	global time_idx
	global trans_idx
	global interval_sec
	global max_candles_len
	global max_volumes_len 


	deal_cnt = 0
	total_income = 0.0
	inc_income = 0.0
	dec_income = 0.0
	inc_cnt = 0
	dec_cnt = 0
	inc_rate = 0.0
	dec_rate = 0.0
	bought_price = 0.0
	has_bought = False
	investment = 1000000 # 투자금
	has_coin_cnt = 0


	### 운영과 관련된 변수 초기화
	empty_trans_datas_cnt     = 0     # 신규 트랜스 데이터가 연속적으로 없는 경우의 회수
	invalid_trans_status_cnt  = 0     # 신규 트랜스 데이터 답변 상태가 연속적으로 에러인 경우의 회수
	has_meet_first_trans_data = False # 유효한 트랜스 데이터를 처음으로 만났는지 유/무
	###/운영과 관련된 변수 초기화



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
		volume_datas[ time_base ] = [ [ 0, 0, 0 ] ]

	# 차트 지표 변수 초기화
	fast_ks = dict()
	slow_ks = dict()
	slow_ds = dict()
	vps     = dict()
	stoch_periods = [ 5, 10, 15, 20 ]
	for time_base in time_idx.keys():
		fast_ks[ time_base ] = dict()
		slow_ks[ time_base ] = dict()
		slow_ds[ time_base ] = dict()
		vps[ time_base ]     = dict()
		for stoch_period in stoch_periods:
			fast_ks[ time_base ][ stoch_period ] = [ 0.0 ]
			slow_ks[ time_base ][ stoch_period ] = [ 0.0 ]
			slow_ds[ time_base ][ stoch_period ] = [ 0.0 ]
			vps[ time_base ][ stoch_period ]     = [ 0.0 ]
	#/차트 지표 변수 초기화
	###/지표 관련된 변수 초기화

	data_fpath = '../deal-data/XRP/'
	data_fnames = [ '180127.json', '180128.json', '180129.json', '180130.json' ]
	#data_fnames = [ '180128.json' ]

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
						volume_type = trans_datas[ 0 ][ 2 ]
						volume      = float( trans_datas[ 0 ][ 4 ] )
						for time_base in time_idx.keys():
							start_times[ time_base ] = trans_datas[ 0 ][ trans_idx[ 'sec_time' ] ]
							candle_datas[ time_base ].append( [ init_price, init_price, init_price, init_price,
							                                    0, init_timestamp ] )
							new_volume_data = [ 0, 0, volume ]
							new_volume_data[ volume_idx[ volume_type ] ] += volume
							volume_datas[ time_base ].append( new_volume_data.copy() )
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
					volume_datas[ time_base ], time_updated[ time_base ], cur_start_time = getVolumeData( volume_datas[ time_base ], trans_datas, start_times[ time_base], interval, max_volumes_len )

					if time_updated[ time_base ]:
						start_times[ time_base ] = cur_start_time
				###/캔들 데이터 업데이트


				### 스토캐스틱 지표 업데이트
				if len( candle_datas[ '10m' ] ) >= stoch_periods[ -1 ]:
					for time_base in time_idx.keys():
						for stoch_period in stoch_periods:
							fast_k, slow_k, slow_d = getStochastics( candle_datas[ time_base ], stoch_period )
							vp = getVolumePower( volume_datas[ time_base ], stoch_period )

							if time_updated[ time_base ]:
								if len( fast_ks[ time_base ][ stoch_period ] ) > 9:
									fast_ks[ time_base ][ stoch_period ] = fast_ks[ time_base ][ stoch_period ][ 1: ]
									slow_ks[ time_base ][ stoch_period ] = slow_ks[ time_base ][ stoch_period ][ 1: ]
									slow_ds[ time_base ][ stoch_period ] = slow_ds[ time_base ][ stoch_period ][ 1: ]
									vps[ time_base ][ stoch_period ]     = vps[ time_base ][ stoch_period ][ 1: ]
								fast_ks[ time_base ][ stoch_period ].append( fast_k )
								slow_ks[ time_base ][ stoch_period ].append( slow_k )
								slow_ds[ time_base ][ stoch_period ].append( slow_d )
								vps[ time_base ][ stoch_period ].append( vp )
							else:
								fast_ks[ time_base ][ stoch_period ][ -1 ] = fast_k
								slow_ks[ time_base ][ stoch_period ][ -1 ] = slow_k
								slow_ds[ time_base ][ stoch_period ][ -1 ] = slow_d
								vps[ time_base ][ stoch_period ][ -1 ]     = vp
				###/스토캐스틱 지표 업데이트


				### 매수 또는 매도 상황 체크
				if not has_bought:
					buy_or_not = buyOrNot( fast_ks, slow_ks, slow_ds, vps )
					if buy_or_not:
						has_bought, bought_price, has_coin_cnt = letsBuy( trans_datas, investment )
						print( '### {0}번째 구매'.format( deal_cnt+1 ) )
						print( '#   구매 평단가 = {0}, 투자금 = {1}'.format( bought_price, investment ) )
						print( '#   구매 시점 = {0}'.format( candle_datas[ '5m' ][ -1 ][ candle_idx[ 'timestamp' ] ] ) )
				else:
					cur_price = int( trans_datas[ -1 ][ 3 ] )
					sell_or_not = sellOrNot( bought_price, cur_price, fast_ks, slow_ks, slow_ds, vps )
					if sell_or_not:
						print( '### {0}번째 판매'.format( deal_cnt+1 ) )
						has_sold, sold_price, investment, income, income_rate = letsSell( trans_datas, bought_price, 
						                                                                  investment, has_coin_cnt )
						str_income_rate = '{:3.2f}%'.format( income_rate )
						print( '#   판매 평단가 = {0}, 수익금 = {1}({2})'.format( sold_price, income, str_income_rate ) )
						print( '#   현재 잔여 투자금 = {0}'.format( investment ) )
						print( '#   판매 시점 = {0}'.format( candle_datas[ '5m' ][ -1 ][ candle_idx[ 'timestamp' ] ] ) )
						#print( '#   판매 시점 = {0}'.format( trans_datas[ -1 ][ 1 ] ) )

						if income > 0:
							inc_cnt += 1
						else:
							dec_cnt += 1
						if income_rate > 0:
							inc_rate += income_rate
						else:
							dec_rate += income_rate

						print( '#   수익 회수 = {0}, 손실 회수 = {1}'.format( inc_cnt, dec_cnt ) )
						print( '#   수익 총 비율 = {0}, 손실 총 비율 = {1}'.format( inc_rate, dec_rate ) )
						#print( '#   수익 평균 비율 = {0}, 손실 평균 비율 = {1}'.format( inc_rate/inc_cnt, dec_rate/dec_cnt ) )
						print()
						deal_cnt += 1
						has_bought = False
				###/매수 또는 매도 상황 체크
###/main



if __name__ == '__main__':
	main()
