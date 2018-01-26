import ujson
import time

time_idx     = { '1m': 0, '3m': 1, '5m': 2, '10m': 3 } # 시간 단위 to 인덱스
trans_idx    = { 'sec_time': 0, 'trans_date': 1, 'deal_type': 2,
                 'price': 3, 'units_traded': 4, 'total': 5 } # 트랜스 데이터 인덱스
interval_sec = { '1m': 60, '3m': 180, '5m': 300, '10m': 600 } # 분 단위 별 초

candle_idx   = { 'start_price': 0, 'end_price': 1, 'min_price': 2, 'max_price': 3 } # 캔들 차트 인덱스, start_price: 시가,
                                                                                    # end_price: 종가, min_price: 저가, max_price: 고가
volume_idx   = { 'bid': 0, 'ask': 1 } # 볼륨 차트 인덱스, bid:매수, ask:매도

def getCandleData( candle_datas, trans_datas, start_time, interval_time, max_len ):
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

		### 여기부터 해보자, 캔들데이터가 뭔가 이상함(볼륨도 같이 조절)
		cur_time = trans_data[ 0 ]
		if cur_time - start_time > interval_time: # 데이터 분할 시간 초과시
			if len( candle_datas ) >= max_len:
				candle_datas = candle_datas[ 1: ]
			candle_datas.append( [ cur_price, cur_price, cur_price, cur_price ] )
			time_updated = True
			start_time += interval_time
		else:
			prev_min_price = candle_datas[ -1 ][ candle_idx[ 'min_price' ] ]
			prev_max_price = candle_datas[ -1 ][ candle_idx[ 'max_price' ] ]
			candle_datas[ -1 ][ candle_idx[ 'min_price' ] ] = min( prev_min_price, min_price ) # 저가 갱신
			candle_datas[ -1 ][ candle_idx[ 'max_price' ] ] = max( prev_max_price, max_price ) # 고가 갱신
			candle_datas[ -1 ][ candle_idx[ 'end_price' ] ] = cur_price # 종가 갱신

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

	return fast_k, slow_d
###/getStochastics


def buyOrNot( fast_ks, slow_ds ):
	'''
	현재 살지 지켜볼지 결정
	'''
	buy_or_not = False

	stoch_using_time_base = '5m' # 스토캐스틱 지표에서 사용할 분봉 단위
	stoch_using_period    = 15   # 스토캐스틱 지표에서 사용할 기간 단위
	using_fast_ks = fast_ks[ stoch_using_time_base ][ stoch_using_period ]
	using_slow_ds = slow_ds[ stoch_using_time_base ][ stoch_using_period ]

	if len( using_fast_ks ) > 4 and len( using_slow_ds ) > 4:
		# 포맷: [ 전 단계 fast_k 증가치, 현재 단계 fast_k 증가치 ]
		fast_k_inc_val = [ using_fast_ks[ -2 ] - using_fast_ks[ -3 ], using_fast_ks[ -1 ] - using_fast_ks[ -2 ] ]

		if using_fast_ks[ -1 ] < 20:
			if fast_k_inc_val[ 0 ] <= 0 and fast_k_inc_val[ 1 ] > 2: # fast_K_inc_val[ 1 ] > 매직넘버
				if using_fast_ks[ -2 ] < using_slow_ds[ -2 ] and using_fast_ks[ -1 ] > using_slow_ds[ -1 ]:
					buy_or_not = True

	return buy_or_not
###/buyOrNot


def sellOrNot( fast_ks, slow_ds ):
	sell_or_not = False

	stoch_using_time_base = '5m' # 스토캐스틱 지표에서 사용할 분봉 단위
	stoch_using_period    = 15   # 스토캐스틱 지표에서 사용할 기간 단위
	using_fast_ks = fast_ks[ stoch_using_time_base ][ stoch_using_period ]
	using_slow_ds = slow_ds[ stoch_using_time_base ][ stoch_using_period ]

	# 포맷: [ 전 단계 fast_k 증가치, 현재 단계 fast_k 증가치 ]
	fast_k_inc_val = [ using_fast_ks[ -2 ] - using_fast_ks[ -3 ], using_fast_ks[ -1 ] - using_fast_ks[ -2 ] ]

	# 포맷: [ 전 단계 slow_d 증가치, 현재 단계 slow_d 증가치 ]
	slow_d_inc_val = [ using_slow_ds[ -2 ] - using_slow_ds[ -3 ], using_slow_ds[ -1 ] - using_slow_ds[ -2 ] ]
	if fast_k_inc_val[ 1 ] < 0: # fast_K_inc_val[ 1 ] < 매직넘버
		sell_or_not = True
	elif using_fast_ks[ -1 ] <= using_slow_ds[ -1 ]-2:
		sell_or_not = True
	elif using_fast_ks[ -1 ] >= 75:
		sell_or_not = True

	return sell_or_not
###/sellOrNot


def letsBuy( trans_datas ):
	cur_price = int( trans_datas[ -1 ][ 3 ] )
	bought_price = cur_price
	has_bought   = True

	return has_bought, bought_price
###/letsBuy


def letsSell( trans_datas, bought_price, investment ):
	cur_price = int( trans_datas[ -1 ][ 3 ] )
	sold_price = cur_price
	income = ( investment * ( sold_price / bought_price ) ) - investment
	income_rate = '{:3.2f}%'.format( ( income / investment ) * 100 )
	has_sold = True

	return has_sold, sold_price, income, income_rate
###/letsSell


def main():
	global time_idx
	global trans_idx
	global interval_sec

	deal_cnt = 0
	total_income = 0.0
	inc_income = 0.0
	dec_income = 0.0
	inc_cnt = 0
	dec_cnt = 0
	bought_price = 0.0
	has_bought = False
	investment = 1000000 # 투자금

	fw = open( '../test-output/a.tsv', 'w', encoding='utf-8' )

	data_fpath = '../deal-data/XRP/'
	data_fname = '180121.json'

	start_times  = dict() # 데이터 축적 시작 시간, 포맷: { 시간단위: 데이터 축적 시작 시간 }
	candle_datas = dict() # 캔들 데이터, 포맷: { 시간단위: [ 단위별 캔들봉, ... ] }
	volume_datas = dict() # 볼륨 데이터, 포맷: { 시간단위: [ [ 단위별 매수 볼륨, 단위별 매도 볼륨 ], ... ] }
	for time_base in time_idx.keys():
		candle_datas[ time_base ] = list()
		start_times[ time_base ] = 0
		volume_datas[ time_base ] = [ [ 0, 0 ] ]

	fast_ks = dict()
	slow_ds = dict()
	stoch_periods = [ 5, 10, 15, 20 ]
	for time_base in time_idx.keys():
		fast_ks[ time_base ] = dict()
		slow_ds[ time_base ] = dict()
		for stoch_period in stoch_periods:
			fast_ks[ time_base ][ stoch_period ] = [ 0.0 ]
			slow_ds[ time_base ][ stoch_period ] = [ 0.0 ]

	has_meet_first_trans_data = False # 유효한 첫 데이터를 만났는지 유무
	empty_trans_datas_cnt    = 0 # 빈 트랜스 데이터가 연속으로 나타난 회수
	invalid_trans_status_cnt = 0 # 연속적으로 trans_status가 0000이 아닌 값이 나타난 회수
	with open( data_fpath+data_fname, 'r', encoding='utf-8' ) as fr:
		for line_idx, line in enumerate( fr ):
			if line_idx == 0:
				continue

			line = ujson.loads( line )

			trans_status = line[ 'trans_status' ]
			trans_datas  = line[ 'trans_datas' ]

			if len( trans_datas ) <= 0:
				empty_trans_datas_cnt += 1
				#print( 'Empty trans dats({0})'.format( empty_trans_datas_cnt ) )
				continue
			else:
				empty_trans_datas_cnt = 0

			if trans_status != '0000':
				invalid_trans_status_cnt += 1
				#print( 'Invalid trans status({0})'.format( invalid_trans_status_cnt ) )
				continue
			else:
				invalid_trans_status_cnt = 0

			# 유효한 첫 데이터를 만난적이 없으며, 현재 데이터도 유효하지 않은 경우
			if not has_meet_first_trans_data and len( trans_datas ) <= 0:
				continue
			# 유효한 첫 데이터를 만난적이 없으며, 현재 데이터가 유효한 경우
			elif not has_meet_first_trans_data and len( trans_datas ) > 0:
				init_price = int( trans_datas[ 0 ][ trans_idx[ 'price' ] ] )
				for time_base in start_times.keys():
					start_times[ time_base ] = trans_datas[ 0 ][ trans_idx[ 'sec_time' ] ]
					candle_datas[ time_base ].append( [ init_price, init_price, init_price, init_price ] )
				has_meet_first_trans_data = True

			time_updated   = dict()
			for time_base in time_idx.keys():
				time_updated[ time_base ] = False

			for time_base in start_times.keys():
				interval = interval_sec[ time_base ]
				candle_datas[ time_base ], time_updated[ time_base ], new_start_time = getCandleData( candle_datas[ time_base ], trans_datas,
				                                                           start_times[ time_base ], interval, 54 )
				volume_datas[ time_base ], time_updated[ time_base ], new_start_time = getVolumeData( volume_datas[ time_base ], trans_datas,
				                                                                                      start_times[ time_base ], interval, 54 )

				if time_updated[ time_base ]:
					start_times[ time_base ] = new_start_time

			if len( candle_datas[ '10m' ] ) >= stoch_periods[ -1 ]:
				for time_base in time_idx.keys():
					for stoch_period in stoch_periods:
						fast_k, slow_d = getStochastics( candle_datas[ time_base ], stoch_period )
						if time_updated[ time_base ]:
							if len( fast_ks[ time_base ][ stoch_period ] ) > 4:
								fast_ks[ time_base ][ stoch_period ] = fast_ks[ time_base ][ stoch_period ][ 1: ]
								slow_ds[ time_base ][ stoch_period ] = slow_ds[ time_base ][ stoch_period ][ 1: ]
							fast_ks[ time_base ][ stoch_period ].append( fast_k )
							slow_ds[ time_base ][ stoch_period ].append( slow_d )
						else:
							fast_ks[ time_base ][ stoch_period ][ -1 ] = fast_k
							slow_ds[ time_base ][ stoch_period ][ -1 ] = slow_d

				if time_updated[ '5m' ]:
					out_start_price = candle_datas[ '5m' ][ -2 ][ 0 ]
					out_end_price   = candle_datas[ '5m' ][ -2 ][ 1 ]
					out_fk = fast_ks[ '5m' ][ 15 ][ -2 ]
					out_sd = slow_ds[ '5m' ][ 15 ][ -2 ]
					fw.write( '{0}\t{1}\t{2}\t{3}\n'.format( out_start_price, out_end_price, out_fk, out_sd ) )

			if not has_bought:
				buy_or_not = buyOrNot( fast_ks, slow_ds )
				if buy_or_not:
					has_bought, bought_price = letsBuy( trans_datas )
					print( '### {0}번째 구매'.format( deal_cnt+1 ) )
					print( '#   구매 평단가 = {0}, 투자금 = {1}'.format( bought_price, investment ) )
					print( '#   구매 시점 = {0}'.format( trans_datas[ -1 ][ 1 ] ) )
			else:
				sell_or_not = sellOrNot( fast_ks, slow_ds )
				if sell_or_not:
					print( '### {0}번째 판매'.format( deal_cnt+1 ) )
					has_sold, sold_price, income, income_rate = letsSell( trans_datas, bought_price, investment )
					print( '#   판매 평단가 = {0}, 수익금 = {1}({2})'.format( sold_price, income, income_rate ) )
					investment += income
					print( '#   현재 잔여 투자금 = {0}'.format( investment ) )
					print( '#   판매 시점 = {0}'.format( trans_datas[ -1 ][ 1 ] ) )
					deal_cnt += 1
					has_bought = False

	fw.close()
###/main



if __name__ == '__main__':
	main()
