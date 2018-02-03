### 전역 변수 초기화
time_idx     = { '1m': 0, '3m': 1, '5m': 2, '10m': 3 } # 시간 단위 to 인덱스
trans_idx    = { 'sec_time': 0, 'trans_date': 1, 'deal_type': 2,
                 'price': 3, 'units_traded': 4, 'total': 5 } # 트랜스 데이터 인덱스
interval_sec = { '1m': 60, '3m': 180, '5m': 300, '10m': 600 } # 분 단위 별 초

# 캔들 차트 인덱스, start_price: 시가, end_price: 종가, min_price: 저가, max_price: 종가, timestamp: 시가 시간
candle_idx = { 'start_price': 0, 'end_price': 1, 'min_price': 2, 'max_price': 3, 
               'deal_cnt': 4, 'timestamp': 5 }
volume_idx   = { 'bid': 0, 'ask': 1, 'all': 2 } # 볼륨 차트 인덱스, bid:매수, ask:매도
###/전역 변수 초기화


def getCandleData( candle_datas, trans_datas, start_time, interval_time, max_len ):
	"""
	캔들 데이터 세팅
	param candle_datas:  캔들 데이터
	param trans_datas:   거래소 트랜잭션(거래) 데이터
	param start_time:    데이터 축적 시작 시간
	param interval_time: 데이터 분할 시간, 초단위
	param max_len:       캔들 데이터(리스트) 최대 크기
	return candle_datas: 업데이트 된 캔들 데이터
	return time_updated: 캔들 데이터 분봉 업데이트 여부
	return start_time:   현재 캔들 분봉 축적 시작 시간
	"""

	global candle_idx # { 'start_price': 0, 'end_price': 1, 'min_price': 2, 'max_price': 3, 
                    #   'deal_cnt': 4, 'timestamp': 5 }
	global trans_idx  # { 'sec_time': 0, 'trans_date': 1, 'deal_type': 2,
                    #   'price': 3, 'units_traded': 4, 'total': 5 } # 트랜스 데이터 인덱스

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
	특정 구간(interval_time, sec) 내에서의 매도채결량, 매수채결량, 전체채결량
	param volume_datas:  볼륨 데이터
	param trans_datas:   거래소 트랜잭션(거래) 데이터
	param start_time:    데이터 축적 시작 시간
	param interval_time: 데이터 분할 시간, 초단위
	param max_len:       캔들 데이터(리스트) 최대 크기
	return volume_datas: 업데이트 된 볼륨 데이터
	return time_updated: 캔들 데이터 분봉 업데이트 여부
	return start_time:   현재 캔들 분봉 축적 시작 시간
	"""
	global volume_idx # { 'bid': 0, 'ask': 1, 'all': 2 } # 볼륨 차트 인덱스, bid:매수, ask:매도

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
	global candle_idx # { 'start_price': 0, 'end_price': 1, 'min_price': 2, 'max_price': 3, 
                    #   'deal_cnt': 4, 'timestamp': 5 }

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


def getMFI( candle_datas, volume_datas, period ):
	"""
	MFI(Money Flow Index) 계산 
	"""
	global candle_idx # { 'start_price': 0, 'end_price': 1, 'min_price': 2, 'max_price': 3, 
                    #   'deal_cnt': 4, 'timestamp': 5 }
	global volume_idx # { 'bid': 0, 'ask': 1, 'all': 2 } # 볼륨 차트 인덱스, bid:매수, ask:매도

	### 인덱스 초기화
	end_price_idx = candle_idx[ 'end_price' ]
	min_price_idx = candle_idx[ 'min_price' ]
	max_price_idx = candle_idx[ 'max_price' ]

	tot_volume_idx = volume_idx[ 'all' ]
	###/인덱스 초기화

	pos_mf = 0.0
	neg_mf = 0.0
	for cur_data_idx in range( -1, -(period+1), -1 ):
		prev_data_idx = cur_data_idx - 1	

		cur_typical_price  = getTypicalPrice( candle_datas[ cur_data_idx ] ) # 현재 전형가격
		prev_typical_price = getTypicalPrice( candle_datas[ prev_data_idx ] ) # 현재 전형가격

		cur_volume = volume_datas[ cur_data_idx ][ tot_volume_idx ]
		if cur_typical_price > prev_typical_price:
			pos_mf += ( cur_typical_price * cur_volume )
		elif cur_typical_price < prev_typical_price:
			neg_mf += ( cur_typical_price * cur_volume )
	
		mfr = float()
		mfi = float()
		if neg_mf == 0:
			mfi = -1000
		else:
			mfr = pos_mf / neg_mf # Money Flow Rate
			mfi = 100 - 100 / ( 1 + mfr )

	return mfi
###/getMFI


def getTypicalPrice( candle_data ):
	"""
	MFI 계산에 사용되는 전형가격 계산
	전형가격 = ( 종가 + 저가 + 고가 ) / 3
	"""
	global candle_idx # { 'start_price': 0, 'end_price': 1, 'min_price': 2, 'max_price': 3, 
                    #   'deal_cnt': 4, 'timestamp': 5 }

	end_price_idx = candle_idx[ 'end_price' ]
	min_price_idx = candle_idx[ 'min_price' ]
	max_price_idx = candle_idx[ 'max_price' ]
	
	end_price = candle_data[ end_price_idx ] # 종가
	min_price = candle_data[ min_price_idx ] # 저가
	max_price = candle_data[ max_price_idx ] # 고가

	typical_price = ( end_price + min_price + max_price ) / 3

	return typical_price
###/getTypicalPrice
