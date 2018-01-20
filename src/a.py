import requests

def a(cur, off, cnt):
	"""
	url = 'https://api.bithumb.com/public/recent_transactions/'
	uri = url + cur 
	r = requests.put(uri, data={'offset':off, 'count':cnt}).json()
	"""
	url = 'https://api.bithumb.com/public/recent_transactions/'
	uri = url + cur + '?offset={0}&count={1}'.format(off, cnt )
	r = requests.get(uri).json()

	return r


r = a( 'BTC', 0, 5 )
print( r )
print( len( r['data']))
