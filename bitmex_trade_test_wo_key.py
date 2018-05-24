# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from time import sleep
import pandas as pd
import numpy as np
import ccxt


##### Detect Signal Function #####
def makeSignal(df, n=18, K=4):
	df["hl"] = df["h"] - df["l"]
	df["hl_ma"] = df["hl"].rolling(n).mean()
	df["hl_ma_1"] = df["hl_ma"].shift()
	df["ho"] = df["h"] - df["o"]
	df["lo"] = df["l"] - df["o"]
	df["signal"] = np.nan

	# loc
	m = n
	s, l = int(m/2), int(m*2)
	df["loc_s"] = (df["c"] - df["l"].rolling(s).min()) / (df["h"].rolling(s).max()-df["l"].rolling(s).min())
	df["loc_m"] = (df["c"] - df["l"].rolling(m).min()) / (df["h"].rolling(m).max()-df["l"].rolling(m).min())
	df["loc_l"] = (df["c"] - df["l"].rolling(l).min()) / (df["h"].rolling(l).max()-df["l"].rolling(l).min())

	# test_case
	test_buy1 = (df["hl_ma_1"] * K) < abs(df["ho"])
	test_buy2 = df["loc_s"] > 0.8
	
	test_sell1 = (df["hl_ma_1"] * K) < abs(df["lo"])
	test_sell2 = df["loc_s"] < 0.2

	df.loc[test_buy1&test_buy2, "signal"] = 1
	df.loc[test_sell1&test_sell2, "signal"] = -1

	return df

##### Get Position Function #####
def getCurrentPosition(json_data):
	data = json_data[0]
	try:
		pos = data["currentQty"]
		if data["currentQty"] > 0:
			side = 1
			avg_entry_price = data["avgEntryPrice"]

		elif data["currentQty"] < 0:
			side = -1
			avg_entry_price = data["avgEntryPrice"]
	
		else:
			side = 0
			avg_entry_price = 0

		return side, abs(pos), avg_entry_price

	except:
		print("Error : Can't get Current Position")
		return None, None, None


##### Make Order Function #####
def makeOrder(sig, side, lot):

	res_order = bm.create_order("BTC/USD", type="market", side=side, amount=int(lot))

	return res_order
	

if __name__ == "__main__":
	bm = ccxt.bitmex({
		"apiKey": "",
		"secret": "",
	})
	# for test
	bm.urls["api"] = bm.urls["test"]

	period = 36 * 2
	K = 4

	pos, lot, price_init = getCurrentPosition(bm.private_get_position())

	while True:
		ts = datetime.now() - timedelta(minutes=180)
		ts = int(datetime.timestamp(ts) * 1000)
		
		data = bm.fetch_ohlcv(symbol="BTC/USD", timeframe="5m", since=ts, limit=period)
		
		bmdf = pd.DataFrame(data, columns=["t", "o", "h", "l", "c", "v"])
		bmdf.index = pd.DatetimeIndex(bmdf["t"].apply(lambda t: datetime.fromtimestamp(t/1000)))
		bmdf = bmdf.drop("t", axis=1)
		print(bmdf.tail())

		sigdf = makeSignal(bmdf, n=18, K=3.2)
		print(sigdf)
		#print(len(sigdf))

		sig = sigdf["signal"][-1]
		print(sig)

		if sig == 1:
			side_label = "buy"
			if pos == 0:
				makeOrder(sig, side_label, lot)
			elif pos == -1:
				makeOrder(sig, side_label, lot*2)
				
		elif sig == -1:
			side_label = "sell"
			if pos == 0:
				makeOrder(sig, side_label, lot)
			elif pos == 1:
				makeOrder(sig, side_label, lot*2)

		else:
			continue

		pos, lot, price = getCurrentPosition(bm.private_get_position())

		sleep(300)


