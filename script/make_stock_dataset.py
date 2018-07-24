# -*- coding:utf-8 -*-

import requests
from bs4 import BeautifulSoup as bs
import lxml
from datetime import datetime
import pandas as pd
import os,re
import MySQLdb as mysql

def getSoup(url):
	res = requests.get(url)
	return bs(res.content,"lxml")

def normalStockSplit(years):
	lst = []
	baseUrl = "https://www.rakuten-sec.co.jp/ITS/Companyfile/stock_split_"
	ng_ptn = "^[0-9]{4}-[0-9]+-[0-9]+$"
	ok_ptn = re.compile("[0-9]{4}-[0-9]+-[0-9]+")

	for year in years:
		for i in range(1,5):
			try:
				url = baseUrl + year + str(i) + ".html"
			
				soup = getSoup(url)
				tables = soup.find_all('table')
				trgroup = tables[5].find_all('tr')

				for n in range(1,len(trgroup)):	
					tr = trgroup[n]
					tdgroup = tr.find_all('td')
					chngdate = tdgroup[0].get_text().replace('\n','').replace('/','-')
					if not re.match(ng_ptn,chngdate):
						chngdate = ok_ptn.findall(chngdate)[0]
					code = tdgroup[2].get_text().replace('\n','')
					name = tdgroup[3].get_text().replace('\n','')
					rate = tdgroup[4].get_text().replace('\n','').split(' : ')
					rate_bf = rate[0]
					rate_af = rate[1]
					lst.append([chngdate,code,name,rate_bf,rate_af])
				#print(url)
			except:
				continue
	return lst

def reverseStockSplit(years):
	lst = []
	baseUrl = "https://www.rakuten-sec.co.jp/ITS/Companyfile/reverse_stock_split_"
	ng_ptn = "^[0-9]{4}-[0-9]+-[0-9]+$"
	ok_ptn = re.compile("[0-9]{4}-[0-9]+-[0-9]+")
	
	for year in years:
		for i in range(1,5):
			try:
				url = baseUrl + year + str(i) + ".html"

				soup = getSoup(url)
				tables = soup.find_all('table')
				trgroup = tables[5].find_all('tr')

				for n in range(1,len(trgroup)):	
					tr = trgroup[n]
					tdgroup = tr.find_all('td')
					code = tdgroup[1].get_text().replace('\n','')
					name = tdgroup[2].get_text().replace('\n','')
					rate = tdgroup[3].get_text().replace('\n','').split('→')
					rate_bf = rate[0].replace('株','')
					rate_af = rate[1].replace('株','')
					rmrk = tdgroup[4].get_text().replace('\n','')
					chngdate = rmrk.split('併合前最終売買日：')[1].replace('/','-')
					if not re.match(ng_ptn,chngdate):
						chngdate = ok_ptn.findall(chngdate)[0]
					lst.append([chngdate,code,name,rate_bf,rate_af])
				#print(url)
			except:
				continue
	return lst

def getdbData(query,*args):
	host = 'localhost'
	user = 'kzk'
	pswd = os.environ.get('SETWORD')
	db = 'stocks'
	charset = 'utf8'	

	connection = mysql.connect(user=user,passwd=pswd,host=host,db=db,charset=charset)
	res = pd.read_sql(query,connection,params=args,index_col=None)
	
	connection.close()

	return res	

def makeSplitStockData(years=["2015","2016","2017","2018"]):
	listNormalSplit = normalStockSplit(years)
	listReverseSplit = reverseStockSplit(years)
	
	df_n = pd.DataFrame(listNormalSplit)
	df_r = pd.DataFrame(listReverseSplit)

	df_m = pd.concat([df_n,df_r])
	df_m.columns = ['date','code','name','before','after']
	df_m.index = pd.DatetimeIndex(df_m.date)
	df_m = df_m.drop('date',1)
	df_m = df_m.astype({'code':int,'before':float,'after':float})
	df_m = df_m.sort_index()
	
	return df_m	
	
def main():
	#株式分割・併合データを2015~2018年最新まで取得	
	splitdata = makeSplitStockData()

	#株価DBからデータを取得および取得データの整形
	#query = 'select date,code,open,high,low,close,volume from jp_stocks where date between %s and %s order by date asc'
	'''
	query = 'select * from jp_stocks where date between %s and %s order by date asc'
	dbdata = getdbData(query,'2016-01-01','2017-12-31')
	dbdata.index = pd.DatetimeIndex(dbdata.date)
	dbdata = dbdata.drop('date',1)
	dbdata = dbdata.sort_index()	

	# 分割・併合レートの計算
	sdate = '2016-01-01'
	edate = '2017-12-31'
	dbdata = dbdata.assign(rate = 1)
	for idx,rows in splitdata[sdate:edate].iterrows():
		#dbdata.loc[(dbdata.index<=idx)&(dbdata['code']==rows['code']),'rate'] = rows['after'] / rows['before']
		dbdata.loc[(dbdata.index<=idx)&(dbdata['code']==rows['code']),'rate'] *= rows['after'] / rows['before']
	
	# 分割・併合後のOHLC修正
	for column in ['open','high','low','close']:
		dbdata.loc[:,column] /= dbdata.loc[:,'rate']

	# 分割・併合後の出来高修正
	dbdata.loc[:,'volume'] *= dbdata.loc[:,'rate']
	dbdata.to_pickle("dbdata.df")
	'''
	splitdata.to_pickle("splitdata.df")


if __name__  == "__main__":
	main()
