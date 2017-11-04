# -*- coding:utf-8 -*-
import urllib
import requests

SOLR_URL_BASE = "http://solrmaster.jimi.360buy.com/ware_consult/update?"
QUERY = "stream.body=<delete><query>{0}</query></delete>"
COMMIT_URL = "http://solrmaster.jimi.360buy.com/ware_consult/update?stream.body=<commit/>"

ids = "1338288-52645202,1338288-50998759"
id_list = ids.split(",")

for i in id_list:
	query = QUERY.format("question_id:" + i)
	url = SOLR_URL_BASE + query
	print(url)
	rsp = requests.get(url)
	print(rsp.status_code)
	print(rsp.text)

	#commit 
	print("start to commit")
	print(COMMIT_URL)
	rsp = requests.get(COMMIT_URL)
	print(rsp.status_code)
	print(rsp.text)

