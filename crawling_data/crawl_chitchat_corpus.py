# -*- coding: UTF-8 -*-
# date: 2017-7-28
# author: Emerson

import os,re
import json
import time
import urllib
import requests
import logging
import openpyxl

logging.basicConfig(filename="log.txt", level="DEBUG", format="%(asctime)s %(levelname)s %(message)s")
#SOLR server URL
URL_TEMPLATE = ''
QUERY_NUMBER_ONCE = 1000
START_INDEX = 0

CSV_FORMAT_ANSI = False
USER_LIST_FILE_ANSI = False

FILE_OUT = time.strftime("chitchat_corpus_%Y-%m-%d_%H%M%S.txt", time.localtime())

def query_corpus():
	start = START_INDEX
	fo = open(FILE_OUT, 'w')
	while True:
		print("current reading start:{} limit:{}".format(start, QUERY_NUMBER_ONCE))
		url = URL_TEMPLATE.format(start, QUERY_NUMBER_ONCE)
		start += QUERY_NUMBER_ONCE
		logging.info(url)
		retry_cnt = 0
		while retry_cnt < 3:
			try:
				rsp = requests.get(url)
				break
			except Exception as e:
				print("requests encounter exception:" + str(e))
				retry_cnt += 1
		if 200 != rsp.status_code:
			logging.error("query error:" + url)
		content = json.loads(rsp.text)
		if 'response' not in content or 'docs' not in content['response']:
			print("error:{}".format(content))
			logging.error("error:{}".format(content))
			return []
		rows = content['response']['docs']
		if not rows:
			fo.close()
			return True 
		# write to files in json format
		for r in rows:
			s = json.dumps(r).encode("UTF-8")
			fo.write(s + '\n')
	return True


if __name__ == "__main__":
	logging.info("start to query chit-chat corpus!")
	query_corpus()
	logging.info("finish querying chit-chat corpus!")
