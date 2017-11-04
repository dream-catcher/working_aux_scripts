# -*- coding: UTF-8 -*-
import os,re
import json
import urllib
import MySQLdb
import requests
import logging

logging.basicConfig(filename="push.log", level="DEBUG", format="%(asctime)s %(levelname)s %(message)s")

ferr = open("push_error.log", "w")
QUERY_SQL_TEMPLATE = 'select questionId, sku, question, answer, cate1, cate2, cate3, date from table_name limit {0}, {1}'
PUSH_URL_HEAD = 'URL?data='
DB_KEYS = ['questionId', 'sku', 'question', 'answer', 'cate1', 'cate2', 'cate3', 'date']
KEYS_LEN = len(DB_KEYS)
START_POS = 337380
PUSH_NUMBER_ONCE = 10

conn = MySQLdb.connect(host='xxx.xxx.xxx.xxx',user='user',passwd='passwd',
	db='db_name',port=3306,charset="utf8")
cursor = conn.cursor()

def ping_db():
	try:
		conn.ping()
	except Exception as e:
		print(e)
		global conn
		global cursor
		conn = MySQLdb.connect(host='xxx.xxx.xxx.xxx',user='user',passwd='passwd',
		db='db_name',port=3306,charset="utf8")
		cursor = conn.cursor()

def select_records(start, number):
	ping_db()
	sql = QUERY_SQL_TEMPLATE.format(start, number)
	results = []
	try:
	    cursor.execute(sql)
	    results = cursor.fetchall()
	except MySQLdb.Error,e:
	     logging.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
	return results

def convert_to_json(results):
	l = []
	for r in results:
		if KEYS_LEN != len(r):
			logging.warning("len error:" + r)
			continue
		logging.debug(r)
		d = dict(zip(DB_KEYS, r))
		l.append(d)
	# logging.debug(l)
	jstr = json.dumps(l)
	logging.debug("json string:" + jstr)
	return jstr

def get_total_db_cnt(PUSH_NUMBER_ONCE):
	ping_db()
	sql = 'select count(*) from table_name '
	try:
	    cursor.execute(sql)
	    results = cursor.fetchall()
	    if results and results[0]:
	    	return results[0][0]
	except MySQLdb.Error,e:
	    logging.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
	return 0

def contains_error(text):
	error_list = ['exception', 'error', 'err', 'warn']
	txt = text.lower()
	for e in error_list:
		if e in txt:
			return True
	return False

def main_loop():
	# 1. get total push cnt
	total_cnt = get_total_db_cnt(PUSH_NUMBER_ONCE)
	logging.info(total_cnt)
	total_push_cnt = (total_cnt - START_POS + PUSH_NUMBER_ONCE -1)/PUSH_NUMBER_ONCE
	logging.info("total push cnt:" + str(total_push_cnt))

	# 2.push each group in order
	cnt = 0
	for i in range(total_push_cnt):
		cnt += 1
		logging.info("----------------------- loop time:" + str(cnt))
		print("----------------------- loop time:" + str(cnt))
		start = START_POS + i * PUSH_NUMBER_ONCE
		logging.info("fetch data start:" + str(start) + " len:" + str(PUSH_NUMBER_ONCE))
		print("fetch data start:" + str(start) + " len:" + str(PUSH_NUMBER_ONCE))
		results = select_records(start, PUSH_NUMBER_ONCE)
		if not results:
			logging.error("content is empty")
			print("error:content is empty")
			return 
		jstr = convert_to_json(results)
		urlencode = urllib.quote(jstr)
		logging.debug("urlencode:" + urlencode)
		push_url = PUSH_URL_HEAD + urlencode

		logging.info("-----")
		logging.info("start to push:" + push_url)
		rsp = requests.get(push_url)
		if 200 != rsp.status_code:
			ferr.write("push error for start:" + str(start))
		logging.info(rsp.text)
		if contains_error(rsp.text):
			print("error! please check!")
			logging.error("error! please check!")
			return

# MAIN LOOP EXECUTE
main_loop()


cursor.close()
conn.close()