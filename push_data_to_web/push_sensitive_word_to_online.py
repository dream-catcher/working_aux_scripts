# -*- coding: UTF-8 -*-
import os,re
import json
import urllib
import MySQLdb
import requests
import logging
import copy

logging.basicConfig(filename="push_sensitive_post.log", level="DEBUG", format="%(asctime)s %(levelname)s %(message)s")

RAW_HEADERS_STR = '''Accept:text/html, */*; q=0.01
Accept-Encoding:gzip, deflate, sdch
Accept-Language:zh-CN,zh;q=0.8
Cache-Control:no-cache
Connection:keep-alive
Cookie:_pfAccess_=QCH22R3ML25ZPT3ZPTVF272USFTMUOECNQRUEEAMWWJOZYS7DR6EH5PF2I7OJMJH; __jdv=122270672|direct|-|none|-; ipLocation=%u5317%u4EAC; areaId=1; ipLoc-djd=1-72-2799-0; erp1.jd.com=908B865CD29B08EEB4107A1C37E798BB0B2F500FFEF4BEE8CC2B381684681AB2535512D4735F161F8BED55F37E9E62F3A971C1BA0A28EEA12BDED17800995EA162A9312C811E803FEC74FB0492A8884A; sso.jd.com=1bf93b86d14842118463d64c9a2c9999; _jrda=1; __jda=122270672.273710959.1471227715.1471590143.1471610456.32; __jdb=122270672.3.273710959|32.1471610456; __jdc=122270672; _jrdb=1471610457224; __jdu=273710959; 3AB9D23F7A4B3C9B=d3fa7324f8414d18b94192dfe90e7eb52012382983; TrackID=1uYr8OHVjwhLF9yS8xpS9-W07gMT_EnvKFXr6hSHmhbPSzYC_CGCtdpPm6UgjcWCN89TGNs-lh8zxX9IR6RGwhljBTMmB7Jku2YqYVRQbQUs; pinId=mSty8KQOoLUS2PU4Gm2kFA; pin=cdlinjianghua5; unick=cdlinjianghua5; thor=D29F6FAF05919C50AFB937D57448EC4E939BA191668B7CF84C116A0A8A935B3594383B9353D1882942798276A04E261489166682FEFE4F410533239168F7E8EBB63E99AEA9971787436FF471E06841444A0861951F19049C8F8AF605D0B365E319C15DA335465574A4CB0BEF76EEC034A9116031B68CD0E08FAB067D7BD22B6E0BE351BCFB39A07E37E83C9F59027944; _tp=ptloJbVlgUoWlYl6Co4CyA%3D%3D; _pst=cdlinjianghua5; ceshi3.com=LqKrhdR2rgKhFWBHuRFE_aejvoZnuoxqH4085en2JNs; jos_pin=49F5FC3B2D09AA861F613632454C1D52861701B6771885572037B1EC12E4D874; jos_robot=F21A7575F93BDED480B99F509BFA5B5F
Host:nlp.jimi.jd.com
Origin:URL
Pragma:no-cache
Referer:URL/gavin
User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36
'''
headers = {}
for line in RAW_HEADERS_STR.splitlines():
	elem = line.split(":", 1)
	if len(elem) == 2:
		headers[elem[0]] = elem[1]
print(headers)

session = requests.Session()
session.headers.update(headers)
print("session.headers:" + str(session.headers))
ferr = open("push_sensitive_error.log", "a")

QUERY_TOTAL_SQL = 'SELECT COUNT(id) FROM npf_sensitive_word WHERE robot_id=0 '
QUERY_SQL_TEMPLATE = 'SELECT sensitive_word,type_id FROM npf_sensitive_word WHERE robot_id=0 LIMIT {0}, {1}'
PUSH_URL = 'IMPORT_URL'
DB_KEYS = ['sensitive_word', 'type_id']

KEYS_LEN = len(DB_KEYS)
START_POS = 0
PUSH_NUMBER_ONCE = 500

RESULT_TEMPLATE = {
	"robot_id":"0",
	"sensitive_word":"",
	"type_id":"",
	"create_time":"1471610577268",
	"modify_time":"1471610577268",
	"modifyUser":"system"
}

type_id_map = {
	"21":"1",
	"22":"2",
	"23":"3",
	"24":"4",
	"25":"5"
}

conn = MySQLdb.connect(host='xxx.xxx.xxx.xxx',user='user',passwd='passwd',
	db='db_name',port=3306,charset="utf8")
cursor = conn.cursor()

def ping_db():
	global conn
	global cursor
	try:
		conn.ping()
	except Exception as e:
		print(e)
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
			logging.warning("len error:" + str(r))
			continue
		logging.debug(r)
		d = dict(zip(DB_KEYS, r))
		key = str(d['type_id'])
		# print(key)
		if key not in type_id_map:
			continue
		type_id = type_id_map[key]
		RESULT_TEMPLATE["type_id"] = type_id
		RESULT_TEMPLATE["sensitive_word"] = d["sensitive_word"]
		result = copy.deepcopy(RESULT_TEMPLATE)
		l.append(result)
	# logging.debug(l)
	jstr = json.dumps(l)
	logging.debug("json string:" + jstr)
	return jstr

def get_total_db_cnt(PUSH_NUMBER_ONCE):
	ping_db()
	sql = QUERY_TOTAL_SQL
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
	# for i in range(1):
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
			# return 
			continue
		jstr = convert_to_json(results)
		print("data:" + jstr)
		urlencode = urllib.quote(jstr)
		logging.debug("urlencode:" + urlencode)
		payload = {"view":jstr}
		# print("data:" + jstr)

		logging.info("-----")
		logging.info("start to push payload:" + str(payload))
		retry_cnt = 0
		while retry_cnt < 3:
			try:
				rsp = session.post(PUSH_URL, data=payload)
				break
			except Exception as e:
				print("requests encounter exception:" + str(e))
				retry_cnt += 1

		print(rsp.text)
		if 200 != rsp.status_code:
			ferr.write("push error for start:" + str(start) + "\n")
			print("push error for start:" + str(start))
		
		logging.info(rsp.text)
		if contains_error(rsp.text):
			print("error! please check!")
			logging.error("error! please check!")
			# return

# MAIN LOOP EXECUTE
main_loop()


cursor.close()
conn.close()