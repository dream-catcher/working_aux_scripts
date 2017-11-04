# -*- coding: UTF-8 -*-
import os,re
import json
import urllib
import MySQLdb
import requests
import logging
import copy
import time

logging.basicConfig(filename="push_smalltalk_post.log", level="DEBUG", format="%(asctime)s %(levelname)s %(message)s")

RAW_HEADERS_STR = '''Accept:text/html, */*; q=0.01
Accept-Encoding:gzip, deflate, sdch
Accept-Language:zh-CN,zh;q=0.8
Cache-Control:no-cache
Connection:keep-alive
Cookie:_pfAccess_=QCH22R3ML25ZPT3ZPTVF272USFTMUOECNQRUEEAMWWJOZYS7DR6EH5PF2I7OJMJH; ipLocation=%u5317%u4EAC; ipLoc-djd=1-72-2799-0; __jdv=122270672|direct|-|none|-; _jrda=2; TrackID=1T-IzTGp-oYKjozitoj01BWvE-bAqmDFof775S2Ev0_pseg2ucBflPjqJ3-QJz4aSByFdrfWm1g9fFMGQd5mAzw; pinId=zXbXsy2bb8l9ao2-6idyxw; pin=gavin1333; unick=gavin1333; thor=518D259E7A6C431890FCC9173C13491BCEEC0182B0EE46B8E20ADF3F6BC29CF6BD74C7A3BAD025367C4A9A9FA209848CDB31F410E530AF2E7F8EF831D8E9F9899C1751FA6B17AF3917596C3B3D7F36FC45503CDDE66108346EDE25580FB843F4709AA47C1C69C9EC6EF2B3699C07BB2FCD5F43258B9BB5BBDD5C0606338953C997807627B42DA7258951FC1AFE8238ED; _tp=QvUQ%2BaCKwKghOWIYqE9d8g%3D%3D; _pst=gavin1333; ceshi3.com=EJSKItZIhR2atEeNtB7FftJTlBxTXMYTFaFFPuv0prw; __jda=137720036.273710959.1471227715.1472743415.1472745775.60; __jdc=137720036; __jdu=273710959; 3AB9D23F7A4B3C9B=d3fa7324f8414d18b94192dfe90e7eb52012382983; JSESSIONID=29FC4671EC4A662F0404D4F052A387A6; erp1.jd.com=D2541CD03DA0A21450692184F5C0ACD55DD54DE3384035CF8B350BE441B645B7E682B75CBD405315C60D9B4698774D3A6D2D8F5F1E2F124E6ACF229F9FFD52C97C3B3953306582744AAF55E6A2C88BDC; sso.jd.com=b72fa9205c2e491c8791f72c9a96f16c; jos_pin=B63BA3D7FCF1E9EB153AE10A8B9CBDB76582CC128D0A17E4F3FC8943E531ED06; jos_token_key=B63BA3D7FCF1E9EB153AE10A8B9CBDB7211A4807E5610E7CB851AC5590C452C26B0E2C0591288C3A3279A3DB83D25A42; jos_robot=2EE0AEDAA0F1BEFDDEC9223A85609666
Host:nlp.jimi.jd.com
Origin:URL
Pragma:no-cache
Referer:URL/ijimi
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
ferr = open("push_smalltalk_error.log", "a")

QUERY_TOTAL_SQL = 'SELECT COUNT(id) FROM npf_faq_corpus WHERE robot_id = 0 AND corpus_type = 2'
QUERY_SQL_TEMPLATE = 'SELECT Q.question, A.answer, A.terminal, C.point_id FROM npf_faq_corpus C INNER JOIN npf_faq_corpus_question Q INNER JOIN npf_faq_corpus_answer A ON C.id=Q.corpus_id AND C.id=A.corpus_id  WHERE C.robot_id=0 AND Q.standard=1 AND A.terminal="web" LIMIT {0}, {1}'
PUSH_URL = 'import_url'
DB_KEYS = ['question', 'answer', 'terminal', 'point_id']
KEYS_LEN = len(DB_KEYS)
START_POS = 24000
PUSH_NUMBER_ONCE = 500

RESULT_TEMPLATE = {
	"pointId":"",
	"stdQuestion":{"standard":"1", "question":""},
	"modifierPin":"admin",
	"modifierUserName":"admin",
	"answerAndTerminal":[{"terminal":"web", "answer":""},{"terminal":"m", "answer":""},
		{"terminal":"weixin", "answer":""},{"terminal":"weibo", "answer":""}]
}

# RESULT_TEMPLATE = '{"modifiedTime": "2016-8-12 00:04:45", "pointId": "{0}", "stdQuestion": {"question": "{1}", "standard": "1"}, "modifierPin": "admin", "modifierUserName": "admin", "answerAndTerminal": [{"terminal": "web", "answer": "{3}"}]}'

point_id_map = {
	"2824":"1",
	"2825":"2",
	"2826":"3",
	"2827":"4",
	"2828":"5",
	"2829":"6",
	"2830":"7",
	"2831":"8",
	"2832":"9",
	"2833":"10",
	"2834":"11",
	"2835":"12",
	"2836":"13",
	"2837":"14",
	"2838":"15"
}

conn = MySQLdb.connect(host='xxx.xxx.xxx.xxx',user="user",passwd='passwd',
	db='db_name',port=3306,charset="utf8")
cursor = conn.cursor()

def ping_db():
	global conn
	global cursor
	try:
		conn.ping()
	except Exception as e:
		print("connect exception:" + str(e))
		print("restart connect")
		conn = MySQLdb.connect(host='xxx.xxx.xxx.xxx',user="user",passwd='passwd',
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
		key = str(d['point_id'])
		# print(key)
		if key not in point_id_map:
			continue
		point_id = point_id_map[key]
		RESULT_TEMPLATE["pointId"] = point_id
		RESULT_TEMPLATE["stdQuestion"]["question"] = d['question']
		RESULT_TEMPLATE["answerAndTerminal"][0]["answer"] = d['answer']
		RESULT_TEMPLATE["answerAndTerminal"][1]["answer"] = d['answer']
		RESULT_TEMPLATE["answerAndTerminal"][2]["answer"] = d['answer']
		RESULT_TEMPLATE["answerAndTerminal"][3]["answer"] = d['answer']
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
		# print("data:" + jstr)
		payload = {"view":jstr}
		# print("payload:" + str(payload))

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
		time.sleep(1)

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