# -*- coding: UTF-8 -*-
import os,re
import json
import urllib
import xlrd
import requests
import logging
import copy
import time

#excel file path
EXCEL_FILE_PATH = u"D:/天气星座.xlsx"
#whether it's in test environment
IS_TEST_ENVIRONMENT = False
#batch commit questions number
COMMIT_NUM = 100
#http headers
RAW_HEADERS_STR = '''Accept:text/html, */*; q=0.01
Accept-Encoding:gzip, deflate, sdch
Accept-Language:zh-CN,zh;q=0.8
Cache-Control:no-cache
Connection:keep-alive
Cookie:_pfAccess_=QCH22R3ML25ZPT3ZPTVF272USFTMUOECNQRUEEAMWWJOZYS7DR6EH5PF2I7OJMJH; JSESSIONID=A9E444C26876856C350079E6FF1C8F28.s1; __jdv=137720036|ssa.jd.com|-|referral|-; ipLoc-djd=1-72-2799-0; ipLocation=%u5317%u4EAC; areaId=1; erp1.jd.com=DC7F23EE1C9EAF0BA391744EA85881204E0279CDE380F5044B4BD2FE0125B1F511CEAC604F2F4DD6EDDD11D97CDBE30AECB96D667D4ECF0BF355C200DAEEEE84B18887E9A2F56E5AD7A61B1C63BC2927; sso.jd.com=bc143e543e2b400190fc0308603f5e39; popWindow=1; TrackID=1ge9xF0gBcuEeL0dEpwY3GAQKKgPwDQvNMTy_Acl60xvN6E1EMC-HsnkilBOuDJ9zNlbTjxFdrodF0hPWyW_J9A; pinId=zXbXsy2bb8l9ao2-6idyxw; pin=gavin1333; unick=gavin1333; thor=CA5FB9FEE622B3AE6EAC5C683D04C15B4CB6E25ECC26764FCFE6420CEC4D90C0B41CE440F8F0DE94E1F70F94E99880B8F9E89D3C09DBB4787BB427A6478E93B23CF45C6CBDEF2533448812F510362D1A12CDCA03E4D446ABA4CC60CF9A197D10E05DADFAB362E716EEBC5D6998B2A583E4397D4B19673A37E9BB4F6F0D97B9A38FA70574610B748071339E8BE2F7B971; _tp=QvUQ%2BaCKwKghOWIYqE9d8g%3D%3D; _pst=gavin1333; ceshi3.com=EJSKItZIhR2atEeNtB7FftJTlBxTXMYTFaFFPuv0prw; __jda=122270672.273710959.1471227715.1474937646.1474945164.126; __jdb=122270672.13.273710959|126.1474945164; __jdc=122270672; __jdu=273710959; jos_robot=2EE0AEDAA0F1BEFDDEC9223A85609666; jos_pin=B63BA3D7FCF1E9EB153AE10A8B9CBDB76582CC128D0A17E4F3FC8943E531ED06; jos_token_key=B63BA3D7FCF1E9EB153AE10A8B9CBDB71B8989556AB5F5E3AE7F32D1D32151712BAC889F56A4D51E7DF1E65D5CB52590
Host:nlp.jimi.jd.com
Origin:URL
Pragma:no-cache
Referer:URL/kaifangcs
User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36
'''

logging.basicConfig(filename="push_smalltalk_from_excel.log", level="DEBUG", format="%(asctime)s %(levelname)s %(message)s")

headers = {}
for line in RAW_HEADERS_STR.splitlines():
	elem = line.split(":", 1)
	if len(elem) == 2:
		headers[elem[0]] = elem[1]
print(headers)

session = requests.Session()
session.headers.update(headers)
print("session.headers:" + str(session.headers))
ferr = open("push_smalltalk_from_excel.log", "a")

PUSH_URL = 'IMPORT_URL'


RESULT_TEMPLATE = {
	"pointId":"",
	"stdQuestion":{"standard":"1", "question":""},
	"modifierPin":"manual",
	"modifierUserName":"admin",
	"answerAndTerminal":[{"terminal":"web", "answer":""},{"terminal":"m", "answer":""},
		{"terminal":"weixin", "answer":""},{"terminal":"weibo", "answer":""}]
}

point_id_map = {
	u"聊一聊":"1",
}

test_point_id_map = {
	u"聊一聊":"2824",
}

if IS_TEST_ENVIRONMENT:
	point_id_map = test_point_id_map

terminal_show_map = {u"WEB":"web", u"移动端":"m", u"微信":"weixin", u"微博":"weibo"}


def parse_terminals(terminal_str):
	terminal = terminal_str.upper()
	term_list = re.split(u",|，", terminal, re.U)
	result_list = []
	for term in term_list:
		if term == u"全部":
			return terminal_show_map.values()
		if term not in terminal_show_map:
			print("term:" + str(term) + " can't find terminal!")
			exit(-1)
			continue
		t = terminal_show_map[term]
		result_list.append(t)
	return result_list


def get_commit_data(question_group_buffer):
	if question_group_buffer == None or len(question_group_buffer) == 0:
		return None
	answers_map = {}
	kpoint = question_group_buffer[0][1]
	if kpoint not in point_id_map:
		print("Can't find point_id for :" + str(kpoint))
		logging.error("Can't find point_id for :" + str(kpoint))
		return None
	point_id = point_id_map[kpoint]
	std_question = question_group_buffer[0][4]
	
	for v in question_group_buffer:
		terminal = v[2]
		answer = v[3]
		terminal_list = parse_terminals(terminal)
		for t in terminal_list:
			answers_map[t] = answer
	
	answerAndTerminal = []
	for k,v in answers_map.iteritems():
		answerAndTerminal.append({"terminal":k, "answer":v})
	RESULT_TEMPLATE["pointId"] = point_id
	RESULT_TEMPLATE["stdQuestion"]["question"] = std_question
	RESULT_TEMPLATE["answerAndTerminal"] = answerAndTerminal
	result = copy.deepcopy(RESULT_TEMPLATE)
	return result


def contains_error(text):
	error_list = ['exception', 'error', 'err', 'warn']
	txt = text.lower()
	for e in error_list:
		if e in txt:
			return True
	return False


#main loop
if __name__ == "__main__":
	excel_file = xlrd.open_workbook(EXCEL_FILE_PATH)
	table = excel_file.sheets()[0]
	nrows = table.nrows
	ncols = table.ncols
	print("Excel table:" + str(table) + " nrows:" + str(nrows) + " ncols:" + str(ncols))

	# 1. total count
	total_cnt = nrows
	logging.info("total_cnt:" + str(total_cnt))

	# 2.parse questions in order
	cnt = 0
	result_list = []
	last_question = None
	question_group_buffer = []
	for i in range(total_cnt):
		value = table.row_values(i)
		kpoint = value[1]
		terminals = value[2]
		answer = value[3]
		std_question = value[4]
		if terminals == None or terminals == "" or std_question == "标准问题（必填）":
			continue
		#clear buffer and record new question.
		if std_question != "" and std_question != last_question:
			result = get_commit_data(question_group_buffer)
			if result != None:
				result_list.append(result)
			jstr = json.dumps(result_list)
			last_question = std_question
			question_group_buffer = [value]
		else:
			question_group_buffer.append(value)
		
		#reach COMMIT_NUM or total_num, commit it.
		if len(result_list) >= COMMIT_NUM or i == (total_cnt - 1):
			cnt += len(result_list)
			print("total questions:" + str(cnt))
			jstr = json.dumps(result_list)
			result_list = []
			print("data:" + jstr)
			payload = {"view":jstr}
			# print("data:" + jstr)

			logging.info("-----")
			logging.info("start to push payload:" + str(payload))
			retry_cnt = 0
			while retry_cnt < 3:
				try:
					# print("payload:" + str(payload))
					rsp = session.post(PUSH_URL, data=payload)
					break
				except Exception as e:
					print("requests encounter exception:" + str(e))
					retry_cnt += 1
			time.sleep(1)

			print(rsp.text)
			if 200 != rsp.status_code:
				ferr.write("push error for start:" + str(cnt) + "\n")
				print("push error for start:" + str(cnt))
			
			logging.info(rsp.text)
			if contains_error(rsp.text):
				print("error! please check!")
				logging.error("error! please check!")

