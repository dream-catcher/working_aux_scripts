# -*- coding: UTF-8 -*-
import os,re
import codecs
import logging
import time

logging.basicConfig(filename="exec.log", level="DEBUG", format="%(asctime)s %(levelname)s %(message)s")

INPUT_FILE_PATH = "D:/Downloads/finance_im_20170112092545.csv"
# INPUT_FILE_PATH = "D:/Downloads/temp2.txt"

INPUT_FILE_ENCODING = "cp936"

CONTEXT_LEN = 1

QUERY_TAG = ""

ANSWER_TAG = ""

FILTER_STR_LIST = [
	u"顾客 .*? 加入咨询",
	u"^(你好|您好)[!！.。]*$",
	u".*?很高兴为您服务.*",
	u".*?有什么可以为您服务的.*",
	u".*?有什么可以帮您的.*",
	u".*?请您记得点击输入框上面的.*?对我的服务做出评价，谢谢您的理解和支持.*",
	u"为了提高服务质量，请您在咨询结束之后，点击.*?对本次服务做出评价，谢谢.*",
	u".*?为了提高服务质量，请您在咨询结束之后，点击.*?对客服的服务做出评价，谢谢！.*",
	u"温馨提醒您和您的家人：最近天气转凉，请注意添加衣物，谨防感冒.*",
	u".*((给个评价)|(发评价)|(做出评价)).*",
	u".*#E-yaoping 请对我的服务做出评价，谢谢.*",
	u".*#E-yaoping 为了提高服务质量.*",
	u".*多多理解与包涵.*",
	u".*点击对话框的小红心.*",
	u".*((祝愿您)|(祝福)|(祝您)|(祝你)).*"
	]
FILTER_PATTERN_LIST = []

REPLACE_STR_LIST = [
	(r"jd_\w+", "#JD_ACCOUNT#"),
	(r"\d{11}_p", "#JD_ACCOUNT_PHONE#"),
	(r"https?://[\w./?=&-]+", "#URL#"),
	(r"[\w\-.]+@[\w.]+", "#EMAIL#"),
	(r"\d{20,}", "#NUM#"),
	(r"\d{19}", "#BANKCARDNUM#"),
	(r"[\dX]{18}", "#IDENTIFICATION#"),
	(r"\d{16}", "#BANKCARDNUM#"),
	(r"\d{2,4}-\d{1,2}-\d{1,2}", "#DATE#"),
	(r"\d{2}:\d{2}:\d{2}", "#TIME#"),
	(r"\d{2,4}-\d{1,2}-\d{1,2}\s+\d{2}:\d{2}:\d{2}", "#DATETIME#"),
	(ur"(\d{2,4}年)?\s*\d{1,2}月\s*\d{1,2}(日|号)", "#DATE#"),
	(ur"(\d{2,4}\.)?\s*\d{1,2}\.\s*\d{1,2}(日|号)", "#DATE#"),
	(r"#E-\w\d{2}", "#EMOTION#"),
	(r"((\d+\.\d+)|(\d{1,}))%", "#NUM#"),
	(r"(\d+\.\d+)|(\d{2,})", "#NUM#"),
	(r"#COMMA#", ",")
]

REPLACE_METHOD_LIST = []

#clean conversation content
def clean(content):
	#filter empty string
	if content is None or str(content).strip() == "":
		return ""

	#filter fixed flow string.
	for p in FILTER_PATTERN_LIST:
		content = p.sub("", content)
		if content == "" or content is None:
			return content
	for p,repl in REPLACE_METHOD_LIST:
		content = p.sub(repl, content)
		if content == "" or content is None:
			return content
	return content


def init():
	for f in FILTER_STR_LIST:
		p = re.compile(f)
		FILTER_PATTERN_LIST.append(p)
	for pstr, repl in REPLACE_STR_LIST:
		p = re.compile(pstr)
		REPLACE_METHOD_LIST.append((p, repl))


def clean_logs(logfile):
	print("start to clean log file:" + logfile)
	r_cnt = 0
	w_cnt = 0
	with open(logfile) as fi, open("clean_logs.txt", "w") as fo:
		for line in fi:
			r_cnt += 1
			if r_cnt % 1000 == 0:
				print("processing lines:" + str(r_cnt))
			try:
				value = line.strip().decode(INPUT_FILE_ENCODING).split(",")
			except Exception as e:
				print("exception:" + str(e))
				continue
			content = value[3]
			clean_line = clean(content)
			if clean_line:
				w_cnt += 1
				fo.write(clean_line + "\n")
	print("total input lines:{0} output lines:{1}".format(r_cnt, w_cnt))


def merge_sentence(logfile):
	print("start to merge log sentences!")
	timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
	cnt = 0
	last_session = None
	last_speaker = None
	context = []
	session_history = []
	with open(INPUT_FILE_PATH) as fi, open("merge_logs.txt", "w") as fo:
		for line in fi:
			cnt += 1
			if cnt % 1000 == 0:
				print("processing lines:" + str(cnt))
			if cnt == 1: # jump over the first line
				continue
			try:
				value = line.strip().decode(INPUT_FILE_ENCODING).split(",")
			except Exception as e:
				print("exception:" + str(e))
				continue
			session_id, _, _, content, is_support_staff, _ = value
			# print("cnt:{0} session:{1} is_support_staff:{2}".format(str(cnt), session_id, is_support_staff))
			sentence = clean(content)
			if sentence is None or sentence == "":
				continue
			# session level processing
			if last_session != session_id:
				if context:
					session_history.append("\t".join(context) + "\n")
				if session_history and len(session_history) > 1:
					# print("write for session change!")
					fo.write("".join(session_history) + "\n\n")
				last_session = session_id
				last_speaker = None
				session_history = []
				context = []
			# speaker level processing
			# print("current saved last_speaker:" + str(last_speaker))
			if last_speaker == is_support_staff or not last_speaker:
				context.append(sentence)
				# print("append to context:" + sentence)
				if not last_speaker:
					last_speaker = is_support_staff
			else:
				if context:
					session_history.append("\t".join(context) + "\n")
				last_speaker = is_support_staff
				# print("update saved last_speaker:" + str(last_speaker))
				context = [sentence]
		# process tail
		if context:
			session_history.append("\t".join(context) + "\n")
			if session_history and len(session_history) > 1:
				fo.write("".join(session_history) + "\n\n")



def generate_cmp_list():
	'''
	generate context/answer compare corpus.
	'''
	timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
	print("start to read files!")
	cnt = 0
	last_session = None
	context = []
	customer_begin = False
	with open(INPUT_FILE_PATH) as fi:
		with open("./context_last{0}_{1}.txt".format(CONTEXT_LEN, timestamp), "w") as fc,\
			open("./answer_{0}.txt".format(timestamp), "w") as fa:
			for line in fi:
				cnt += 1
				try:
					value = line.strip().decode(INPUT_FILE_ENCODING).split(",")
				except Exception as e:
					print("exception:" + str(e))
					continue
				session_id = value[0]
				content = value[3]
				is_support_staff = value[4]
				sentence = clean(content)
				if sentence is None or sentence == "":
					continue
				if session_id != last_session:
					context = []
					last_session = session_id
					customer_begin = False
				if is_support_staff == "1" and re.match("(#EMOTION#)+", sentence):
					continue
				if not customer_begin and is_support_staff == "0":
					customer_begin = True
				if customer_begin and is_support_staff == "1":
					fc.write("".join(context[-CONTEXT_LEN:]) + "\n")
					fa.write(ANSWER_TAG + sentence + "\n")
				if customer_begin:
					prefix = QUERY_TAG if is_support_staff == "0" else ANSWER_TAG
					context.append(prefix + sentence)

				if cnt % 100 == 0:
					print("read cnt:" + str(cnt))

			
	print("finish!")

def generate_qa_list():
	timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
	print("start to read files!")
	cnt = 0
	last_session = None
	context = []
	last_sentence_role = None
	customer_begin = False
	with open(INPUT_FILE_PATH) as fi:
		with open("./context_qa_{}.txt".format(timestamp), "w") as fc,\
			open("./answer_qa_{}.txt".format(timestamp), "w") as fa:
			for line in fi:
				cnt += 1
				try:
					value = line.strip().decode(INPUT_FILE_ENCODING).split(",")
				except Exception as e:
					print("exception:" + str(e))
					continue
				session_id = value[0]
				content = value[3]
				is_support_staff = value[4]
				sentence = clean(content)
				if sentence is None or sentence == "":
					continue
				if session_id != last_session:
					context = []
					last_session = session_id
					last_sentence_role = None
					customer_begin = False
				if is_support_staff == "1" and re.match("(#EMOTION#)+", sentence):
					continue
				if not customer_begin and is_support_staff == "0":
					customer_begin = True
				if customer_begin and is_support_staff == "1" and last_sentence_role == "0":
					fc.write("".join(context[-CONTEXT_LEN:]) + "\n")
					fa.write(sentence + "\n")
				if customer_begin:
					context.append(sentence)
					last_sentence_role = is_support_staff

				if cnt % 100 == 0:
					print("read cnt:" + str(cnt))

			
	print("finish!")

def extract_customer_questions():
	print("start to extract questions!")
	cnt = 0
	last_session = None
	context = []
	customer_begin = False
	with open(INPUT_FILE_PATH) as fi:
		with open("./question.txt", "w") as fq:
			for line in fi:
				cnt += 1
				try:
					value = line.strip().decode(INPUT_FILE_ENCODING).split(",")
				except Exception as e:
					print("exception:" + str(e))
					continue
				content = value[3]
				is_support_staff = value[4]

				if is_support_staff == "0":
					sentence = clean(content)
					if sentence:
						fq.write(sentence + "\n")
				
				if cnt % 100 == 0:
					print("read cnt:" + str(cnt))
	print("finish")

def extract_recommend_questions(finput, foutput):
	KEY_WORDS = [u"我想买", u"推荐"]
	cnt = 0
	line_cnt = 0
	with open(finput) as fi, open(foutput, "w") as fo:
		for line in fi:
			line_cnt += 1
			try:
				line = line.strip().decode(INPUT_FILE_ENCODING)
			except Exception as e:
				print("line:{} exception:{}".format(line_cnt, e))
				continue
			for w in KEY_WORDS:
				if w in line:
					fo.write(line + "\n")
					cnt += 1
					break
		print("found total lines:{}".format(cnt))

if __name__ == "__main__":
	init()

	generate_cmp_list()

	# clean_logs(INPUT_FILE_PATH)

	# merge_sentence(INPUT_FILE_PATH)

	# extract_customer_questions()

	# generate_qa_list()

	# extract_recommend_questions(INPUT_FILE_PATH, "recommand_dialog.txt")
	
	

