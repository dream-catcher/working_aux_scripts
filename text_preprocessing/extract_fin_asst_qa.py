# -*- coding: UTF-8 -*-
# date: 2017-10-30
# author: gaoyi

import os,re
import time
import openpyxl
import logging

logging.basicConfig(filename="merge_log.txt", level="DEBUG", format="%(asctime)s %(levelname)s %(message)s")
USE_STD_CORPUS = True
USE_PERSONATE_CORPUS = True
NEED_CLASSIFY = False
NO_INCORRECT_CHAR = True

FIN_EXCEL_NAME = u"D:/Downloads/线上QA语料.xlsx"

filename_suffix = time.strftime("_%Y%m%d.txt", time.localtime())
if NEED_CLASSIFY:
	filename_suffix = "c" + filename_suffix
FQ_NAME = u"D:/01WORK/03WorkPlan/2017年10月30日 金融语料训练/fin_q" + filename_suffix
FA_NAME = u"D:/01WORK/03WorkPlan/2017年10月30日 金融语料训练/fin_a" + filename_suffix

SEP = "#SEP#"

question_map = {}
dup_cnt = 0
def extract(original_file, need_classify = False):
	original_wb = openpyxl.load_workbook(original_file)
	worksheet = original_wb.active
	global dup_cnt
	global question_map
	for line_cnt, row in enumerate(worksheet.rows):
		if line_cnt == 0:
			continue
		if line_cnt % 1000 == 0:
			print("processing lines:{}".format(line_cnt))
		row_value = [cell.value for cell in row]

		classify = row_value[0]
		question = row_value[1]
		answer = row_value[2]
		if question not in question_map:
			question_map[question] = line_cnt
		else:
			print("line:{} question:{} duplicates line:{}".format(line_cnt, question, question_map[question]))
			dup_cnt += 1
			continue
		if not question or not answer or not classify:
			print("line:{} question/answer/classify can't be empty".format(line_cnt))
			continue
		yield question,answer,classify

def loop(fq_name, fa_name, need_classify=False):
	total_cnt = 0
	with open(fq_name, "w") as fq, open(fa_name, "w") as fa:
		for elements in extract(FIN_EXCEL_NAME, need_classify):
			total_cnt += 1
			q,a,classify = elements
			line = classify + "\t" + q + "\n"
			fq.write(line)
			fa.write(a + "\n")
	print("total count is:{}".format(total_cnt))
	print("duplicate cnt:{}".format(dup_cnt))

def get_question_map(excel_file):
	workbook = openpyxl.load_workbook(excel_file)
	worksheet = workbook.active
	question_map = {}
	for line_cnt, row in enumerate(worksheet.rows):
		if line_cnt == 0:
			continue
		if line_cnt % 1000 == 0:
			print("processing lines:{}".format(line_cnt))
		# if line_cnt > 100:
		# 	break
		row_value = [cell.value for cell in row]
		question = row_value[1]
		answer = row_value[2]
		# print("question type:{}".format(type(question)))
		# print("answer type:{}".format(type(answer)))
		if question not in question_map:
			question_map[question] = answer
		else:
			print("question:{} already in map!".format(question))
	return question_map

def generate_training_corpus(retrieval_file, question_map):
	err_cnt = 0
	with open(retrieval_file) as fi, open("fq.txt", "w") as fq,\
		open("fr.txt", "w") as fr, open("fa.txt", "w") as fa:
		for cnt,line in enumerate(fi):
			# if cnt > 100:
			# 	break
			if cnt % 1000 == 0:
				print("processing lines:{}".format(cnt))
			parts = line.strip().split("#SEP#")
			if len(parts) != 2:
				print("format error:{}".format(line))
				continue
			question,answer = parts
			question = unicode(question)
			answer = unicode(answer)
			# print("convert question type:{}".format(type(question)))
			# print("convert answer type:{}".format(type(answer)))
			if question not in question_map:
				print("can't find question:{} in question_map".format(question))
				err_cnt += 1
				continue
			orig_answer = question_map[question]
			orig_answer = orig_answer.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
			fq.write(question + "\n")
			fr.write(answer + "\n")
			fa.write(orig_answer + "\n")
		print("total error cnt:{}".format(err_cnt))




if __name__ == "__main__":
	print("start!")
	logging.info("start to extract")
	# loop(FQ_NAME, FA_NAME, NEED_CLASSIFY)
	
	print("start to get question_map")
	question_map = get_question_map(FIN_EXCEL_NAME)
	print("start to produce training corpus")
	generate_training_corpus("retrieval_output_file_20171031173249.txt", question_map)
	print("finish extracting!")
	logging.info("finish extracting!")
