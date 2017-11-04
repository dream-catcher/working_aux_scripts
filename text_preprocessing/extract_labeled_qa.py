# -*- coding: UTF-8 -*-
# date: 2017-8-2
# author: Emerson

import os,re
import time
import openpyxl
import logging

logging.basicConfig(filename="merge_log.txt", level="DEBUG", format="%(asctime)s %(levelname)s %(message)s")
USE_STD_CORPUS = True
USE_PERSONATE_CORPUS = True
NEED_CLASSIFY = False
NO_INCORRECT_CHAR = True

FIN_EXCEL_NAME = u"D:/Downloads/standard_qa_2017-8-26.xlsx"
PERSONATE_EXCEL_FILE = u"D:/Downloads/personate_qa_2017-8-26.xlsx"

filename_suffix = time.strftime("_%Y%m%d.txt", time.localtime())
if NEED_CLASSIFY:
	filename_suffix = "c" + filename_suffix
FQ_NAME = u"D:/01WORK/q" + filename_suffix
FA_NAME = u"D:/01WORK/a" + filename_suffix

def extract_std(original_file, need_classify = False):
	original_wb = openpyxl.load_workbook(original_file)
	worksheet = original_wb.active
	for line_cnt, row in enumerate(worksheet.rows):
		if line_cnt == 0:
			continue
		if line_cnt % 1000 == 0:
			print("processing lines:{}".format(line_cnt))
		row_value = [cell.value for cell in row]
		if NO_INCORRECT_CHAR:
			question_list = [row_value[2], row_value[5], row_value[6], row_value[7], row_value[8]]
		else:
			question_list = [row_value[2], row_value[5], row_value[6], row_value[7], row_value[8], row_value[9]]
		answer = row_value[3]
		classify = row_value[1]
		if not answer:
			print("answer can't be empty")
			continue
		for q in question_list:
			if not q:
				continue
			if need_classify:
				if not classify:
					print("classify can't be empty")
					continue
				yield q,classify,answer
			else:
				yield q,answer

def extract_personate(excel_file, need_classify = False):
	original_wb = openpyxl.load_workbook(excel_file)
	worksheet = original_wb.active
	for line_cnt, row in enumerate(worksheet.rows):
		if line_cnt == 0:
			continue
		if line_cnt % 1000 == 0:
			print("processing lines:{}".format(line_cnt))
		row_value = [cell.value for cell in row]
		question = row_value[1]
		answer = row_value[2]
		classify = row_value[3]
		if not all([question, answer]):
			print("question answer can't be empty")
			continue
		if need_classify:
			if not classify:
				print("classify can't be empty")
				continue
			yield question, classify, answer
		else:
			yield question, answer

def write_parallel(fq, fa, elements, need_classify):
	if not all(elements):
		print("question,classify,answer can't be empty:{}".format(elements))
		return 
	suffix = ""
	if need_classify:
		q,c,a = elements
		suffix = "#SEP#" + c
	else:
		q,a = elements
	question = ''.join(str(q).strip().split("\n"))
	fq.write(question + suffix + "\n")
	answer = ''.join(str(a).strip().split("\n"))
	fa.write(answer + "\n")

def loop(fq_name, fa_name, use_std=True, use_personate=True, need_classify=False):
	total_cnt = 0
	std_cnt = 0
	p_cnt = 0
	if use_std:
		with open(fq_name, "w") as fq, open(fa_name, "w") as fa:
			for elements in extract_std(FIN_EXCEL_NAME, need_classify):
				std_cnt += 1
				write_parallel(fq, fa, elements, need_classify)
			print("std number:{}".format(std_cnt))
	if use_personate:
		with open(fq_name, "a") as fq, open(fa_name, "a") as fa:
			for elements in extract_personate(PERSONATE_EXCEL_FILE, need_classify):
				p_cnt += 1
				write_parallel(fq, fa, elements, need_classify)
		print("personate number:{}".format(p_cnt))
	total_cnt = std_cnt + p_cnt
	print("total count is:{}".format(total_cnt))

if __name__ == "__main__":
	print("start!")
	logging.info("start to extract")
	loop(FQ_NAME, FA_NAME, USE_STD_CORPUS, USE_PERSONATE_CORPUS, NEED_CLASSIFY)
	print("finish extracting!")
	logging.info("finish extracting!")
