# -*- coding: UTF-8 -*-
# date: 2017-8-2
# author: gaoyi

import os,re
import time
import openpyxl
import logging

logging.basicConfig(filename="merge_log.txt", level="DEBUG", format="%(asctime)s %(levelname)s %(message)s")

FIN_LOGS = u"D:/01WORK/03WorkPlan/2017年8月2日 金融知识库/知识库faq.xlsx"
FOUT_EXCEL_NAME = u"D:/01WORK/03WorkPlan/2017年8月2日 金融知识库/knowledge_base.xlsx"

def merge(original_file, output_excel_fname):
	wb = openpyxl.Workbook()
	ws = wb.active
	original_wb = openpyxl.load_workbook(original_file)
	worksheet = original_wb.active
	line_cnt = 0
	for row in worksheet.rows:
		line_cnt += 1
		if line_cnt == 1:
			continue
		if line_cnt % 1000 == 0:
			print("processing lines:{}".format(line_cnt))
		row_value = [cell.value for cell in row]
		question = row_value[0]
		category = row_value[1]
		answer = ''.join([str(v) for v in row_value[2:] if v is not None])
		ws.append([question, category, answer])
	wb.save(filename=output_excel_fname)
	logging.info("saving knowledge_base into file:{}".format(output_excel_fname))



if __name__ == "__main__":
	print("start!")
	logging.info("start to merge")
	merge(FIN_LOGS, FOUT_EXCEL_NAME)
	print("finish merging!")
	logging.info("finish merging!")
