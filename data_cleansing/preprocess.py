#!/usr/bin/env python
# -*- coding:utf-8 -*-

LENGTH_LIMIT = 30
FILE_INPUT = "answer_20170823173713.txt"
FILE_OUTPUT = "answers.txt"

answers_set = set()

with open(FILE_INPUT, encoding="utf-8") as fi, open(FILE_OUTPUT, mode="w", encoding="utf-8") as fo:
    for l in fi:
        l = l.strip()
        if l not in answers_set:
            if len(l) <= LENGTH_LIMIT:
                answers_set.add(l)
    print("total lines:{}".format(len(answers_set)))
    for cnt, l in enumerate(answers_set):
        line = l + "##seperator##" + str(cnt) + "\n"
        fo.write(line)
    print("finish")
