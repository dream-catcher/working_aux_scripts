# -*- coding: UTF-8 -*-
# date: 2017-7-30
# author: gaoyi
import re
import json

# FIN = "chitchat_corpus_2017-07-28_172725.txt"
FIN = "chitchat_corpus_2017-07-28_174327.txt"
FOUT_SRC = "sources.txt"
FOUT_TGT = "targets.txt"

LIMIT_LEN = 100
        
def process_to_unigram(query, query_len_limit):
    """
    process chinese query into unigram split sentences
    english word reserved as one word.
    white space is for delimiter
    """
    if query is None or query == "":
        return ""
    char_list = list(query)
    result = []
    temp = []
    for c in char_list:
        if re.match("[#0-9a-zA-Z_-]", c):
            temp.append(c)
        else:
            if len(temp) > 0:
                result.append(''.join(temp).strip())
                temp = []
            result.append(c.strip())
    start = len(result) - query_len_limit
    start = start if start > 0 else 0
    sent_len = len(result[start:])
    if sent_len > query_len_limit:
        print("{}:{}".format(sent_len, result[start:]))
    ret = ' '.join(result[start:])
    if len(ret.split(' ')) > query_len_limit:
        print("split:{}:{}".format(sent_len, result[start:]))
    return ret

if __name__ == "__main__":
    qa_pairs = set()
    with open(FIN) as fi, open(FOUT_SRC, "w") as f_src, open(FOUT_TGT, "w") as f_tgt:
        for cnt, l in enumerate(fi):
            if cnt % 1000 == 0:
                print("processing lines:{}".format(cnt))
            line = json.loads(l)
            src = line['question']
            target = line['answer']
            qa = src + "$$$" + target
            if qa not in qa_pairs:
                qa_pairs.add(qa)
                f_src.write(process_to_unigram(src, LIMIT_LEN) + "\n")
                f_tgt.write(process_to_unigram(target, LIMIT_LEN) + "\n")