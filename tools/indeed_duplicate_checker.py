#coding:utf-8

import sys
import json
import os
from collections import Counter
from db_util import *

### 存在同一公司在不同时间发布同样内容的情况，这种情况存在30多万条
def check_data_duplicate():



    _db = MySQLdb.connect("localhost","root","hy123","indeed_city")
    sql = 'select id,title,company,location,summary,publishdate from job'
    _cursor = _db.cursor()

    # rows = set([])
    progress = 0
    _cursor.execute(sql)
    for jid,title,company,location,summary,publishdate in _cursor:
        progress+=1

        if progress%10000==0:
            logging.info('progress {:}, length of rows:{:} ...'.format(progress,len(rows)))

        # rows.add('{:},{:},{:},{:}'.format(title,company,location,summary))
        jid = jid.strip()
        city_sate  = parse_location(location)

## 读取已经parse出来的title中的Np，并且根据
def parse_position(path):
    logging.info('loading jid positions ....')
    jid_position = {}
    for line in open(path):
        line = line.strip()

        if line=='':
            continue
            
        splits= line.split('\t')
        jid = splits[0]
        position = ','.join(splits[1:])
        jid_position[jid] = position


    logging.info('number of jobs {:} ..'.format(len(jid_position.keys())))

    jobwords = []
    ## 首先根据出现的词的频率来确定job类型
    for jid in jid_position.keys():
        ##position中可能存在多个NP，那么和进行选择
        poses= jid_position[jid].split(',')

        for pos in poses:
            # for word in pos.split():
            #     jobwords.append(word.lower())
            jobwords.append(pos.lower())
    job_counter = Counter(jobwords)

    lines = []
    for job in sorted(job_counter.keys(),key=lambda x:job_counter[x],reverse=True):
        lines.append('{:}\t{:}'.format(job,job_counter[job]))

    open('data/job_words.txt','w').write('\n'.join(lines))






###将所有条目的location，parse成state city的样式，存在zipcode
def parse_location(location):
    city_state  = location.strip().replace(', ','===').split()[0]
    return city_state





if __name__ == '__main__':
    # check_data_duplicate()

    parse_position(sys.argv[1])



