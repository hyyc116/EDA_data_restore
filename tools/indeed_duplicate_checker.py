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
        if len(splits)<2:
            continue

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
            if pos.strip()=='' or len(pos.strip())<4 or hasnum(pos):
                continue

            for word in pos.split():
                if len(word)<4:
                    continue
                jobwords.append(word.lower())
            
            # jobwords.append(pos.lower())
    job_counter = Counter(jobwords)

    lines = []
    job_words = []
    for job in sorted(job_counter.keys(),key=lambda x:job_counter[x],reverse=True):
        if job_counter[job]<100:
            continue
        lines.append('{:}\t{:}'.format(job,job_counter[job]))
        job_words.append(job)

    open('data/job_words.txt','w').write('\n'.join(lines))

    job_words = set(job_words)
    ### 如果NP中包含job word，那么将这个position作为该条记录的pos，并且将job的类型设置为job words
    job_pos_type = []
    for jid in jid_position.keys():
        poses = jid_position[jid].split(',')

        job_pos = None
        job_type = None
        word_index = -1
        for pos in poses:
            pos = pos.lower()

            for i,word in enumerate(pos.split()):
                if word in job_words and i>word_index:
                    job_pos = pos
                    job_type = word
                    word_index = i


        if job_pos is not None:
            job_pos_type.append('{:}\t{:}\t{:}'.format(jid,job_pos,job_type))

    open('data/job_pos_type.txt','w').write('\n'.join(job_pos_type))

    logging.info('Done')


def hasnum(inputString):
    return any(char.isdigit() for char in inputString)





###将所有条目的location，parse成state city的样式，存在zipcode
def parse_location(location):
    city_state  = location.strip().replace(', ','===').split()[0]
    return city_state




### 解析每一个job所需要的skill,假设一个job需要多个技能
def parse_skill(path):
    skill_list = []
    for line in open(path):
        line = line.strip()
        jid,skills = line.split('\t')

        ## 首先统计高频出现的NP

        for skill in skills.split(','):
            if hasnum(skill) or len(skill)<4:
                continue

            skill_list.append(skill.lower())

    skill_counter = Counter(skill_list)

    lines = []
    skill_counter = []
    for skill in sorted(skill_counter.keys(),key=lambda x:skill_counter[x],reverse=True):
        if skill_counter[skill]<100:
            continue
        lines.append('{:}\t{:}'.format(skill,skill_counter[skill]))
        skill_counter.append(skill)

    open('data/skill_counter.txt','w').write('\n'.join(lines))

if __name__ == '__main__':
    # check_data_duplicate()

    # parse_position(sys.argv[1])

    parse_skill(sys.argv[1])



