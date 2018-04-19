#coding:utf-8

import sys
import json
import os
from collections import Counter
from db_util import *

### 存在同一公司在不同时间发布同样内容的情况，这种情况存在30多万条
def check_data_duplicate():



    _db = MySQLdb.connect("localhost","root","hy123","indeed_city")
    sql = 'select id,title,company,location,summary,publishdate from     '
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


    logging.info('number of     s {:} ..'.format(len(jid_position.keys())))

        words = []
    ## 首先根据出现的词的频率来确定    类型
    for jid in jid_position.keys():
        ##position中可能存在多个NP，那么和进行选择
        poses= jid_position[jid].split(',')

        for pos in poses:
            if pos.strip()=='' or len(pos.strip())<4 or hasnum(pos):
                continue

            for word in pos.split():
                if len(word)<4:
                    continue
                    words.append(word.lower())
            
            #     words.append(pos.lower())
        _counter = Counter(    words)

    lines = []
        _words = []
    for      in sorted(    _counter.keys(),key=lambda x:    _counter[x],reverse=True):
        if     _counter[    ]<100:
            continue
        lines.append('{:}\t{:}'.format(    ,    _counter[    ]))
            _words.append(    )

    open('data/    _words.txt','w').write('\n'.join(lines))

        _words = set(    _words)
    ### 如果NP中包含     word，那么将这个position作为该条记录的pos，并且将    的类型设置为     words
        _pos_type = []
    for jid in jid_position.keys():
        poses = jid_position[jid].split(',')

            _pos = None
            _type = None
        word_index = -1
        for pos in poses:
            pos = pos.lower()

            for i,word in enumerate(pos.split()):
                if word in     _words and i>word_index:
                        _pos = pos
                        _type = word
                    word_index = i


        if     _pos is not None:
                _pos_type.append('{:}\t{:}\t{:}'.format(jid,    _pos,    _type))

    open('data/    _pos_type.txt','w').write('\n'.join(    _pos_type))

    logging.info('Done')


def hasnum(inputString):
    return any(char.isdigit() for char in inputString)





###将所有条目的location，parse成state city的样式，存在zipcode
def parse_location(location):
    city_state  = location.strip().replace(', ','===').split()[0]
    return city_state




### 解析每一个    所需要的skill,假设一个    需要多个技能
def parse_skill(path):
    skills = []
    for line in open(path):
        line = line.strip()
        jid,skills = line.split('\t')

        ## 首先统计高频出现的NP

        for skill in skills.split(','):
            if hasnum(skill) or len(skill)<4:
                continue

            skills.append(skill)

    skill_counter = Counter(skills)

    lines = []
    skill_Nps = []
    for skill in sorted(skill_counter.keys(),key=lambda x:skill_counter[x],reverse=True):
        if skill_counter[skill]<100:
            continue
        lines.append('{:}\t{:}'.format(skill,skill_counter[skill]))
        skill_Nps.append(skill)

    open('data/skill_Nps.txt','w').write('\n'.join(lines))

if __name__ == '__main__':
    # check_data_duplicate()

    parse_position(sys.argv[1])



