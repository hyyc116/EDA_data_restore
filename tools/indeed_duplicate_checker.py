#coding:utf-8

import sys
import json
import os
from collections import Counter
from db_util import *

### 存在同一公司在不同时间发布同样内容的情况，这种情况存在30多万条
# def check_data_duplicate():
def export_indeed_data():
    ## 首先加载jid salary
    jid_salary = {}

    for line in open('extracted_salary.txt'):
        jid,salary = line.split('\t')

        if salary is None or salary.strip()=='None':
        	continue
        # print salary
        jid_salary[jid.strip()] = float(salary)

    ## 加载 jid pos的文件
    jid_pos_type = {}
    for line in open('data/job_pos_type.txt'):
        line = line.strip()
        jid,pos,pt = line.split('\t')

        jid_pos_type[jid.strip()] = [pos,pt]

    _db = MySQLdb.connect("localhost","root","hy123","indeed_city")
    sql = 'select id,title,company,location,summary,publishdate from job'
    _cursor = _db.cursor()

    # rows = set([])
    progress = 0
    _cursor.execute(sql)
    jid_attrs = {}
    # lines = []
    for jid,title,company,location,summary,publishdate in _cursor:
        progress+=1

        if progress%10000==0:
            logging.info('progress {:}, length of rows:{:} ...'.format(progress,len(rows)))

        # rows.add('{:},{:},{:},{:}'.format(title,company,location,summary))
        jid = jid.strip()
        city,state  = parse_location(location)

        jid_attrs[jid]=[jid,company,city,state,publishdate]

    logging.info('saving data ...')
    ##只用已经抽出去pos的职位
    lines = ['id,company,city,state,publishdate,position,postype,salary']
    for jid in jid_pos_type.keys():
    	pos,pt = jid_pos_type[jid]
    	salary = jid.salary.get(jid,'NONE')
    	attrs = jid_attrs[jid]
    	attrs.append(pos)
    	attrs.append(pt)
    	attrs.append(salary)
    	lines.append(','.join(attrs))

   	open('data/indeed_data.txt','w').write('\n'.join(lines))

   	logging.info('DONE')



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
            job_pos_type.append('{:}\t{:}\t{:}'.format(jid,job_pos.replace('\t',''),job_type))

    open('data/job_pos_type.txt','w').write('\n'.join(job_pos_type))

    logging.info('Done')


def hasnum(inputString):
    return any(char.isdigit() for char in inputString)





###将所有条目的location，parse成state city的样式，存在zipcode
def parse_location(location):
    city,state  = location.strip().replace(', ','===').split()[0].split('===')
    return city,state




### 解析每一个job所需要的skill,假设一个job需要多个技能
def parse_skill(path):
    word_dict = defaultdict(list)
    for line in open(path):
        line = line.strip()
        splits  = line.split('\t')
        if len(splits)<2:
            continue
        jid = splits[0]
        skills = ','.join(splits[1:])

        ## 首先统计高频出现的NP
        skill_words = []
        for skill in skills.split(','):
            if hasnum(skill) or len(skill)<4:
                continue

            for skill_word in skill.split():
                skill_words.append(skill_word.lower())
        sc = Counter(skill_words)
        for word in sc.keys():
            word_dict[word].append([jid,sc[word]])

    jid_word_tfidf = defaultdict(dict)
    for word in word_dict.keys():
        df = word_dict[word]
        for jid,freq in df:
            tfidf = freq/float(len(df))


            jid_word_tfidf[jid][word]=tfidf

    lines = []

    for jid in jid_word_tfidf.keys():
        word_tfidf = jid_word_tfidf[jid]
        for word in sorted(word_tfidf.keys(),key=lambda x:word_tfidf[x],reverse=True)[:5]:
            lines.append('{:}\t{:}\t{:}'.format(jid,word,word_tfidf[word]))

    open('data/job_word_tfidf.txt','w').write('\n'.join(lines))
    


if __name__ == '__main__':
    # check_data_duplicate()

    parse_position(sys.argv[1])

    # parse_skill(sys.argv[1])

    export_indeed_data()



