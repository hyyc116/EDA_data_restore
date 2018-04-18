#coding:utf-8

import sys
import os
from db_util import *
from collections import defaultdict
import re
# from bs4 import BeautifulSoup as BS


## mapping location to MSA
def trans_location(location):
    location = re.sub(r'\d+','===',location)
    return location.split("===")[0]

## statistic of 
def date_count_statistic():
    sql = 'select id,title,company,location,publishdate from job'

    query_op = dbop()
    cursor = query_op.query_database(sql)

    location_count = defaultdict(int)
    for row in cursor:
        jid,title,company,location,publishdate = row
        location = trans_location(location)
        location_count[location]+=1


    for location in location_count.keys():
        print location,location_count[location]

def parse():
    for f in os.listdir('../Pages'):
        content = open('../Pages/'+f).read().strip().replace("\\u002F",'/')

        if '$' in content:
            salary =  extract_salary(content)
            if salary.strip()=='':
                si = content.index('$')
                print f+"==="+content[si-10:si+100]
            else:
                print f+"=="+salary

def extract_salary(content):
    regex=re.compile("\$\d+,?\d*\.?\d*\s?\-?t?o?\s?\$?\d+,?\d*\.?\d*\s?\/\s?\w{0,5}")
    ss = []
    for s in regex.findall(content):
        ss.append(s)
    return ','.join(ss)

def clean_salary(path):
    for line in open(path):
        line = line.strip()
        jid,salary_str = line.split('\t')

        regrex = re.compile('\\$(\d+\,?\d*\.?\d*) \/(\w+)')
        ss = []
        for s in regrex.findall(salary_str):
            ss.append(s)

        regrex = re.compile('\\$(\d+\.?\d*) per (\w+) ')
        for s in regrex.findall(salary_str):
            ss.append(s)
        
        if len(ss)==0:
            print line





if __name__ == '__main__':
    # date_count_statistic()
    # salary()
    # parse()

    clean_salary(sys.argv[1])

    















