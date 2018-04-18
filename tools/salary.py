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
        salary_str = re.sub(r'<.*?>',' ',salary_str).replace('\\n',' ')
        salary_str = re.sub(r'\s+',' ',salary_str)
        ## match $15/year $15 /hr
        tag,ss = match_salary(salary_str.lower())
        # print tag , ss
        if len(ss)==0:
            print line


def match_salary(salary_str):

    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) ?\/ ?(\w+)')
    ss = []
    for s in regrex.findall(salary_str):
        ss.append(s)

    if len(ss)!=0:
        return 'one',ss

    ## match $14 - $15/year , $14-15/year
    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) ?\- ?\\$?(\d+\,?\d*\.?\d*) ?\/(\w+)')
    for s in regrex.findall(salary_str):
        ss.append(s)

    if len(ss)!=0:
        return 'two',ss

    ## match 13 per hour
    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) per (\w+) ?\,?\.?')
    for s in regrex.findall(salary_str):
        ss.append(s)

    if len(ss)!=0:
        return 'three',ss

    ## match 13 per hour
    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) monthly')
    for s in regrex.findall(salary_str):
        ss.append(s)

    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) yearly')
    for s in regrex.findall(salary_str):
        ss.append(s)

    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) hourly')
    for s in regrex.findall(salary_str):
        ss.append(s)

    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) weekly')
    for s in regrex.findall(salary_str):
        ss.append(s)

    if len(ss)!=0:
        return 'three',ss

    if len(ss)==0:
        ## match $14 - $15
        regrex = re.compile('\\$(\d+\,?\d*\.?\d*) ?\- ?\\$?(\d+\,?\d*\.?\d*)')
        for s in regrex.findall(salary_str):
            ss.append(s)

    return 'four',ss

if __name__ == '__main__':
    # date_count_statistic()
    # salary()
    # parse()

    # salary_str = '$10,000.00 to $15,000.00 /year  $19.32 - $28.62 per hour</p>\n<p><b>\nDESCRIPTION OF DUTIES:  $25-$50/hr,$30-60/hour   $15.18 - $19.09 $4,159/Month,$4,159/Month'
    # salary_str = re.sub(r'<.*?>',' ',salary_str).replace('\\n',' ')
    # salary_str = re.sub(r'\s+',' ',salary_str)
    # for salary in match_salary(salary_str):
    #     print salary

    clean_salary(sys.argv[1])    















