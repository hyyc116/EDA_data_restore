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
        content = re.sub(r'<.*?>',' ',content).replace('\\n',' ')
        content = re.sub(r'\s+',' ',content)
        ## 如果有美元符号
        if '$' in content:
            salary =  extract_salary(content)
            if salary.strip()=='':
                si = content.index('$')
                print f+"==="+content[si-100:si+100]
            else:
                print f+"=="+salary
        ## 有dollor的说明
        elif 'dollor' in content:
            print content

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

        num,scale = match_salary(salary_str.lower())
        
        if num is not None and salary is not None:
            # print jid+"\t"+num+"\t"+scale
            salary = convert_to_hour(num,scale)
            print jid+"\t"+str(salary)



def match_salary(salary_str):

    ## 第一种也是最典型的第一种， $12,000.00 / year
    regrex = re.compile('\\$?(\d+\,?\d*\.?\d*) ?\/ ?(\w+)')
    ss = []
    for s in regrex.findall(salary_str):
        ss.append(s)
    if len(ss)!=0:
        ## 认为一个salary的前后是相差不大的，所以无论如何变幻，只取第一个
        ## 每一行包括两个值，一个是数字一个是scale
        return ss[0]

    ## match $14 - $15/year , $14-15/year
    regrex = re.compile('\\$\d+\,?\d*\.?\d* ?\- ?\\$?(\d+\,?\d*\.?\d*) ?\/ ?(\w+)')
    for s in regrex.findall(salary_str):
        ss.append(s)

    if len(ss)!=0:
        return ss[0]

    ## match "$13 per hour, per week, per month, per year" 
    regrex = re.compile('\\$?(\d+\,?\d*\.?\d*)\s?per\s?(\w+)')
    for s in regrex.findall(salary_str):
        ss.append(s)

    if len(ss)!=0:
        return ss[0]

    ## match " $12 a week, a month, a year, an hour"
    regrex = re.compile('\\$?(\d+\,?\d*\.?\d*) an? (\w+)')
    for s in regrex.findall(salary_str):
        ss.append(s)

    if len(ss)!=0:
        return ss[0]

    ## match 13 hourly weekly monthly, anually, 
    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) monthly')
    for s in regrex.findall(salary_str):
        ss.append([s[0],'month'])

    if len(ss)!=0:
        return ss[0]

    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) yearly')
    for s in regrex.findall(salary_str):
        ss.append([s[0],'year'])

    if len(ss)!=0:
        return ss[0]

    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) hourly')
    for s in regrex.findall(salary_str):
        ss.append([s[0],'hour'])

    if len(ss)!=0:
        return ss[0]

    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) weekly')
    for s in regrex.findall(salary_str):
        ss.append([s[0],'week'])

    if len(ss)!=0:
        return ss[0]

    regrex = re.compile('\\$(\d+\,?\d*\.?\d*) anually')
    for s in regrex.findall(salary_str):
        ss.append([s[0],'year'])

    if len(ss)!=0:
        return ss[0]

    if len(ss)==0:
        ## match $14 - $15
        regrex = re.compile('\\$\d+\,?\d*\.?\d* ?\- ?\\$?(\d+\,?\d*\.?\d*)')
        for s in regrex.findall(salary_str):
            ss.append([s[0],'anoni'])

        if len(ss)!=0:
            return ss[0]

    if 'salary' in salary_str:
        regrex = re.compile('\\$?(\d+\,?\d*\.?\d*)')
        for s in regrex.findall(salary_str):
            ss.append([s[0],'anoni'])


        if len(ss)!=0:
            return ss[0]

    return None,None

def convert_to_hour(salary,scale):

    if scale in ['hour','hr','hours','hourl'] or scale.startswith('sessi'):
        return float(salary.replace(',',''))
    elif scale in ['week'] or scale.startswith('weekl'):
        return float(salary.replace(',',''))/40
    elif scale in ['day','night']:
        return float(salary.replace(',',''))/8
    elif scale in ['month','mo']:
        return float(salary.replace(',',''))/40/4
    elif scale=='anoni':
        ##根据大小定义是时薪
        salary = float(salary.replace(',',''))
        if salary < 300:
            return salary
        elif salary < 1600:
            return salary/40
        elif salary < 10000:
            return salary/40/4
        else:
            return salary/52/40
    elif scale in ['year','annua','yr'] or scale.startswith('year'):
        return float(salary.replace(',',''))/52/40

    else:
        logging.info("None parsed: {:},{:}".format(salary,scale))






if __name__ == '__main__':
    # date_count_statistic()
    # salary()
    # parse()

    # salary_str = '$19.32 hourly hour DESCRIPTION OF DUTIES:    $15.18 - $19.09 '
    # salary_str = re.sub(r'<.*?>',' ',salary_str).replace('\\n',' ')
    # salary_str = re.sub(r'\s+',' ',salary_str)
    # for salary in match_salary(salary_str.lower()):
    #     print salary

    clean_salary(sys.argv[1])    