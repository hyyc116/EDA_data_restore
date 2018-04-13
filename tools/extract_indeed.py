#coding:utf-8
'''
	Tasks in this file:

		1. extract salaries from content, and convert all salaries to hourly income.
		2. extract position from content
		3. extract NP from title of job
'''
import re
import sys
import os
from db_util import *

def extract_position_from_title(title):

    pass

def extract_salary_from_content(content):

    content = content.strip().replace("\\u002F",'/')

    if '$' in content:
        salary =  extract_salary(content)
        if salary.strip()=='':
            si = content.index('$')
            return content[si-10:si+100]
        else:
            return salary

def extract_salary(content):
    regex=re.compile("\$\d+,?\d*\.?\d*\s?\-?t?o?\s?\$?\d+,?\d*\.?\d*\s?\/\s?\w{0,5}")
    ss = []
    for s in regex.findall(content):
        ss.append(s)
    return ','.join(ss)


def extract_NP_from_content():

    pass


def extract_salary_from_folder(folder):
	folder = folder if folder.endswith('/') else folder+'/'
	progress = 0
	for filename in os.listdir(folder):
		filepath = folder+filename
		content = open(filepath).read().strip()
		salary = extract_salary_from_content(content)
		progress+=1

		if progress%10000==0:
			logging.info('progress {:} ...'.format(progress))

		if salary is None:
			continue


		print filename[:-5]+'\t'+salary



if __name__ == '__main__':
	label = sys.argv[1]
	if label=='extract_salary':
		extract_salary_from_folder(sys.argv[2])












