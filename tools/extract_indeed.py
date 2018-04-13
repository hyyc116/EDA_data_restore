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
from patterns import chunk_file
from multiprocessing.dummy import Pool as ThreadPool
import MySQLdb
from patterns import chunk_content

def extract_positions(extracted_ids_path):

    logging.info('Extracted file from {:} ...'.format(extracted_ids_path))
    extracted_ids = set([line.strip().split('\t')[0] for line in open(extracted_ids_path)])

    _db = MySQLdb.connect("localhost","root","hy123","EDA_DATA")
    sql = 'select id,title from job'
    _cursor = _db.cursor()

    params = []
    _cursor.execute(sql)
    for jid,title in _cursor:
    	if jid in extracted_ids:
    		continue
    	params.append((jid,title))
    _cursor.close()
    _db.close()

    pool = ThreadPool(10)
    pool.map(chunk_content, params)

    logging.info('DONE')


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


def extract_skill_from_folder(folder,extracted_file_path,start=0, end=-1, worker=20):
    
    logging.info("folder path:{:}, start from {:} to end {:} with {:} workers".format(folder,start,end,worker))
    extracted_files = set([line.strip().split('\t')[0] for line in open(extracted_file_path)])

    filelist = []
    folder = folder if folder.endswith('/') else folder+'/'
    for filename in os.listdir(folder):
    	if filename[:-5] in extracted_files:
    		filepath = None
    	else:
        	filepath = folder+filename
        filelist.append(filepath)

    lines = []
    pool = ThreadPool(worker)
    pool.map(chunk_file, filelist[start:end])
    logging.info('done')

def has_skills(content):
	content = content.lower()

	keywords = ['experience','skill','technology','requirement']

	for keyword in keywords:
		if keyword in content:
			return True

	return False


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
	elif label=='extract_skill':
		folder = sys.argv[2]
		start  = int(sys.argv[3])
		end = int(sys.argv[4])
		workers = int(sys.argv[5])
		exsiting_file = sys.argv[6]
		extract_skill_from_folder(folder,exsiting_file,start,end,workers)
	elif label=='extract_position':
		extract_positions(sys.argv[2])
	else:
		logging.info('No such operations!')












