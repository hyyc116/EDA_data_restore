#coding:utf-8

import sys
import json
import os
from collections import defaultdict
from db_util import *

def check_data_duplicate():
    _db = MySQLdb.connect("localhost","root","hy123","indeed_city")
    sql = 'select title,company,location,summary,publishdate from job'
    _cursor = _db.cursor()

    rows = set([])
    progress = 0
    _cursor.execute(sql)
    for title,company,location,summary,publishdate in _cursor:
        progress+=1

        if progress%10000==0:
            logging.info('progress {:}, length of rows:{:} ...'.format(progress,len(rows)))

        rows.add('{:},{:},{:},{:},{:}'.format(title,company,location,summary,publishdate))

if __name__ == '__main__':
	check_data_duplicate()



