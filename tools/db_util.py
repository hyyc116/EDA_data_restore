#coding:utf-8

import MySQLdb
from MySQLdb import cursors
import sys
from collections import defaultdict
import re
import codecs
import random as rn
import logging
#logging file
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',level=logging.DEBUG)

class dbop:

    def __init__(self,insert_index=0,isSS=False):
        
        if isSS:
            logging.debug("connect database with normal SScursor.")
            self._db = MySQLdb.connect("localhost","root","hy123","EDA_DATA",cursorclass = cursors.SSCursor)
        else:
            logging.debug("connect database with normal cursor.")
            self._db = MySQLdb.connect("localhost","root","hy123","EDA_DATA")    
        self._cursor = self._db.cursor()
        
        self._insert_index=insert_index
        self._insert_values=[]


    def query_database(self,sql):
        self._cursor.close()
        self._cursor = self._db.cursor()
        self._cursor.execute(sql)
        logging.debug("query database with sql {:}".format(sql))
        return self._cursor

    def insert_database(self,sql,values):
        self._cursor.close()
        self._cursor = self._db.cursor()
        self._cursor.executemany(sql,values)
        logging.debug("insert data to database with sql {:}".format(sql))
        self._db.commit()
        

    def batch_insert(self,sql,row,step,is_auto=True,end=False):
        if end:
            if len(self._insert_values)!=0:
                logging.info("insert {:}th data into database,final insert.".format(self._insert_index))
                self.insert_database(sql,self._insert_values)
        else:
            self._insert_index+=1
            if is_auto:
                row[0] = self._insert_index
            self._insert_values.append(tuple(row))
            if self._insert_index%step==0:
                logging.info("insert {:}th data into database".format(self._insert_index))
                self.insert_database(sql,self._insert_values)
                self._insert_values=[]

    def get_insert_count(self):
        return self._insert_index

    def execute_del_update(self,sql):
        self._cursor.execute(sql)
        self._db.commit()
        logging.debug("execute delete or update sql {:}.".format(sql))

    def execute_sql(self,sql):
        self._cursor.execute(sql)
        self._db.commit()
        logging.debug("execute sql {:}.".format(sql))

    def insert_sql(self,sql,values):
        self._cursor.execute(sql,values)
        cid =  int(self._db.insert_id())
        self._db.commit()
        return cid

    def close_db(self):
        self._db.close()

