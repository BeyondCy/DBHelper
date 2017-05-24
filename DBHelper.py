#!/usr/bin/env python
# -*-coding:utf8-*-
# author = felicitychou
# date = 2017/4/17
# support python 3

import pymongo
import sqlite3
import psycopg2
import psycopg2.extras
import logging
import configparser

class Logger(object):

    def __init__(self, logname = "log.txt", loglevel = logging.DEBUG, loggername = "logger", \
                format='%(asctime)s %(filename)s: %(levelname)s %(message)s',datefmt='%Y/%m/%d %H:%M:%S'):

        self.logger = logging.getLogger(loggername)
        self.logger.setLevel(loglevel)
        formatter = logging.Formatter(format,datefmt)
        self.log2file(logname=logname,loglevel=loglevel,formatter=formatter)
        self.log2print(loglevel=loglevel,formatter=formatter)
    
    def log2file(self,logname,loglevel,formatter):
        # log to file
        fh = logging.FileHandler(logname)
        fh.setLevel(loglevel)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def log2print(self,loglevel,formatter):
        # print log
        ch = logging.StreamHandler()
        ch.setLevel(loglevel)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)


class PostgreSQL(object):

    def __init__(self,cfgpath = 'cfg/PostgreSQL.cfg'):

        self._readconf(cfgpath = cfgpath)
        self.logger = Logger(logname=self.logname).logger
    
    # read config
    def _readconf(self,cfgpath):
        config = configparser.ConfigParser().read(cfgpath)
        conf = config[os.path.splitext(os.path.basename(cfgpath))[0]]
        self.logname = conf['log']
        self.connconf = {}
        for key in ('host','port','user','passwd','dbname'):
            self.connconf[key] = conf[key]

    # connect database
    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.connconf)
            #self.cur = self.conn.cursor()
            #create Dict Cursor return result (dict)
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except Exception as e:
            self.logger.exception(e)
            self.closeconn()

    def closeconn(self):
        if self.conn.closed:
            pass
        else:
            self.conn.closed()

    def _execute(self,sql):
        try:
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("%s:%s" % ('Success',self.cur.mogrify(sql)))
        except Exception as e:
            self.conn.rollback()
            self.logger.error("%s:%s" % ('Failure',self.cur.mogrify(sql)))
            self.logger.exception(e)
                    
    def insert(self,tablename,vals):
        sql = r'INSERT INTO %s (%s) VALUES (%s);' % (tablename,','.join(vals.keys()),','.join(['%s']*len(vals)))
        self._execute(sql=sql,vals=)

    def select(self,tablename,cols=['*',]):
        sql = r'SELECT %s from %s;' % (','.join(cols),tablename)
        try:
            self._execute(sql)
        except Exception as e:
            raise e

