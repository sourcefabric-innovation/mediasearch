#!/usr/bin/env python
#
# Mediasearch
#

class MongoDBs(object):
    def __init__(self, dbname=''):
        self.dbname = dbname
        self.db = None

    def set_dbname(self, dbname):
        self.dbname = dbname

    def get_dbname(self):
        return self.dbname

    def set_db(self, db):
        self.db = db

    def get_db(self):
        return self.db

mongo_dbs = MongoDBs()

