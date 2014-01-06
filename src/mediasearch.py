#!/usr/bin/env python

MEDIASEARCH_DBNAME = 'mediasearch'
MEDIASEARCH_HOST = 'localhost'
MEDIASEARCH_PORT = 9020
MEDIASEARCH_DEBUG = True

import os, sys, datetime, json, logging
try:
    from mediasearch.app.run import run_flask
except:
    logging.error('Mediasearch library is not installed')
    os._exit(1)

run_flask(MEDIASEARCH_DBNAME, MEDIASEARCH_HOST, MEDIASEARCH_PORT, MEDIASEARCH_DEBUG)

