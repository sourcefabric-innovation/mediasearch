#!/usr/bin/env python
#
# Mediasearch
#

import logging
import fcntl

class Sync(object):
    def __init__(self, lockfile=''):
        self.lockfile = lockfile
        self.fh = None

    def prepare(self, lockfile=''):
        if lockfile:
            self.lockfile = lockfile

        if not self.lockfile:
            logging.warning('no lock file specified')
            return False

        try:
            if self.fh:
                self.fh.close()
                self.fh = None
        except:
            self.fh = None

        try:
            self.fh = open(self.lockfile, 'w')
        except:
            logging.error('can not open lock file: ' + str(self.lockfile))
            return False

        return True

    def clean(self):
        if self.fh:
            try:
                self.fh.close()
            except:
                return False

        self.fh = None
        self.lockfile = lockfile


        return True

    def lock(self):
        if not self.fh:
            return False

        try:
            fcntl.lockf(fh.fileno(), fcntl.LOCK_EX)
        except:
            return False

        return True

    def unlock(self):
        if not self.fh:
            return False

        try:
            fcntl.lockf(fh.fileno(), fcntl.LOCK_UN)
        except:
            return False

        return True

synchronizer = Sync()

def sync_clean():
    try:
        synchronizer.clean()
    except:
        pass

