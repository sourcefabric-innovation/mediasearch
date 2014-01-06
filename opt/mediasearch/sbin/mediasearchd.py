#!/usr/bin/env python
#
# Mediasearch
#

import sys, os, time, atexit, signal, logging
import urlparse, argparse
import pwd, grp

MEDIASEARCH_DBNAME = 'mediasearch'
MEDIASEARCH_DEBUG = False

WEB_ADDRESS = 'localhost'
WEB_PORT = 9020
WEB_USER = 'www-data'
WEB_GROUP = 'www-data'

LOG_LEVEL = logging.WARNING
TO_DAEMONIZE = False

PID_PATH = ''
LOG_PATH = ''

HOME_DIR = '/tmp'
LOG_SERVER_NAME = 'mediasearchd'
IMPORT_DIRS = ['/opt/mediasearch/lib', '/opt/mediasearch/local/site-packages', '/opt/mediasearch/local/dist-packages']

parser = argparse.ArgumentParser()
parser.add_argument('-n', '--database', help='mediasearch database name')
parser.add_argument('-b', '--debug_mode', help='to run it in debug mode', action='store_true')

parser.add_argument('-a', '--web_address', help='web address to listen at')
parser.add_argument('-p', '--web_port', help='web port to listen at', type=int, default=WEB_PORT)
parser.add_argument('-u', '--web_user', help='web server user')
parser.add_argument('-g', '--web_group', help='web server group')

parser.add_argument('-v', '--verbose', help='increase log verbosity', action='store_true')
parser.add_argument('-d', '--daemonize', help='daemonize the server', action='store_true')

parser.add_argument('-i', '--pid_path', help='pid file path')
parser.add_argument('-l', '--log_path', help='log file path')

parser.add_argument('-s', '--install_dir', help='installation directory', default='/opt/mediasearch/')

args = parser.parse_args()

if args.database:
    MEDIASEARCH_DBNAME = args.database
if args.debug_mode:
    MEDIASEARCH_DEBUG = True

if args.web_address:
    WEB_ADDRESS = args.web_address
if args.web_port:
    WEB_PORT = int(args.web_port)
if args.web_user:
    WEB_USER = args.web_user
if args.web_group:
    WEB_GROUP = args.web_group

if args.verbose:
    LOG_LEVEL = logging.INFO
if args.daemonize:
    TO_DAEMONIZE = True

if args.pid_path:
    PID_PATH = args.pid_path
if args.log_path:
    LOG_PATH = args.log_path

install_dir = '/'
if args.install_dir:
    install_dir = args.install_dir
    if not install_dir.endswith('/'):
        install_dir += '/'
    IMPORT_DIRS = [install_dir + 'lib', install_dir + 'local/site-packages', install_dir + 'local/dist-packages']

if TO_DAEMONIZE:
    if not PID_PATH:
        PID_PATH = install_dir + 'var/run/mediasearchd.pid'
    if not LOG_PATH:
        LOG_PATH = install_dir + 'var/log/mediasearchd.log'

def daemonize(work_dir, pid_path):
    UMASK = 022

    if (hasattr(os, 'devnull')):
       REDIRECT_TO = os.devnull
    else:
       REDIRECT_TO = '/dev/null'

    try:
        pid = os.fork()
    except OSError, e:
        logging.error('can not daemonize: %s [%d]' % (e.strerror, e.errno))
        sys.exit(1)

    if (pid != 0):
        os._exit(0)

    os.setsid()
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    try:
        pid = os.fork()
    except OSError, e:
        logging.error('can not daemonize: %s [%d]' % (e.strerror, e.errno))
        sys.exit(1)

    if (pid != 0):
        os._exit(0)

    try:
        os.chdir(work_dir)
        os.umask(UMASK)
    except OSError, e:
        logging.error('can not daemonize: %s [%d]' % (e.strerror, e.errno))
        sys.exit(1)

    try:
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(REDIRECT_TO, 'r')
        so = file(REDIRECT_TO, 'a+')
        se = file(REDIRECT_TO, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
    except OSError, e:
        logging.error('can not daemonize: %s [%d]' % (e.strerror, e.errno))
        sys.exit(1)

    if pid_path is None:
        logging.warning('no pid file path provided')
    else:
        try:
            fh = open(pid_path, 'w')
            fh.write(str(os.getpid()) + '\n')
            fh.close()
        except Exception:
            logging.error('can not create pid file: ' + str(pid_path))
            sys.exit(1)

def set_user(user_id, group_id, pid_path):

    if (user_id is not None) and (str(user_id) != '0'):
        if (pid_path is not None) and os.path.exists(pid_path):
            try:
                os.chown(pid_path, user_id, -1)
            except OSError, e:
                logging.warning('can not set pid file owner: %s [%d]' % (e.strerror, e.errno))

    if group_id is not None:
        try:
            os.setgid(group_id)
        except Exception as e:
            logging.error('can not set group id: %s [%d]' % (e.strerror, e.errno))
            sys.exit(1)

    if user_id is not None:
        try:
            os.setuid(user_id)
        except Exception as e:
            logging.error('can not set user id: %s [%d]' % (e.strerror, e.errno))
            sys.exit(1)

def cleanup():

    logging.info('stopping the ' + LOG_SERVER_NAME + ' web server')

    pid_path = PID_PATH
    if pid_path:
        try:
            fh = open(pid_path, 'w')
            fh.write('')
            fh.close()
        except Exception:
            logging.warning('can not clean pid file: ' + str(pid_path))

        if os.path.isfile(pid_path):
            try:
                os.unlink(pid_path)
            except Exception:
                pass

    logging.shutdown()
    os._exit(0)

def exit_handler(signum, frame):

    cleanup()

def run_server(dbname, web_address, web_port, to_debug):

    logging.info('starting the ' + LOG_SERVER_NAME + ' web server')

    from mediasearch.app.run import run_flask
    run_flask(dbname, web_address, web_port, to_debug)

if __name__ == "__main__":
    atexit.register(cleanup)

    signal.signal(signal.SIGTERM, exit_handler)
    signal.signal(signal.SIGINT, exit_handler)

    if LOG_PATH:
        logging.basicConfig(filename=LOG_PATH, level=LOG_LEVEL, format=LOG_SERVER_NAME + ': %(levelname)s [%(asctime)s] %(message)s')
    else:
        logging.basicConfig(level=LOG_LEVEL, format=LOG_SERVER_NAME + ': %(levelname)s [%(asctime)s] %(message)s')

    if TO_DAEMONIZE:
        daemonize(HOME_DIR, PID_PATH)

        try:
            user_info = pwd.getpwnam(WEB_USER)
            user_id = int(user_info.pw_uid)
        except:
            logging.error('can not find the web user')
            sys.exit(1)

        try:
            group_info = grp.getgrnam(WEB_GROUP)
            group_id = int(group_info.gr_gid)
        except:
            logging.error('can not find the web group')
            sys.exit(1)

        set_user(user_id, group_id, PID_PATH)

    if IMPORT_DIRS:
        for imp_dir in IMPORT_DIRS:
            sys.path.insert(0, imp_dir)

    try:
        run_server(MEDIASEARCH_DBNAME, WEB_ADDRESS, WEB_PORT, MEDIASEARCH_DEBUG)
    except Exception as exc:
        logging.error('can not start the ' + LOG_SERVER_NAME + ' web server: ' + str(exc))
        sys.exit(1)

