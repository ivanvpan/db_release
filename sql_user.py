import sys
import argparse
import logging
from datetime import datetime

import dbif
from history import HistoryManager
import util


EXIT_NORMAL = 0 # when program is done exit with this
EXIT_FAIL = 1 # when a critical error occured
EXIT_LOGIN = 13 # could not login

def setup_logging(log_file, verbosity=logging.ERROR):
    '''Creates four loggers: 'all', 'plain', 'file', 'console'.

    'all' - formatted output to console and file
    'plain' - unformatted to console and file
    'file', 'console' - formatted to individual outputs
    '''
    plain_format = logging.Formatter('%(message)s')

    filelog = logging.FileHandler(log_file, 'a')
    filelog.setLevel(logging.DEBUG)
    filelog_format = logging.Formatter('@[ %(asctime)s ]>[ %(name)s ]: %(message)s', '%m-%d-%Y %H:%M')
    filelog.setFormatter(filelog_format)

    console = logging.StreamHandler()
    console.setLevel(verbosity)
    console_format = logging.Formatter('SQLUSER: %(message)s')
    console.setFormatter(console_format)

    console_plain = logging.StreamHandler()
    console_plain.setLevel(verbosity)
    console_plain.setFormatter(plain_format)

    filelog_plain = logging.FileHandler(log_file, 'a')
    filelog_plain.setLevel(logging.DEBUG)
    filelog_plain.setFormatter(plain_format)

    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('all').addHandler(console)
    logging.getLogger('all').addHandler(filelog)
    logging.getLogger('plain').addHandler(filelog_plain)
    logging.getLogger('plain').addHandler(console_plain)
    logging.getLogger('file').addHandler(filelog)
    logging.getLogger('console').addHandler(console)

def log_(logger, msg, level):
    '''Log to a specified logger. Break strings into lines. Accepts lists.'''
    if type(msg) is str or type(msg) is list:
        if type(msg) is str:
            lines = msg.splitlines()
        elif type(msg) is list:
            lines = msg

        for line in lines:
            logger.log(level, line)
    else:
        logger.log(level, msg)

def log(msg, level=logging.INFO):
    log_(logging.getLogger('all'), msg, level)

def log_file(msg, level=logging.INFO):
    log_(logging.getLogger('file'), msg, level)

def log_console(msg, level=logging.INFO):
    log_(logging.getLogger('console'), msg, level)

def log_plain(msg, level=logging.INFO):
    log_(logging.getLogger('plain'), msg, level)

def log_script_changes(diff, sub_diff):
    if diff[0] == HistoryManager.DIFF:
        log('Main script file has changed. The diff is:')
        log_plain(diff[1])

    if sub_diff[HistoryManager.NEW]:
        log('Some subscripts are new:')
        for sub in sub_diff[HistoryManager.NEW]:
            log(sub + ' is new.')

    if sub_diff[HistoryManager.DIFF]:
        log('Some subscripts have changed,:')
        for sub in sub_diff[HistoryManager.DIFF]:
            log(sub[0] + ' has changed. The diff is:')
            log_plain(sub[1])


def parse_args(args):
    '''Run argparse on args and return the result.'''
    parser = argparse.ArgumentParser(description='Run a SQL script file on a server')
    parser.add_argument('username')
    parser.add_argument('password')
    parser.add_argument('database')
    parser.add_argument('sql_file')

    #COMPLETE
    parser.add_argument('-x', '--extra', default='',
                        help='Specify arguments to the SQL script.')
    parser.add_argument('-q', '--quite', dest='log_level', action='store_const',
                        const=logging.CRITICAL, default=logging.ERROR,
                        help='Show only critical output.')
    parser.add_argument('-v', '--verbose', dest='log_level', action='store_const',
                        const=logging.DEBUG, help='Show lots of output.')
    parser.add_argument('-D', '--dbms', metavar='DBMS', default='oracle',
                        help='Name of the DBMS that you are connecting to. Default is \'oracle\'.')
    parser.add_argument('-l', '--log', dest='logfile', default='sql_out.log',
                        help='Allows to specify the log file; the default is \'sql_out.txt\'')
    parser.add_argument('-r', '--record', dest='recdir', default='complete',
                        help='Specify the directory for execution records. Default is \'complete\'')
    parser.add_argument('-d', '--diag', dest='test_run', action='store_true',
                        help='Does a diagnostic check on the script and all ' \
                        'scripts it calls instead of running the script.')
    parser.add_argument('-m', '--manual', action='store_true',
                        help='Gives control over the SQL*Plus session directly ' \
                        'to the user instead of running the script. (Not necessary to specify sql_file.)')
    parser.add_argument('-i', '--ignore', dest='ignore_history', action='store_true',
                        help='Ignore execution record.')
    parser.add_argument('-s', '--show', action='store_true',
                        help='Show SQL*Plus output from running the script and ' \
                        'ask for confirmation before continuing.')
    parser.add_argument('-n', '--noquery', action='store_true',
                        help='Automatically execute files that are new or have changed, ' \
                        'but skip completed non-chaged files')
    parser.add_argument('-H', '--host', metavar='host', default='',
                        help='Connect to a specific host.')
    parser.add_argument('-T', '--timeout', metavar='T', type=int, default=30,
                        help='User input timeout, default to 30 secs.')
    return parser.parse_args(args)

def test_run(args):
        errors = False
        for handler in logging.getLogger('console').handlers:
            handler.setLevel(logging.DEBUG)
        log('## Doing a test run. ##')

        if not util.file_exists(args.sql_file):
            errors = True
            log('ERROR: SQL script file ' + args.sql_file + ' does not exist.', logging.ERROR)
            log('Exiting...')
            sys.exit(EXIT_FAIL)
        else:
            log('Found script file ' + args.sql_file)

        # make sure all subscripts exist
        log('## Looking for subscripts')
        try:
            subs = util.find_subscripts(args.sql_file, args.dbms)
        except IOError, e:
            errors = True
            log('ERROR: One of the subscripts does not exist. Opening it threw and exception: ', logging.ERROR)
            log(e, logging.ERROR)
            sys.exit(EXIT_FAIL)

        log('Found %d subscripts' % (len(subs),))

        #####
        # Check execution history and subscripts
        hist = HistoryManager(args.sql_file, args.database, args.username, path=args.recdir)
        log('\n## Checking execution record ##')
        status = hist.status(args.sql_file, subs)
        if status == HistoryManager.DIFF:
            log('# Some of the files have changed since last run.')
            diff = hist.diff(args.sql_file)
            sub_diff = hist.diff_all(subs)
            log_script_changes(diff, sub_diff)
        elif status == HistoryManager.NO_DIFF:
            log('# The main script and subscripts have not changed since last run.')
        elif status == HistoryManager.NEW:
            log('# This script has not been executed yet.')

        #####
        # Try connecting to DB
        try:
            log('\n## Trying to connect to db ##')
            db = dbif.create_interface(args.dbms, logger=logging.getLogger('plain'))
            if not db.connect(args.username, args.password, args.database, args.host):
                errors = True
                log('Was not able to connect to DB with these credentials: '
                    'user=%s, pass=%s, db=%s' % (args.username, '------', args.database), logging.ERROR)
            errors = db.dequeue_errors()
            if errors:
                errors = True
                log('Errors during execution were:')
                log(errors)
        except Exception, e:
            log('ERROR: ' + str(e))
            errors = True
        finally:
            if db and db.connected():
                db.exit()

        if errors:
            log('\nTHERE WERE ERRORS DURING EXECUTION')
            log('Please review the above log for details. Goodbye.')
        return errors


def main(argv):
    '''Where magick happens.'''
    args = parse_args(argv[1:])
    setup_logging(args.logfile, args.log_level)
    log('\n### SQL_USER.PY: dbname=' + args.database + ' user=' \
            + args.username + ' sqlfile=' + args.sql_file + ' host=' + args.host + ' ###')
    #log('### Arguments: ' + ' '.join(argv[1:]))

    # make sure main file exists
    if not util.file_exists(args.sql_file):
        log('ERROR: The file ' + args.sql_file + ' does not exist. Exiting.', logging.ERROR)
        sys.exit(EXIT_FAIL)

    # make sure all subscripts exist
    try:
        subs = util.find_subscripts(args.sql_file, args.dbms)
    except IOError, e:
        log('ERROR: One of the subscripts does not exist. Opening it threw and exception: ', logging.ERROR)
        log(e, logging.ERROR)
        sys.exit(EXIT_FAIL)


    if args.test_run:
        errors = test_run(args)
        if errors:
            sys.exit(EXIT_FAIL)
        else:
            sys.exit(EXIT_NORMAL)
        # EXIT

    if args.manual:
        log('## Entering manual control of SQL client. Press \'ESC\' to quit. ##')
        db = dbif.create_interface(args.dbms, logging.getLogger('all'))
        res = db.run_manual(args.username, args.password, args.database)
        if res:
            log('Manual control of client relinquished.')
            sys.exit(EXIT_NORMAL)
        else:
            log('SQL client exited/killed by user action.')
            sys.exit(EXIT_NORMAL)
            # EXIT

    # setup history
    hist = HistoryManager(args.sql_file, args.database, args.username, path=args.recdir)

    #### Check history
    if not args.ignore_history:
        log('## Checking execution record ##')
        status = hist.status(args.sql_file, subs)
        if status == HistoryManager.DIFF:
            log('# Some of the files have changed since last run.')
            diff = hist.diff(args.sql_file)
            sub_diff = hist.diff_all(subs)
            log_script_changes(diff, sub_diff)

            if not args.noquery:
                log_console('Should we execute the script ' + args.sql_file + ' again?', logging.CRITICAL)
                choice = util.query_user('yn', args.timeout)
                if choice == 'y':
                    log('# Re-running the script ' + args.sql_file + ' per user request.')
                elif choice == 'n':
                    log('# Skipping the script ' + args.sql_file + ' per user request.')
                    sys.exit(EXIT_NORMAL) # EXIT
                else:
                    log('Failed to get user input. Exiting.')
                    sys.exit(EXIT_FAIL) # EXIT
            else:
                log('Not querying user due to -n or --noquery')
        elif status == HistoryManager.NO_DIFF:
            log('# The main script and subscripts have not changed since last run. Exiting.', logging.CRITICAL)
            sys.exit(EXIT_NORMAL) # EXIT
        elif status == HistoryManager.NEW:
            log('# First time running this SQL file. Proceed normally.')
    else:
        log('## Ignoring execution record due to an -i or --ignore flag')


    ### Run the DB client
    try:
        db = dbif.create_interface(args.dbms, logging.getLogger('plain'))
        if not db.connect(args.username, args.password, args.database, args.host):
            log('Was not able to connect to DB with these credentials: '
                'user=%s, pass=%s, db=%s' % (args.username, '------', args.database), logging.ERROR)
            errors = db.dequeue_errors()
            if errors:
                log('Errors encountered: ', logging.ERROR)
                log_plain(errors, level=logging.ERROR)
            sys.exit(EXIT_LOGIN)
            # EXIT

        # check if client exited while executing the script
        str_time = datetime.today().strftime('[%H:%M:%S]')
        log('\n%s Running file: %s' % (str_time, args.sql_file), logging.CRITICAL)
        not_EOF = db.exec_sql_file(args.sql_file, args.extra)
        if not not_EOF:
            log('Reached end of file (EOF). This probably means the script exited on its own.')
            errors = db.dequeue_errors()

            if errors:
                log('Also, errors were encountered: ', logging.ERROR)
                log_plain(errors, level=logging.ERROR)

            log_console('# Unexpected EOF. You can rollback and quit with failure(f), '
                         + 'or continue with push and not rollback (p)', logging.CRITICAL)
            choice = util.query_user('pf', args.timeout)
            if choice == 'p':
                log('# Rolling back the changes.')
                if db.connected():
                    db.rollback()
                sys.exit(EXIT_FAIL) # EXIT
            elif choice == 'f':
                log('# User wants things as is, so not rolling back.')
                sys.exit(EXIT_NORMAL) # EXIT
            else:
                log('# Failed to get user input. Exiting.')
                db.rollback()
                sys.exit(EXIT_FAIL) # EXIT

        do_record = True
        errors = db.dequeue_errors()
        if args.show or errors:
            if errors:
                log('Errors were encountered during execution: ', logging.CRITICAL)
                log_plain(errors, level=logging.ERROR)
                log('Errors were encountered during execution: ')

            if args.show:
                log('--show mode was specified, so you can review the output and do any of the following: ')

            log_console('You can: \n' +
                        '(c) continue execution, but do not record as completed\n' +
                        '(i) continue , record as completed\n' +
                        #'(t) switch to manual control of the DB client\n' +
                        #'(q) rollback, exit with failure, but record execution as completed\n' +
                        '(x) rollback and exit with failure', logging.CRITICAL)
            choice = util.query_user('cix', args.timeout)
            if choice == 'c':
                log('# Continuing, not recording.')
                do_record = False
            elif choice == 'i':
                log('# Continuing normally.')
#            elif choice == 't':
#                log('# Switching to manual control of the client')
#                log('  Press ESC to return to automation and complete the push')
#                alive = db.attach()
#                if alive:
#                    log('Manual control relinquished, returning to automation.')
#                else:
#                    log('Child process exited/killed by user action')
#                    sys.exit(EXIT_NORMAL) # EXIT
#            elif choice == 'q':
#                log('# Rolling back the changes, assuming failure, recording as complete.')
#                if db.connected():
#                    db.rollback()
#                hist.record(args.sql_file)
#                hist.record(subs)
#                sys.exit(EXIT_FAIL) # EXIT
            elif choice == 'x':
                log('# Rolling back the changes, assuming failure, not recording.')
                if db.connected():
                    db.rollback()
                sys.exit(EXIT_FAIL) # EXIT
            else:
                log('Failed to get user input. Exiting.')
                db.rollback()
                sys.exit(EXIT_FAIL) # EXIT
        db.commit()
        if do_record:
            log('# Recording history')
            hist.record(args.sql_file)
            hist.record(subs)
        log('# Execution of script ' + args.sql_file + ' completed. Changes commited.')
    except Exception, e:
        log('Exception raised.')
        log(e)
        if db and db.connected():
            db.rollback()
        raise
    finally:
        if db and db.connected():
            db.exit()


if __name__ == '__main__':
    main(sys.argv)
