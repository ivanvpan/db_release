import re
import pexpect
import logging
import sys
import unittest
import os


class DisconnectedException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
    def __repr__(self):
        return self.__str__()


class _CommandInterface:
    PXP_LOGFILE = 'dbif_pexpect.log'
    def __init__(self, logger=None, rawlog='dbif_pexpect.log'):
        self.PXP_LOGFILE = rawlog
        self.SPAWN_CMD = ''
        self.SPAWN_WITH_HOST_CMD = ''
        self.SET_PROMPT_CMD = ''
        self.EXEC_SCRIPT_CMD = ''
        self.EXIT_CMD = ''
        self.ERROR_PATTERN = ''
        self.LOGIN_ERROR = ''
        self.LOGIN_SUCCESS = ''

        self.prompt = ''
        self._connected = False
        self._errors = []
        self._child = None

        if logger:
            self._logger = logger
        else:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('[%(name)s]: %(message)s'))
            self._logger = logging.getLogger(self.__class__.__name__)
            self._logger.setLevel(logging.INFO)
            self._logger.addHandler(handler)

    def run_manual(self, user, passwd, dbname, host='', exit=True):
        '''Spawns the sql client process in interfactive mode, returns True is
        all is well false if the client terminated.'''
        self._child = pexpect.spawn(self._spawn_cmd(user, passwd, dbname, host))
        try:
            self._child.interact('\x1b')
        except OSError: # the user quit manualy
            pass

        if self._child.isalive():
            self.exit()
            return True
        else:
            return False

    def attach(self):
        '''Does the same thing as run_manual, but on an already connected
        interface. Returns True if the client is still running.'''
        self._child.setecho(True)
        try:
            self._child.interact('\x1b')
        except OSError:
            self._logger.error('Terminated, but next time simply press ESC.')

        if self._child.isalive():
            self._child.setecho(False)
            return True
        else:
            return False


    def connect(self, user, passwd, dbname, host=''):
        '''Connect to the database and return True if connected, False otherwise'''
        self._logger.info('Spawning the following command:' + self._spawn_cmd(user, '------', dbname, host))
        self._child = pexpect.spawn(self._spawn_cmd(user, passwd, dbname, host))
        self._child.logfile = open(self.PXP_LOGFILE, 'w')
        self._child.setecho(False)
        result = self._child.expect([self.LOGIN_SUCCESS, self.LOGIN_ERROR, pexpect.EOF], timeout=10)
        self._find_errors()

        if result > 0:
            return False
        self._connected = True

        # set_prompt also consumes all the garbage until first good prompt, except for mysql
        self._set_prompt(user, dbname)
        self._prepare_env()
        return True

    def exec_cmd(self, cmd, expect_patterns=None, timeout=None):
        '''Execute a command. If expect_patterns not specified
        defaults to the prompt. Returns pattern index.'''
        if not self._child or not self._child.isalive():
            raise DisconnectedException('Cannot send commands through an unconnected interface')
        if not expect_patterns:
            expect_patterns = [self.prompt]
        self._child.sendline(cmd)
        pat_num = self._child.expect(expect_patterns, timeout=timeout)
        self._find_errors()
        return pat_num

    def exec_sql_file(self, sql_file, args=''):
        '''Execute a script file. Optionally pass arguments to it.
        Return False EOF was hit, True otherwise.'''
        cmd = self._script_exec_cmd(sql_file, args)
        result = self.exec_cmd(cmd, [pexpect.EOF, self.prompt])
        return result == 1

    def dequeue_errors(self):
        '''Get all the errors since last expect call. If the errors
        have already been dequeued returns an empty list.'''
        if self._errors:
            errs = self._errors
            self._errors = []
            return errs
        else:
            return self._errors

    def rollback(self):
        '''Rollback changes'''
        self._logger.info('# Rolling back changes.')
        self.exec_cmd('rollback;')

    def commit(self):
        '''Commit changes.'''
        self._logger.info('# Commiting changes.')
        self.exec_cmd('commit;')

    def exit(self):
        '''Send an exit command, if client does not exit shut it down by force'''
        self._logger.info('Terminating...')
        if self._child and self._child.isalive():
            # ask to quit politely, but with timeout
            self.exec_cmd(self.EXIT_CMD, [pexpect.EOF, pexpect.TIMEOUT], timeout=3)
            if self._child.isalive():
                self._child.terminate(True)

    def connected(self):
        '''Check if the spawned process is alive'''
        return self._child and self._child.isalive() and self._connected

    def _find_errors(self):
        '''Look at the child's "before" attribute and look at lines that have errors'''
        before = self._child.before
        if before:
            self._logger.critical(before)
        lines = before.split('\n')
        #print 'lines: ', lines
        #print 'after: ', self._child.after
        self._errors = [l for l in lines if re.match(self.ERROR_PATTERN, l)]

    def _spawn_cmd(self, user, passwd, dbname, host=''):
        '''Generate a command that spawns the client with the username/password'''
        if host:
            return self.SPAWN_WITH_HOST_CMD % dict(user=user,
                                           passwd=passwd,
                                           dbname=dbname,
                                           host=host)
        else:
            return self.SPAWN_CMD % dict(user=user, passwd=passwd, dbname=dbname)

    def _set_prompt(self, user, dbname):
        '''Sets the tool's prompt to something meaningful'''
        self.prompt = '%(user)s.%(dbname)s> ' % dict(user=user, dbname=dbname)
        self.exec_cmd(self.SET_PROMPT_CMD % dict(prompt=self.prompt))

    def _script_exec_cmd(self, sql_file, args=''):
        '''Generate a command that executed a script file'''
        return self.EXEC_SCRIPT_CMD % dict(sql_file=sql_file, args=args)

    def _prepare_env(self):
        '''Set environment variables, to pretty up the output, etc.'''
        pass


#    def _escape_string(self, string):
#        '''Take a string and put quotes around it escaping all special characters'''
#        pass
#    def sql_select(self, column, table, **where_args):
#        '''select('column', id=0, name='bob') would run "select column where id=0, name='bob'",
#        and return the value of that column'''
#        self.exec_cmd(self._select_stmt(column, table, **where_args))
#        return self._child.before
#
#    def sql_insert(self, table, values):
#        '''select('column', id=0, name='bob') would run "select column where id=0, name='bob'",
#        and return the value of that column'''
#        self.exec_cmd(self._insert_stmt(table, values))
#        return self._child.before
#    def _select_stmt(self, column, table, **where_args):
#        '''_select_stmt('column', id=0, name='bob') would return
#        "SELECT column FROM table WHERE id=0, name='bob'".'''
#        where = ''
#        if where_args:
#            for k,v in where_args.iteritems():
#                if type(v).__name__ == 'str':
#                    where_args[k] = self._escape_string(v)
#            where = ' WHERE ' + ', '.join([k + '=' + str(v) for k,v in where_args.iteritems()])
#        stmt = 'SELECT ' + column + ' FROM ' + table + where + ';'
#        return stmt
#
#    def _insert_stmt(self, table, values):
#        '''_insert_stmt('table', [0, 'bob', 'jonson']) would return
#        "INSERT into table VALUES(0, 'bob', 'johnson')".'''
#        values_str = ''
#        if values:
#            for i, v in enumerate(values):
#                if type(v).__name__ == 'str':
#                    values[i] = self._escape_string(v)
#            values_str = ' VALUES(' + ', '.join(values) + ')'
#        stmt = 'INSERT INTO ' + table + values_str + ';'
#        return stmt


class OracleInterface(_CommandInterface):

    def __init__(self, logger=None, rawlog='ora_client.log'):
        _CommandInterface.__init__(self, logger, rawlog=rawlog)
        self.SPAWN_CMD = 'sqlplus %(user)s/%(passwd)s@%(dbname)s'
        # Oracle must use tnsnames
        self.SPAWN_WITH_HOST_CMD = 'sqlplus %(user)s/%(passwd)s@%(dbname)s'
        self.SET_PROMPT_CMD = 'set sqlp "%(prompt)s"'
        self.EXEC_SCRIPT_CMD = '@%(sql_file)s %(args)s'
        self.EXIT_CMD = 'exit'
        self.ERROR_PATTERN = r'^.*(ORA|SP\d+)-\d*:.*$'
        self.LOGIN_ERROR = 'Enter user-name:'
        self.LOGIN_SUCCESS = 'Connected to:'
        self.prompt = 'SQL> ' # we'll change this one later
#    def _prepare_env(self):
#        self.exec_cmd('SET DEFINE OFF')
#
#    def _escape_string(self, string):
#        return '\'' + string.replace('\'', '\'\'') + '\''


class PostgreInterface(_CommandInterface):

    def __init__(self, logger=None, rawlog='postgre_client.log'):
        _CommandInterface.__init__(self, logger, rawlog=rawlog)
        self.SPAWN_CMD = 'psql %(dbname)s %(user)s -w'
        self.SPAWN_WITH_HOST_CMD = 'psql -h %(host)s %(dbname)s %(user)s -w'
        self.SET_PROMPT_CMD = '\\set PROMPT1  \'%(prompt)s\''
        self.EXEC_SCRIPT_CMD = '\\i %(sql_file)s'
        self.EXIT_CMD = '\\q'
        self.ERROR_PATTERN = r'^.*ERROR:.*$'
        self.LOGIN_ERROR = 'authentication failed'
        self.LOGIN_SUCCESS = 'Type "help" for help.'
        self.prompt = 'sql=> ' # we'll change this one later

    def connect(self, user, passwd, dbname, host=''):
        os.putenv('PGPASSWORD', passwd)
        return _CommandInterface.connect(self, user, passwd, dbname, host)

    def _prepare_env(self):
        self.exec_cmd('\\pset pager') # turn off paging

class MysqlInterface(_CommandInterface):

    def __init__(self, logger=None, rawlog='mysql_client.log'):
        _CommandInterface.__init__(self, logger, rawlog=rawlog)
        self.SPAWN_CMD = 'mysql -u %(user)s -p%(passwd)s %(dbname)s'
        self.SPAWN_WITH_HOST_CMD = 'mysql -h %(host)s -u %(user)s -p%(passwd)s %(dbname)s'
        self.SET_PROMPT_CMD = ''
        self.EXEC_SCRIPT_CMD = '\\. %(sql_file)s'
        self.EXIT_CMD = '\\q'
        self.ERROR_PATTERN = r'^.*ERROR \d*.*$'
        self.LOGIN_ERROR = 'ERROR 1045'
        self.LOGIN_SUCCESS = '' # set in connect
        self.prompt = 'sql=> ' # we'll change this one later

    def connect(self, user, passwd, dbname, host=''):
        self.prompt = '%(user)s.%(dbname)s> ' % dict(user=user, dbname=dbname)
        os.putenv('MYSQL_PS1', self.prompt)

        # we know the prompt ahead of time, so we can wait for it on connect
        self.LOGIN_SUCCESS = self.prompt
        return _CommandInterface.connect(self, user, passwd, dbname, host)

    def _set_prompt(self, user, dbname):
        '''mysql cannot set a prompt from within the client'''
        pass

class VerticaInterface(_CommandInterface):

    def __init__(self, logger=None, rawlog='vertica_client.log'):
        _CommandInterface.__init__(self, logger, rawlog=rawlog)
        self.SPAWN_CMD = '/opt/vertica/bin/vsql -U %(user)s -w %(passwd)s -d %(dbname)s'
        self.SPAWN_WITH_HOST_CMD = '/opt/vertica/bin/vsql -h %(host)s -U %(user)s -w %(passwd)s -d %(dbname)s'
        self.SET_PROMPT_CMD = '\\set PROMPT1  \'%(prompt)s\''
        self.EXEC_SCRIPT_CMD = '\\i %(sql_file)s'
        self.EXIT_CMD = '\\q'
        self.ERROR_PATTERN = r'^.*(ERROR|ROLLBACK):.*$'
        self.LOGIN_ERROR = 'Invalid username or password'
        self.LOGIN_SUCCESS = 'Welcome to vsql'
        self.prompt = 'sql=> ' # we'll change this one later

    def exec_cmd(self, cmd, expect_patterns=[], timeout=None):
        '''Execute a command.

        If expect_patterns not specified defaults to the prompt. Returns
        pattern index. This function had to be customized for vertica, because
        vsql constantly deletes the command line and then prints it over,
        so prompt matches once for every character thats put in'''
        if not self._child.isalive():
            raise DisconnectedException('Cannot send commands through an unconnected interface')
        if not expect_patterns:
            expect_patterns = [self.prompt]
        self._child.sendline(cmd)
        # it tries to compensate for vertica doing a lot of control character work.
        # seems to work most of the time.
        self._child.expect([cmd[len(cmd) - 1] + '\n\r', pexpect.TIMEOUT], timeout=timeout)
        pat_num = self._child.expect(expect_patterns, timeout=None)
        self._find_errors()
        return pat_num

#    def _escape_string(self, string):
#        return '\'' + string.replace('\'', '\'\'') + '\''

VALID_DBS = ['oracle', 'vertica', 'postgresql', 'mysql']

def create_interface(dbms, logger=None, rawlog=None):
    if (rawlog):
        param = (logger, rawlog)
    else:
        param = (logger,)

    if not dbms in VALID_DBS:
        return None
    if dbms == 'oracle':
        return OracleInterface(*param)
    elif dbms == 'vertica':
        return VerticaInterface(*param)
    elif dbms == 'postgresql':
        return PostgreInterface(*param)
    elif dbms == 'mysql':
        return MysqlInterface(*param)



class _TestInterface(unittest.TestCase):
    def setUp(self):
        self.SQL_SCRIPT_FNAME = 'sql_test_input321.sql'
        try:
            script_file = open(self.SQL_SCRIPT_FNAME, 'w')
            script_file.write('select none from nothingg;\n')
        finally:
            script_file.close()

    def testConnected(self):
        self.assert_(self.iface.connected())

    def testExecCmd(self):
        self.iface.exec_cmd('select none from nothing;')
        errors = self.iface.dequeue_errors()
        self.assert_(errors)

    def testExecScript(self):
        self.iface.exec_sql_file(self.SQL_SCRIPT_FNAME)
        errors = self.iface.dequeue_errors()
        self.assert_(errors)

    def tearDown(self):
        os.system('rm ' + self.SQL_SCRIPT_FNAME)
        self.iface.exit()

class _TestOracle(_TestInterface):
    def setUp(self):
        _TestInterface.setUp(self)
        self.iface = OracleInterface()
        self.assert_(self.iface.connect('user', 'pass', 'db'))

    def testGoodCmd(self):
        self.iface.exec_cmd('select * from dual;')
        errors = self.iface.dequeue_errors()
        self.assert_(not errors)

class _TestVertica(_TestInterface):
    def setUp(self):
        _TestInterface.setUp(self)
        self.iface = VerticaInterface()
        self.assert_(self.iface.connect('user', 'pass', 'db'))

    def testGoodCmd(self):
        self.iface.exec_cmd('select * from v_catalog.dual_p;')
        errors = self.iface.dequeue_errors()
        self.assert_(not errors)

class _TestPostgre(_TestInterface):
    def setUp(self):
        _TestInterface.setUp(self)
        self.iface = OracleInterface()
        self.assert_(self.iface.connect('user', 'pass', 'db'))

    def testGoodCmd(self):
        self.iface.exec_cmd('select 1;')
        errors = self.iface.dequeue_errors()
        self.assert_(not errors)

def _oracle_suite():
    return unittest.makeSuite(_TestOracle)

def _vertica_suite():
    return unittest.makeSuite(_TestVertica)


def main(args):
    '''Runs tests and exits with exit code 0 if all is well,
    and 1 if a test failed. Also returns 1 if DBMS name parameter
    is missing or incorrect'''
    if len(args) < 2:
        print 'Please provide DBMS name as a test argument'
        sys.exit(1)

    dbname = args[1]
    if not dbname in VALID_DBS:
        print 'You are attempting to test an unsupported database, we only do these: ', \
                ', '.join(VALID_DBS)
        sys.exit(1)

    if dbname == 'oracle':
        suite = _oracle_suite()
    elif dbname == 'vertica':
        suite = _vertica_suite()

    verbosity = 1
    if '-v' in args:
        verbosity = 2
    result = unittest.TextTestRunner(verbosity=verbosity).run(suite)
    if not result.wasSuccessful():
        sys.exit(1)
    sys.exit(0)

if __name__ == '__main__':
    main(sys.argv)
