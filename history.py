import os

import subprocess as subp

from util import file_exists

class HistoryManager:
    FS_BACKEND = 'FS'
    NEW = 'NEW'
    NO_DIFF = 'NO_DIFF'
    DIFF = 'DIFF'

    def __init__(self, script_file, dbname, schema, backend='FS', path='complete'):
        self._writer = None
        if (backend == HistoryManager.FS_BACKEND):
            self._writer = _FSWriter(script_file, dbname, schema, path)
        else:
            self._writer = _FSWriter(script_file, dbname, schema, path)

    def status(self, script, subs):
        '''Check the file and all its subscripts, and say whether
        anything has changed. Returns NEW, NO_DIFF or DIFF'''
        diff = self.diff(script)
        sub_diff = self.diff_all(subs)
        if diff[0] == HistoryManager.NO_DIFF \
                and not sub_diff[HistoryManager.NEW] \
                and not sub_diff[HistoryManager.DIFF]: # nothing changed
            return HistoryManager.NO_DIFF
        elif diff[0] == HistoryManager.NEW \
                and not sub_diff[HistoryManager.NO_DIFF] \
                and not sub_diff[HistoryManager.DIFF]: # if all the files are new
            return HistoryManager.NEW
        else: # something did change
            return HistoryManager.DIFF

    def diff(self, file):
        '''Returns a tuple (status, diff), where status NEW, NO_DIFF
        or the results of a 'diff' command if the file have changed.'''
        if not file_exists(file):
            raise InvalidPathException('File ' + file + ' does not exist.')
        diff = self._writer.diff(file)
        if diff == HistoryManager.NEW or diff == HistoryManager.NO_DIFF:
            return (diff, None)
        else:
            return (HistoryManager.DIFF, diff)


    def diff_all(self, files):
        ''' Returns a dictionary with lists.
        {'NEW': ['file1.txt'],
         'DIFF': [('file2.txt', 'the diff results')],
         'NO_DIFF': ['file3.txt', 'file4.txt']}'''
        result = {HistoryManager.NEW: [],
                  HistoryManager.DIFF: [],
                  HistoryManager.NO_DIFF: []}
        for file in files:
            if not file_exists(file):
                raise InvalidPathException('File ' + file + ' does not exist.')
            diff = self._writer.diff(file)
            if diff == HistoryManager.NEW:
                result[HistoryManager.NEW].append(file)
            elif diff == HistoryManager.NO_DIFF:
                result[HistoryManager.NO_DIFF].append(file)
            else:
                result[HistoryManager.DIFF].append((file, diff))
        return result

    def record(self, file):
        '''Record this script file as executed. Or you can pass a list of file names'''
        if type(file) is list:
            for f in file:
                if not file_exists(f):
                    raise InvalidPathException('File ' + file + ' does not exist.')
                self._writer.record(f)
        else:
            if not file_exists(file):
                raise InvalidPathException('File ' + file + ' does not exist.')
            self._writer.record(file)

class InvalidPathException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
    def __repr__(self):
        return self.__str__()


class _HistoryWriter:
    def __init__(self, script_file, dbname, schema):
        pass

    def diff(self, file):
        pass

    def record(self, file):
        pass

class _FSWriter(_HistoryWriter):
    def __init__(self, script_file, dbname, schema, path):
        self.BASE_PATH = os.path.normpath(path) + '/' + os.path.basename(script_file) + '/' + dbname + '/' + schema + '/'

    def diff(self, file):
        path = self._record_path(file)
        if not file_exists(path):
            return HistoryManager.NEW
        proc = subp.Popen(['diff', file, path], stdout=subp.PIPE)
        diff = proc.communicate()[0]
        if diff:
            return diff
        else:
            return HistoryManager.NO_DIFF

    def record(self, file):
        dest = self._record_path(file)
        path = os.path.split(dest)[0]
        if not file_exists(path):
            os.makedirs(path)
        os.system('cp ' + file + ' ' + dest)

    def _record_path(self, file):
        return self.BASE_PATH + file
