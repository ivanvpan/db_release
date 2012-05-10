import re
import os
import sys
from select import select
from functools import partial

def file_exists(file):
    return os.path.exists(file) or os.path.exists(file + '.sql')

def query_user(options, timeout=30):
    prompt = '[' + '/'.join(list(options)) + ']: '
    while True:
        print prompt
        rlist, _, _ = select([sys.stdin], [], [], timeout)
        if rlist:
            line = sys.stdin.readline()
            if not line:
                return None
            user_input = line[0]
            if options.find(user_input) > -1:
                return user_input
        else:
            return None

def find_subscripts(sql_file, syntax):
    '''Recursively goes through the script and all its dependencies
    and returns a list of all subscripts. Throw IOError if a file is missing.'''
    result = []
    if syntax == 'oracle':
        match_fn = partial(_sub_match, comment_pat=r'^-- ', sub_pat='@([\w/.-]*);?\s+(--)?')
    elif syntax == 'vertica' \
        or syntax == 'postgresql':
        match_fn = partial(_sub_match, comment_pat='^-- ', sub_pat=r'\\i ([\w/.-]*)')
    elif syntax == 'mysql':
        match_fn = partial(_sub_match, comment_pat='(^-- )|(^#)', sub_pat=r'\\. ([\w/.-]*)')

    with open(sql_file, 'r') as file:
        lines = file.readlines()
        subs = (match_fn(line) for line in lines if match_fn(line))
        for sub in subs:
            if not os.path.exists(sub) and os.path.exists(sub + '.sql'):
                sub = sub + '.sql'
            result.append(sub)
            result.extend(find_subscripts(sub, syntax)) # recurse into the subscript
    return result

def _sub_match(line, comment_pat='', sub_pat=''):
    comment = re.compile(comment_pat)
    sub = re.compile(sub_pat)
    if not comment.match(line) and sub.match(line):
        return sub.match(line).group(1)
    return None

