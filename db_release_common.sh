#!/bin/bash

#
# Common script to create database pushes.
#
# To use this, put this  in your bash script:
#
#
# And you are ready. You can then run the .sql file(s) by calling
# sql_run "file.sql" on your files.
#
#

# display what's running
echo " === Running $0"

INSTALL_PATH=
PYTHON_BIN=

function usage()
{
    echo "$0 -e db [-v | -V] [-l sqllogfile] [-r] [-n] [-f] [-i] [-a] [-g] [-T user_input_timeout]"
    echo "\
          -e db[@host]     db is the db to push code into.
          [-v]             Verbose mode
          [-V]             REALLY verbose mode.
          [-l \$sqllogfile] \$sqllogfile to change the sql log file location (default is sql_\$tag.log)
          [-f]             This is a \"real\" fake mode; the Python scripts will only output diagnostic reports when run.
          [-i]             Ignore execution history of SQL scripts.
          [-a]             SQL interaction script will show output and ask user for confirmation before continuing. 
          [-g]             If specified, use a global log file in $HOME dir. note: the file should be removed between pushes.
          [-c dir]         Directory to cd to, all the paths will be relative to it.
          [-r recdir]      Put execution records in recdir instead of 'complete/'
          [-n]             Automatically execute sql files that are new or have changed, but skip completed non-chaged files.
          [-T timeout]     User input timeout. Will exit with error when timeout reached. Default to 30 secs.
          [-d dbtype]      Specify type of db (oracle, vertica, mysql, postgresql). Oracle by default.
          [-B]             Disable bacgrounding mode. Should be used for interactive runs.
          "
          #[-S]             If specified, will pass the given flags to the SQL*Plus interaction script. Enter them in quotes like this: -S \"-d -h\"
    exit -1
}

# ask the question passed as argument, and requires a "y" confirmation
function confirm {
    echo $1
    echo "(Enter \"y\" to continue...)"
    read ans
    if [ "$ans" != "y" ]; then
        exit 0
    fi
}

# some oracle env. vars checks
if [ "$NLS_LANG" != "american_america.we8iso8859p1" ]; then
    export NLS_LANG=american_america.we8iso8859p1
fi

SQL_RETVAL=0
database=""
verbose=0
dbuser=""
olddbuser="unset"
use_global_log=0
oldpath=""
sqllogfile=""
interact_args=""    # args to go to the database interaction script
pyVerbosity="-q"    # defualt verbosity for Python scripts is quiet
db_type=""
add_args=""
host=""
cddir=""
hist_path="complete/`hostname`"
timeout="30"
no_background=""

### MAIN ####
while getopts "e:T:l:d:r:c:v n V f i a g S: C: B" options; do
    case "$options" in
        e ) environment=$OPTARG
        ;;
        l ) sqllogfile=$OPTARG
        ;;
        v ) verbose=1
            pyVerbosity=""
        ;;
        V ) verbose=1
            pyVerbosity="-v"
        ;;
        g ) use_global_log=1
        ;;
        c ) cddir="$OPTARG"
        ;;
        f ) interact_args="-d $interact_args"
        ;;
        i ) interact_args="-i $interact_args"
        ;;
        a ) interact_args="-s $interact_args"
        ;;
        n ) interact_args="-n $interact_args"
        ;;
        S ) interact_args="$interact_args $OPTARG"
        ;;
        T ) interact_args="-T $OPTARG $interact_args"
            timeout="$OPTARG"
        ;;
        r ) hist_path="$OPTARG/`hostname`"
        ;;
        d ) interact_args="--dbms $OPTARG $interact_args"
            db_type=$OPTARG
        ;;
        B ) no_background=1
        ;;
        \? ) echo "bad arg"
        usage
        ;;
        * ) echo "bad arg"
        usage
        ;;
    esac
done

interact_args="$pyVerbosity $interact_args"

if [ -z "$environment" ]; then
    usage
fi

# check whether db specifier includes host ("db@host")
if [[ "$environment" =~ "@" ]]; then
    database="`echo $environment | cut -d@ -f1`"
    host="`echo $environment | cut -d@ -f2`"
    interact_args="$interact_args -H $host"
else
    database="$environment"
fi


if [ "$sqllogfile" == "" ]; then
    if [ $use_global_log -eq 1 ]; then
        sqllogfile="$HOME/sql_$environment.log"
    else
        sqllogfile="`pwd`/sql_$environment.log"
    fi

fi


ORACLE_HOME=/opt/wgoracle-client/u01/app/oracle/product/10.2.0.3.0
if [ "$db_type" == "oracle" ] || [ "$db_type" == "" ]; then
    export ORACLE_HOME
    export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH
    export PATH=$ORACLE_HOME/bin:$PATH
fi

if [ ! -z "$DB_VERSION" ]; then
    cd /home/wgrelease/db_pushes/$DB_VERSION/
elif [ "$cddir" != "" ]; then
    cd $cddir
fi

touch $sqllogfile
echo "Detailed logs in $sqllogfile"

##### end MAIN

# some people still call that, make it empty and create a private version
function get_dbpassword()
{
    return 0
}

# Get the db password (right now it asks for it, but
# we can change this globally to greping in a file for example - well,
# say an encrypted file..). The echo is turned off so the password isnt displayed.
#
function _get_dbpassword()
{
    password=`cat /home/wgrelease/.ssh/passwords | awk -vdb="$database" -vuser="$dbuser" '{if($1 == db && $2 == user){ print $3;exit}}'`

    if [ "$password" = "" ]; then
       password_pattern="abc01"
       if [ "$dbuser" = "sync" ]; then
           password_pattern="abc02"
       fi
       password="$dbuser$password_pattern"
       echo "Password automatically entered."
    fi

    return 0
}

# Request a password from user, timeout if none given
function query_password()
{
    local timeout=$1
    read -t $timeout password || true
}

function kill_child()
{
    echo "Caught EXIT. Terminating child"
    kill $!
}

#
# Runs the .sql file passed as argument. Appends the content in $sqllogfile 
# The caller can either read the return value of sqlplus from SQL_RETVAL variable
# or from $?
# If $verbose is on, the sql output will be echo'ed to stdout
#
function sql_run()
{
    sql_file=$1
    if [ "$#" -ge "2" ]; then
        sql_args=$2
    else
        sql_args=""
    fi
    pass_args=""

    pass_args="$pass_args $interact_args"

    if [ "$add_args" ]; then
        pass_args="$pass_args $add_args"
        add_args=""
    fi

    if [ "$dbuser" != "$olddbuser" ]; then
        olddbuser="$dbuser"
        _get_dbpassword
    fi

    if [ "$use_global_log" == "1" ]; then
        curpath=`pwd`
        if [ "$curpath" != "$oldpath" ]; then
            oldpath="$curpath"
            echo "#####################################" >> "$sqllogfile"
            echo "$curpath" >> "$sqllogfile"
            echo "#####################################" >> "$sqllogfile"
        fi
    fi

    echo "=================" >> "$sqllogfile"
    echo "`date` && sqlplus $dbuser/-----@$database @$sql_file" >> "$sqllogfile" 
    echo "=================" >> "$sqllogfile" 


    if [ "$no_background" ]; then
        set +e
        $PYTHON_BIN $INSTALL_PATH/pyfiles/sql_user.py $dbuser $password $database $sql_file -l $sqllogfile -r $hist_path -x "$sql_args" $pass_args
        SQL_RETVAL=$?
        set -e
    else
        trap kill_child EXIT
        set +e
        $PYTHON_BIN $INSTALL_PATH/pyfiles/sql_user.py $dbuser $password $database $sql_file -l $sqllogfile -r $hist_path -x "$sql_args" $pass_args \
        & wait $! # without this trap will not execute
        SQL_RETVAL=$?
        set -e
        trap - EXIT
    fi

    ret_val=$SQL_RETVAL
    echo ""
    if [[ $ret_val == 1 ]]; then
        echo "$ret_val: Exited due to bad error."
        echo "Abandoning deployment."
        exit 1
    elif [[ $ret_val == 13 ]]; then
        echo "$ret_val: Incorrect password"
        password=""
        echo "Type the corrent password: "
        query_password $timeout
        if [ "$password" == "" ]; then
            echo "No password given. Abandon deployment."
            exit 1
        fi
        sql_run $sql_file $sql_args || exit $?
    elif [[ $ret_val == 0 ]]; then
        echo "$ret_val: Exited normally."
    else
        echo "$ret_val: Unknown return code."
        exit 1
    fi
    return $ret_val
}

function sql_run_always()
{
    add_args=" -i"
    sql_run
}

function end_dbpush()
{
    do_nothing=1
    echo " === End $0"
}

# This will catch errors in the user's scripts.
set -e
