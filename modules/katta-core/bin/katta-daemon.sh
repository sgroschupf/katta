#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Runs a Katta command as a daemon.
#
# Environment Variables
#
#   KATTA_CONF_DIR  Alternate conf dir. Default is ${KATTA_HOME}/conf.
#   KATTA_LOG_DIR   Where log files are stored.  PWD by default.
#   KATTA_MASTER    host:path where katta code should be rsync'd from
#   KATTA_PID_DIR   The pid files are stored. /tmp by default.
#   KATTA_IDENT_STRING   A string representing this instance of katta. $USER by default
#   KATTA_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: katta-daemon.sh [--config confdir] (start|stop) <katta-command> <args...>"

unset CDPATH
# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/katta-config.sh

# get arguments
startStop=$1
shift
command=$1
shift

katta_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ "$num" -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num="$prev"
	done
	mv "$log" "$log.$num";
    fi
}

if [ -f "${KATTA_CONF_DIR}/katta-env.sh" ]; then
  . "${KATTA_CONF_DIR}/katta-env.sh"
fi

# get log directory
if [ "$KATTA_LOG_DIR" = "" ]; then
  export KATTA_LOG_DIR="$KATTA_HOME/logs"
fi
mkdir -p "$KATTA_LOG_DIR"

if [ "$KATTA_PID_DIR" = "" ]; then
  KATTA_PID_DIR=/tmp
fi

if [ "$KATTA_IDENT_STRING" = "" ]; then
  export KATTA_IDENT_STRING="$USER"
fi

# some variables
commandForFileName=`echo "$command" | sed -e 'y/ /_/'`
commandForLogFileName=`echo "$commandForFileName" | sed -e 's/katta_start//'`
export KATTA_LOGFILE=katta-"$commandForLogFileName"-"$HOSTNAME".log
export KATTA_ROOT_LOGGER="INFO,DRFA"
log=$KATTA_LOG_DIR/katta-$commandForLogFileName-$HOSTNAME.out
pid=$KATTA_PID_DIR/katta-$KATTA_IDENT_STRING-$commandForFileName.pid

# Set default scheduling priority
if [ "$KATTA_NICENESS" = "" ]; then
    export KATTA_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$KATTA_PID_DIR"

    if [ -f "$pid" ]; then
      if kill -0 `cat "$pid"` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$KATTA_MASTER" != "" ]; then
      echo rsync from "$KATTA_MASTER"
      rsync -a -e ssh --delete --exclude=.svn --exclude=logs --exclude=zookeeper-data --exclude=zookeeper-log-data "$KATTA_MASTER"/ "$KATTA_HOME"
    fi

    katta_rotate_log "$log"
    echo starting "$command", logging to "$log"
    nohup nice -n $KATTA_NICENESS "$KATTA_HOME"/bin/katta --config "$KATTA_CONF_DIR" "$command" "$@" > "$log" 2>&1 < /dev/null &
    echo $! > "$pid"
    sleep 1; head "$log"
    ;;
          
  (stop)

    stopCommandDisplayName=`echo "$command" | sed -e 's/start//'`
    if [ -f "$pid" ]; then
      if kill -0 `cat "$pid"` > /dev/null 2>&1; then
        echo stopping $stopCommandDisplayName
        kill `cat "$pid"`
      else
        echo No $stopCommandDisplayName to stop
      fi
    else
      echo No $stopCommandDisplayName to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


