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


# Run a shell command on all node hosts.
#
# Environment Variables
#
#   KATTA_NODES    File naming remote hosts.
#     Default is ${KATTA_CONF_DIR}/nodes.
#   KATTA_CONF_DIR  Alternate conf dir. Default is ${KATTA_HOME}/conf.
#   KATTA_NODE_SLEEP Seconds to sleep between spawning remote commands.
#   KATTA_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: nodes.sh [--config confdir] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/katta-config.sh

# If the nodes file is specified in the command line,
# then it takes precedence over the definition in 
# katta-env.sh. Save it here.
HOSTLIST=$KATTA_NODES

if [ -f "${KATTA_CONF_DIR}/katta-env.sh" ]; then
  . "${KATTA_CONF_DIR}/katta-env.sh"
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$KATTA_NODES" = "" ]; then
    export HOSTLIST="${KATTA_CONF_DIR}/nodes"
  else
    export HOSTLIST="${KATTA_NODES}"
  fi
fi

for node in `cat "$HOSTLIST"`; do
 ssh $KATTA_SSH_OPTS $node $"${@// /\\ }" \
   2>&1 | sed "s/^/$node: /" &
 if [ "$KATTA_NODE_SLEEP" != "" ]; then
   sleep $KATTA_NODE_SLEEP
 fi
done

wait
