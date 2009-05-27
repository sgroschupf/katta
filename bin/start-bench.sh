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


# Start all katta daemons.  Run this on master node.

usage="Usage: start-bench.sh <num-test-clients>"

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

NUM_TEST_CLIENTS=$1

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/katta-config.sh

# start master daemons
"$bin"/katta-daemon.sh start katta\ startMaster --config $KATTA_CONF_DIR

sleep 10

# start katta test clients
"$bin"/katta-daemons.sh --num-nodes $NUM_TEST_CLIENTS start katta\ startLoadTestNode --config $KATTA_CONF_DIR

# start katta nodes
"$bin"/katta-daemons.sh --start-node $NUM_TEST_CLIENTS start katta\ startNode --config $KATTA_CONF_DIR
