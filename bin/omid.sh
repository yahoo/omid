#!/bin/bash

########################################################################
#
# Copyright (c) 2011 Yahoo! Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. See accompanying LICENSE file.
#
########################################################################


BATCHSIZE=1000;

SCRIPTDIR=`dirname $0`
cd $SCRIPTDIR;
CLASSPATH=../conf
for j in ../target/omid*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

for j in ../lib/*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

if which greadlink; then
	READLINK=greadlink
else
	READLINK=readlink
fi

tso() {
    export LD_LIBRARY_PATH=`$READLINK -f ../src/main/native`
    exec java -Xmx1024m -cp $CLASSPATH -Domid.maxItems=1000000 -Domid.maxCommits=30000000 -Djava.library.path=$LD_LIBRARY_PATH -Dlog4j.configuration=log4j.properties com.yahoo.omid.tso.TSOServer -port 1234 -batch $BATCHSIZE -ensemble 4 -quorum 2 -zk localhost:2181
}

tsobench() {
NMSG=10
NCLIENTS=5
MAX_ROWS=20
if [ $# -ge 1 ]; then
NMSG=$1
fi
if [ $# -ge 2 ]; then
NCLIENTS=$2
fi
if [ $# -ge 3 ]; then
MAX_ROWS=$3
fi

echo running with $NMSG outstanding messages and $NCLIENTS clients
echo MAX_ROWS = $MAX_ROWS
    exec java -Xmx1024m -cp $CLASSPATH -Dlog4j.configuration=log4j.properties com.yahoo.omid.tso.TransactionClient localhost 1234 1000000 $NMSG $NCLIENTS $MAX_ROWS
}

bktest() {
    exec java -cp $CLASSPATH -Dlog4j.configuration=log4j.properties org.apache.bookkeeper.util.LocalBookKeeper 5
}

tranhbase() {
    pwd
    echo $CLASSPATH
    exec java -cp $CLASSPATH org.apache.hadoop.hbase.LocalHBaseCluster 
}

testtable() {
    exec java -cp $CLASSPATH:../target/test-classes com.yahoo.omid.TestTable
}

usage() {
    echo "Usage: omid.sh <command>"
    echo "where <command> is one of:"
    echo "  tso           Start the timestamp oracle server."
    echo "  tsobench      Run a simple benchmark of the TsO."
    echo "  bktest        Start test bookkeeper ensemble. Starts zookeeper also."
    echo "  tran-hbase    Start hbase with transaction support."
    echo "  test-table    Create test table"
}

# if no args specified, show usage
if [ $# = 0 ]; then
    usage;
    exit 1
fi

COMMAND=$1

if [ "$COMMAND" = "tso" ]; then
    tso;
elif [ "$COMMAND" = "tsobench" ]; then
  shift
    tsobench $*;
elif [ "$COMMAND" = "bktest" ]; then
    bktest;
elif [ "$COMMAND" = "tran-hbase" ]; then
    tranhbase;
elif [ "$COMMAND" = "test-table" ]; then
    testtable;
else
    usage;
fi


