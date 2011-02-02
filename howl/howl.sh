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

HOWL_JAR_LOC=`find . -name "howl*.jar"`

if [ "$HOWL_JAR_LOC" == "" ]
then
  echo "Unable to find Howl jar. Exiting."
  exit 1
fi

if [ ! "$HADOOP_HOME" ]
then
  echo "HADOOP_HOME is not defined.  Exiting."
  exit 1
fi


HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${HOWL_JAR_LOC}:../lib/commons-cli-2.0-SNAPSHOT.jar:../build/cli/hive-cli-0.7.0.jar:../ql/lib/antlr-runtime-3.0.1.jar

for f in `ls ../build/dist/lib/*.jar`; do
  HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$f
done

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH

HADOOP_OPTS="$HADOOP_OPTS -Dhive.metastore.uris=thrift://localhost:9083 "

export HADOOP_OPTS=$HADOOP_OPTS

exec $HADOOP_HOME/bin/hadoop jar  ${HOWL_JAR_LOC} org.apache.hadoop.hive.howl.cli.HowlCli "$@"

# Above is the recommended way to launch howl cli. If it doesnt work, you can try the following:
# java -Dhive.metastore.uris=thrift://localhost:9083 -cp ../lib/commons-logging-1.0.4.jar:../build/hadoopcore/hadoop-0.20.0/hadoop-0.20.0-core.jar:../lib/commons-cli-2.0-SNAPSHOT.jar:../build/cli/hive-cli-0.7.0.jar:../ql/lib/antlr-runtime-3.0.1.jar:$HOWL_JAR org.apache.hadoop.hive.howl.cli.HowlCli "$@"
