#!/usr/bin/env bash

HOWL_DIR=`dirname "$0"`

HOWL_JAR_LOC=`find . -name "howl*.jar"`

HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${HOWL_JAR_LOC}:../lib/commons-cli-2.0-SNAPSHOT.jar:../build/cli/hive-cli-0.7.0.jar:../ql/lib/antlr-runtime-3.0.1.jar

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH

HADOOP_OPTS="$HADOOP_OPTS -Dhive.metastore.uris=thrift://localhost:9083 " 

export HADOOP_OPTS=$HADOOP_OPTS

exec $HADOOP_HOME/bin/hadoop jar  ${HOWL_JAR_LOC} org.apache.hadoop.hive.howl.cli.HowlCli "$@"


