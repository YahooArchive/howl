#!/usr/bin/env bash

HOWL_DIR=`dirname "$0"`

HOWL_JAR_LOC=`find . -name "howl*.jar"`

HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${HOWL_JAR_LOC}:../lib/commons-cli-2.0-SNAPSHOT.jar:../build/cli/hive-cli-0.7.0.jar:../ql/lib/antlr-runtime-3.0.1.jar

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH

HADOOP_OPTS="$HADOOP_OPTS -Dhive.metastore.uris=thrift://localhost:9083 " 

export HADOOP_OPTS=$HADOOP_OPTS

exec $HADOOP_HOME/bin/hadoop jar  ${HOWL_JAR_LOC} org.apache.hadoop.hive.howl.cli.HowlCli "$@"

# Above is the recommended way to launch howl cli. If it doesnt work, you can try the following:
# java -Dhive.metastore.uris=thrift://localhost:9083 -cp ../lib/commons-logging-1.0.4.jar:../build/hadoopcore/hadoop-0.20.0/hadoop-0.20.0-core.jar:../lib/commons-cli-2.0-SNAPSHOT.jar:../build/cli/hive-cli-0.7.0.jar:../ql/lib/antlr-runtime-3.0.1.jar:$HOWL_JAR org.apache.hadoop.hive.howl.cli.HowlCli "$@"
