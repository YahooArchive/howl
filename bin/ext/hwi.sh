THISSERVICE=hwi
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

hwi() {

  if $cygwin; then
    HIVE_LIB=`cygpath -w "$HIVE_LIB"`
  fi

  CLASS=org.apache.hadoop.hive.hwi.HWIServer
  export HWI_JAR_FILE=${HIVE_LIB}/hive_hwi.jar
  export HWI_WAR_FILE=${HIVE_LIB}/hive_hwi.war

  #hwi requires ant jars
  if [ "$ANT_LIB" = "" ] ; then
    ANT_LIB=/opt/ant/lib
  fi
  for f in ${ANT_LIB}/*.jar; do
    if [[ ! -f $f ]]; then
      continue;
    fi
    HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f
  done

  export HADOOP_CLASSPATH
  exec $HADOOP jar $AUX_JARS_CMD_LINE ${HWI_JAR_FILE} $CLASS $HIVE_OPTS "$@"
  #nohup $HADOOP jar $AUX_JARS_CMD_LINE ${HWI_JAR_FILE} $CLASS $HIVE_OPTS "$@" >/dev/null 2>/dev/null &

}

hwi_help(){
  echo "Usage ANT_LIB=XXXX hive --service hwi"	
}
