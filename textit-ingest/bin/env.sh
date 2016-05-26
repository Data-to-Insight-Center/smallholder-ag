#!/bin/sh

# set env
this=$0
bin=`dirname "$this"`
bin=`cd "$bin"; pwd`
TEXTIT_DOWNLOADER_HOME=$bin/..


# java
#JAVA=$JAVA_HOME/bin/java
JAVA=java
JAVA_HEAP_MAX=-Xmx2048m 

ls "$TEXTIT_DOWNLOADER_HOME/conf/config.properties"
out=$? 
if [ $out -eq 0 ]; then
rm "$TEXTIT_DOWNLOADER_HOME/conf/config.properties"
fi

printf "token=$token\noutputdir=$2\nworkernum=$workernum\ntimezone=$timezone\ndownload_no_of_days=$download_no_of_days" > $TEXTIT_DOWNLOADER_HOME/conf/config.properties

# classpath
TEXTIT_DOWNLOADER_CLASSPATH=.
for f in ${TEXTIT_DOWNLOADER_HOME}/lib/*.jar; do
  TEXTIT_DOWNLOADER_CLASSPATH=${TEXTIT_DOWNLOADER_CLASSPATH}:$f;
done
for f in ${TEXTIT_DOWNLOADER_HOME}/*.jar; do
  TEXTIT_DOWNLOADER_CLASSPATH=${TEXTIT_DOWNLOADER_CLASSPATH}:$f;
done
TEXTIT_DOWNLOADER_CLASSPATH=${TEXTIT_DOWNLOADER_CLASSPATH}:${TEXTIT_DOWNLOADER_HOME}/conf
