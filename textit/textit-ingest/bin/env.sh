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

# classpath
TEXTIT_DOWNLOADER_CLASSPATH=.
for f in ${TEXTIT_DOWNLOADER_HOME}/lib/*.jar; do
  TEXTIT_DOWNLOADER_CLASSPATH=${TEXTIT_DOWNLOADER_CLASSPATH}:$f;
done
for f in ${TEXTIT_DOWNLOADER_HOME}/*.jar; do
  TEXTIT_DOWNLOADER_CLASSPATH=${TEXTIT_DOWNLOADER_CLASSPATH}:$f;
done
TEXTIT_DOWNLOADER_CLASSPATH=${TEXTIT_DOWNLOADER_CLASSPATH}:${TEXTIT_DOWNLOADER_HOME}/conf
