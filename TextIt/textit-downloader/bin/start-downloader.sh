#!/bin/sh

# set env
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/env.sh

$JAVA $JAVA_HEAP_MAX -classpath "$TEXTIT_DOWNLOADER_CLASSPATH" \
  edu.indiana.d2i.textit.TextItDownloader &
