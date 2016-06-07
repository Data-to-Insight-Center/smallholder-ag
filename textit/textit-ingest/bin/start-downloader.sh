#!/bin/sh

# set env
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

outputdir_daily=$outputdir"daily/"
. "$bin"/env.sh

if [[ "$2" == "d" ]]; then
    $JAVA $JAVA_HEAP_MAX -classpath "$TEXTIT_DOWNLOADER_CLASSPATH" \
        -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=7000 \
        edu.indiana.d2i.textit.TextItDownloader $1 & wait
else
    $JAVA $JAVA_HEAP_MAX -classpath "$TEXTIT_DOWNLOADER_CLASSPATH" \
        edu.indiana.d2i.textit.TextItDownloader $1 & wait
fi