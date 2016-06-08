#!/bin/sh

# set env
home=$1
bin=`cd "$home"; cd bin; pwd`

cd $home

. "$bin"/env.sh

if [[ "$3" == "d" ]]; then
    $JAVA $JAVA_HEAP_MAX -classpath "$TEXTIT_DOWNLOADER_CLASSPATH" \
        -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=7000 \
        edu.indiana.d2i.textit.TextItDownloader $2 & wait
else
    $JAVA $JAVA_HEAP_MAX -classpath "$TEXTIT_DOWNLOADER_CLASSPATH" \
        edu.indiana.d2i.textit.TextItDownloader $2 & wait
fi