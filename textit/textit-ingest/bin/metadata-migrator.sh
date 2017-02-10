#!/bin/sh

# set env
home=$1
bin=`cd "$home"; cd bin; pwd`

cd $home

. "$bin"/env.sh

$JAVA $JAVA_HEAP_MAX -classpath "$TEXTIT_DOWNLOADER_CLASSPATH" \
    edu.indiana.d2i.textit.utils.MetadataMigrator $2 & wait
