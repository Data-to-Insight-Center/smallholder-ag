#!/bin/sh

# set env
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. $1

outputdir_daily=$outputdir"daily/"
. "$bin"/env.sh $token $outputdir_daily $workernum $timezone $download_no_of_days $mongodb_username $mongodb_pswd

$JAVA $JAVA_HEAP_MAX -classpath "$TEXTIT_DOWNLOADER_CLASSPATH" \
  edu.indiana.d2i.textit.TextItDownloader & wait


today=`date '+%A'`

if [[ $today == "Monday" && $timezone == "Africa/Zambia" ]]; then
download_no_of_days=7
outputdir_weekly=$outputdir"weekly/"
. "$bin"/env.sh $token $outputdir_weekly $workernum $timezone $download_no_of_days $mongodb_username $mongodb_pswd

$JAVA $JAVA_HEAP_MAX -classpath "$TEXTIT_DOWNLOADER_CLASSPATH" \
  edu.indiana.d2i.textit.TextItDownloader &
fi

if [[ $today == "Saturday" && $timezone == "Africa/Kenya" ]]; then
download_no_of_days=7
outputdir_weekly=$outputdir"weekly/"
. "$bin"/env.sh $token $outputdir_weekly $workernum $timezone $download_no_of_days $mongodb_username $mongodb_pswd

$JAVA $JAVA_HEAP_MAX -classpath "$TEXTIT_DOWNLOADER_CLASSPATH" \
  edu.indiana.d2i.textit.TextItDownloader &
fi
