#!/bin/sh

. $1

today=`date '+%A'`
location=`echo $timezone | cut -d '/' -f2`

for f in /u/smallag/textit-data/WSC/$location/daily/`date +%Y-%m-%d`/*.json; do
file_name=`echo $f | egrep -wio --color 'runs|contacts|flows'`
/usr/bin/mongoimport --db WSC_RAW --collection $location"_"daily"_"`date +%Y%m%d`$file_name --username $mongodb_username --password $mongodb_pswd $f
done

if [[ $today == "Monday" && $timezone == "Africa/Zambia" ]]; then
time_span="weekly"
location="Zambia"
for f in /u/smallag/textit-data/WSC/$location/$time_span/`date +%Y-%m-%d`/*.json; do
file_name=`echo $f | egrep -wio --color 'runs|contacts|flows'`
/usr/bin/mongoimport --db WSC_RAW --collection $location"_"$time_span"_"`date +%Y%m%d`$file_name --username $mongodb_username --password $mongodb_pswd $f
done
fi

if [[ $today == "Saturday" && $timezone == "Africa/Kenya" ]]; then
time_span="weekly"
location="Kenya"
for f in /u/smallag/textit-data/WSC/$location/$time_span/`date +%Y-%m-%d`/*.json; do
file_name=`echo $f | egrep -wio --color 'runs|contacts|flows'`
/usr/bin/mongoimport --db WSC_RAW --collection $location"_"$time_span"_"`date +%Y%m%d`$file_name --username $mongodb_username --password $mongodb_pswd $f
done
fi
