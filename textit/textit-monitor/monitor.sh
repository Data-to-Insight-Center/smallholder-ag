#!/bin/sh
# check mongod
if [ ! -f /var/run/mongodb/mongod.pid ] || ! ps -p `cat /var/run/mongodb/mongod.pid` > /dev/null ; then
  echo "Mongo main database is down"
  echo "Mongo main database is down" | mail -s "Smallholder-Ag MongoDB Alert" example@umail.iu.edu
fi
# check mongod_raw
if [ ! -f /var/run/mongodb/mongod_raw.pid ] || ! ps -p `cat /var/run/mongodb/mongod_raw.pid` > /dev/null ; then
  echo "Mongo raw database is down"
  echo "Mongo raw database is down" | mail -s "Smallholder-Ag MongoDB Alert" example@umail.iu.edu
fi

