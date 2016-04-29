#!/bin/sh


for f in /u/smallag/textit-data/Zambia/daily/`date +%Y-%m-%d`/*.json; do
/usr/bin/mongoimport --db WSC --collection zambia`date +%Y%m%d`daily --username smallag --password Supriya2890 $f
done