TextIt Ingestor
===============

The software here continuously ingests data from the TextIT cloud service. TextIT administers surveys through cell phones to farmers in Zambia and Kenya. The data are collected at the TextIT server. These tools pull down the data, minimally process it, and drop it into a MongoDB database. The data are then available for monitoring, visualization, and integration with local sensor data from PulsePod sensors being developed at Princeton. This work funded in part under grants from the National Science Foundation BCS 1026776 and SES 1360463.

Deploy Textit-Ingestor
======================

Documentation at: https://github.com/Data-to-Insight-Center/smallholder-ag/wiki/Smallholder-Ag--:-TextIt-Ingest-and-API

Pre-requisits:
--------------

1. MongoDB - two mongo instances should be running on port 27017 and 27018. Mongo server on port 27017 is to store the split data and Mongo server run on port 27018 is to store the raw data.</br>
2. Tomcat

Steps to build:
--------------

1. Checkout the Smallholder-Ag project</br>
<code>git clone https://github.com/Data-to-Insight-Center/smallholder-ag.git</code>

2. Move to the textit/ directory and execute the following command.</br>
<code>mvn clean install</code>

Steps to setup the textit data collecting cronjob:
--------------

1. Move to the textit-ingest/target/deploy/ directory

2. Add correct values for token in two configuration files;</br>
<code>conf/global_zambia.properties</code></br>
<code>conf/global_kenya.properties</code>

3. Schedule the Cron Jobs</br>
Run the bin/textit-schedular.sh script in bin/ folder as follows;</br>
<code>sh bin/textit-schedular.sh -c conf/global_zambia.properties -w -dw 1 -hdw 7</code></br>
<code>sh bin/textit-schedular.sh -c conf/global_kenya.properties -w -dw 6 -hdw 2</code></br>

This will create two cron jobs.</br>
In Zambia the runs are created after 1pm Zambia time(11am GMT) on every Monday. Therefore the first cron job will run the script weekly on Monday at 7am local time(11am GMT) in local servers to collect data in Zambia.</br>
In Kenya the runs are created after 9am Zambia time(6am GMT) on every Saturday. Therefore the second cron job will run the script weekly on Saturday at 2am local time(6am GMT) in local servers to collect data in Kenya.</br>

Script parameters:</br>
-c    : configuration file path</br>
-w/-d : a flag to indiacate whether the script is going to run weekly or daily</br>
-dw   : if script runs weekly, which day the script should run on ( 0-6 : Sunday-Saturday)</br>
-hdw  : if script runs weekly, which hour of the day of the week the script should run on[0-23]</br>
-hd   : if the script runs daily, which hour the script should run on ( 0 - 23)</br>
-s    : customized start date for data collection (ex: 2016-06-27)
-h    : help

Steps to deploy the tesxtit-ingest-api in tomcat:
--------------

1. Copy the textit-api/target/textit-api.war into TOMCAT_HOME/webapps/ directory</br>
<code>cp textit-api/target/textit-api.war TOMCAT_HOME/webapps/</code>

2. Start the server.

Now the TextIT Ingest API should be accessible through the following URL.
<code>http://[tomcat_host]:[tomcat_port]/textit-api</code>
