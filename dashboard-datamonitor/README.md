Dashboard DataMonitor
======================

The “Dashboard DataMonitor” is a admin dashboard developed in bootstrap and jquery. It analyse textit-data through UI with various graphs and tables in different scenario.

Pre-requisits:
--------------

1. MongoDB - two mongo instances should be running on port 27017 and 27018. Mongo server on port 27017 is to store the split data and Mongo server run on port 27018 is to store the raw data.</br>
2. Tomcat

Steps to build:
--------------

1. Checkout the Smallholder-Ag project</br>
<code>git clone https://github.com/Data-to-Insight-Center/smallholder-ag.git</code>

2. Move to the dashboard-datamonitor/ directory and execute the following command.</br>
<code>mvn clean install</code>

Steps to deploy the dashboard-datamonitor in tomcat:
--------------

1. Copy the dashboard-datamonitor/target/dashboard-datamonitor.war into TOMCAT_HOME/webapps/ directory</br>
<code>cp dashboard-datamonitor/target/dashboard-datamonitor.war TOMCAT_HOME/webapps/</code>

2. Start the server.

Now the TextIT Ingest API should be accessible through the following URL.
<code>http://[tomcat_host]:[tomcat_port]/dashboard-datamonitor</code>
