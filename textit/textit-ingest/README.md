Textit-Ingest
========

The “textit-ingest” is a service developed in Java, in order to retrieve the data from TextIt service from the TextIt API. This service can be scheduled to run weekly or daily.

Build the package
-----------------

1. Checkout the smallholder-ag/textit/textit-ingest/ project and move to the textit-ingest directory
2. Run 'mvn clean install'

You will see the newly created /target/deploy directory. This directory has all the resources needed to run the textit-ingest service.
 
Run the service
---------------
 
1. Move to the smallholder-ag/textit/textit-ingest/target/deploy directory
2. Add correct values to the conf/global_kenya.properties and conf/global_zambia.properties files
3. Run the following two commands to initiate the cron job for two countries;</br>
  sh bin/textit-schedular.sh conf/global_zambia.properties</br>
  sh bin/textit-schedular.sh conf/global_zambia.properties</br>
 
  
