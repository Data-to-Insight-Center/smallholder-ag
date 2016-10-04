package edu.indiana.d2i.textit.ingest;

import edu.indiana.d2i.textit.ingest.utils.MongoDB;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

public class TextItIngestor {
    private static Logger logger = Logger.getLogger(TextItIngestor.class);
    static DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) throws IOException {
        PropertyConfigurator.configure("./conf/log4j.properties");
        df.setTimeZone(TimeZone.getTimeZone("timezone"));

		if (args.length != 2 && args.length != 3 && args.length != 4) {
			logger.error("Usage: [config_file] [weekly|daily|dur] [start_date] [end_date]");
			System.exit(-1);
		}

        if(!args[1].equals("daily") && !args[1].equals("weekly") && !args[1].equals("dur")){
            logger.error("Error: Second argument to the TextItDownloader should be either 'daily', 'weekly' or 'dur'");
            System.exit(-1);
        }

        if(args[1].equals("dur") && args.length <= 3) {
            logger.error("Error: If the script need to be run for a duration provide a start and end date");
            System.exit(-1);
        }

        if(args.length > 2 && args[2] != null && args[2] != "") {
            try {
                Date end_date = df.parse(args[2]);
            } catch (ParseException e) {
                logger.error("Error: Third argument to the TextItDownloader should be in yyyy-MM-dd format");
                System.exit(-1);
            }
        }

        if(args.length > 3 && args[3] != null && args[3] != "") {
            try {
                Date start_date = df.parse(args[3]);
            } catch (ParseException e) {
                logger.error("Error: Third argument to the TextItDownloader should be in yyyy-MM-dd format");
                System.exit(-1);
            }
        }

        logger.info("Starting TextItIngestor...");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("Clean up resources.");
            }
        });

        Properties properties = new Properties();
        InputStream stream = TextItClient.class.getClassLoader()
                .getResourceAsStream(args[0]);
        if (stream == null) {
            throw new RuntimeException("Error : " + args[0] + " is not found!");
        }
        if(args[1].equals("daily")) {
            properties.put("download_no_of_days", "1");
        } else if (args[1].equals("weekly")) {
            properties.put("download_no_of_days", "7");
        }

        Date end_date = new Date();
        if(args.length > 2 && args[2] != null && args[2] != "") {
            try {
                end_date =df.parse(args[2]);
                properties.put("start_date", args[2]);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        properties.load(stream);
        stream.close();

        String output_dir = properties.getProperty("outputdir") + df.format(end_date);

        try {
            MongoDB.createDatabase(properties.getProperty("mongodb.host"), Integer.parseInt(properties.getProperty("mongodb.port"))
                    , properties.getProperty("mongodb.db.name"), properties.getProperty("mongodb.username")
                    , properties.getProperty("mongodb.password"));
            MongoDB.createRawDatabase(properties.getProperty("raw.mongodb.host"), Integer.parseInt(properties.getProperty("raw.mongodb.port"))
                    , properties.getProperty("mongodb.db.name"));
        } catch (Exception e) {
            logger.error("Error while initializing Mongo instances with properties, " + properties.toString());
            System.exit(-1);
        }

        boolean downloaded = false;
        logger.info("Just download the runs.");
        TextItClient client = TextItClient.createClient(properties);
        downloaded = client.downloadRuns();
        client.close();


        if(!downloaded) {
            logger.error("Failure to download data from TextIt");
            System.exit(-1);
        }

        DBHandler dbHandler = new DBHandler(output_dir);
        boolean persisted = dbHandler.persistData();

        if(!persisted) {
            logger.error("Failure to persist data in MongoDB");
            System.exit(-1);
        }
        logger.info("Finished persisting data to MongoDB...");

        logger.info("Finished TextItIngestor...");
    }
}
