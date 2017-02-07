package edu.indiana.d2i.textit.ingest;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import edu.indiana.d2i.textit.utils.MongoDB;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MetadataUpdater {
    private static Logger logger = Logger.getLogger(MetadataUpdater.class);
    static DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat df_Z = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private SimpleDateFormat df_dm = new SimpleDateFormat("d MMM");

    private static final String FLOWS = "flows";
    private static final String RUNS = "runs";
    private static final String CONTACTS = "contacts";
    private static final String STATS = "stats";
    private String END_DATE;
    private String START_DATE;
    private String INTERVAL;
    private String EMAILS;
    private int NO_OF_DAYS;
    private int TEXT_TIME;
    private String COUNTRY;
    List<String> runsOfFlowsCreated;

    public MetadataUpdater(Properties properties) {
        this.END_DATE = properties.getProperty("end_date");
        this.START_DATE = properties.getProperty("start_date");
        this.INTERVAL = properties.getProperty("interval");
        this.EMAILS = properties.getProperty("notification.email.addresses");
        this.NO_OF_DAYS = Integer.valueOf(properties.getProperty("update_no_of_days"));
        int textTime = 0;
        if (properties.getProperty("text.time") != null) {
            textTime = Integer.parseInt(properties.getProperty("text.time"));
        }
        this.TEXT_TIME = textTime;
        COUNTRY = properties.getProperty("mongodb.db.name");
        df.setTimeZone(TimeZone.getTimeZone("timezone"));
    }

    public static final int TextItMaxDataCount = 250;

    public static void main(String[] args) throws IOException {
        PropertyConfigurator.configure("./conf/log4j.properties");
        df.setTimeZone(TimeZone.getTimeZone("timezone"));

		if (args.length != 2 && args.length != 3 && args.length != 4) {
			logger.error("Usage: [config_file] ["+MongoDB.WEEKLY+"|"+MongoDB.DAILY+"|"+MongoDB.DURATION+"] [end_date] [start_date]");
			System.exit(-1);
		}

        if(!args[1].equals(MongoDB.DAILY) && !args[1].equals(MongoDB.WEEKLY) && !args[1].equals(MongoDB.DURATION)){
            logger.error("Error: Second argument to the MetadataUpdater should be either '" + MongoDB.WEEKLY
                    + "', '" + MongoDB.DAILY + "' or '" + MongoDB.DURATION + "'");
            System.exit(-1);
        }

        if(args[1].equals(MongoDB.DURATION) && args.length <= 3) {
            logger.error("Error: If the script need to be run for a duration provide a start and end date");
            System.exit(-1);
        }

        if(args.length > 2 && args[2] != null && args[2] != "") {
            try {
                Date end_date = df.parse(args[2]);
            } catch (ParseException e) {
                logger.error("Error: Third argument to the MetadataUpdater should be in yyyy-MM-dd format");
                System.exit(-1);
            }
        }

        if(args.length > 3 && args[3] != null && args[3] != "") {
            try {
                Date start_date = df.parse(args[3]);
            } catch (ParseException e) {
                logger.error("Error: Third argument to the MetadataUpdater should be in yyyy-MM-dd format");
                System.exit(-1);
            }
        }

        logger.info("Starting MetadataUpdater...");

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

        properties.put("interval", args[1]);

        int no_of_days = 0;
        if(args[1].equals(MongoDB.DAILY)) {
            no_of_days = 1;
        } else if (args[1].equals(MongoDB.WEEKLY)) {
            no_of_days = 7;
        }
        properties.put("update_no_of_days", ""+no_of_days);

        Date end_date = new Date();
        if(args.length > 2 && args[2] != null && args[2] != "") {
            try {
                end_date =df.parse(args[2]);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        String end_date_str = df.format(end_date);
        properties.put("end_date", end_date_str);

        String start_date_str;
        if(args[1].equals(MongoDB.DURATION)) {
            start_date_str = args[3];
        } else {
            start_date_str = df.format(new DateTime(end_date).minusDays(no_of_days).toDate());
        }
        properties.put("start_date", start_date_str);

        properties.load(stream);
        stream.close();

        try {
            MongoDB.createDatabase(properties.getProperty("mongodb.host"), Integer.parseInt(properties.getProperty("mongodb.port"))
                    , properties.getProperty("mongodb.db.name"), properties.getProperty("mongodb.username")
                    , properties.getProperty("mongodb.password"));
        } catch (Exception e) {
            logger.error("Error while initializing Mongo instances with properties, " + properties.toString());
            System.exit(-1);
        }

        MetadataUpdater metadataUpdater = new MetadataUpdater(properties);
        boolean updated = metadataUpdater.updateFlowMetadata();

        if(!updated) {
            logger.error("Failure to update metadata");
            System.exit(-1);
        }
        logger.info("Finished updating metadata...");

        logger.info("Finished MeatadataUpdater...");
    }

    private boolean updateFlowMetadata() {
        // TODO from=2016-08-08T11:00:00.000Z&to=2016-08-22T11:00:00.000Z
        MongoDatabase db = MongoDB.getDatabase();
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.FLOWS_COLLECTION_NAME);
        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.CONTACTS_COLLECTION_NAME);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.RUNS_COLLECTION_NAME);

        int countryCode = 0;
        int countryHour = TEXT_TIME;
        String fromDate = START_DATE + "T" + String.format("%02d", TEXT_TIME) + ":00:00.000Z";
        String toDate = END_DATE + "T" + String.format("%02d", TEXT_TIME) + ":00:00.000Z";
        if(COUNTRY.equals("zambia")) {
            countryCode = Calendar.MONDAY;
        } else if (COUNTRY.equals("kenya")) {
            countryCode = Calendar.SATURDAY;
        } else {
            logger.error("invalid country");
            System.exit(-1);
        }


        System.out.println("Update metadata from " + fromDate + " to " + toDate + " in time " + TEXT_TIME);
        Date first = null;
        Date last = null;
        try {
            if(fromDate != null) {
                first = df_Z.parse(fromDate);
            } else {
                FindIterable<Document> iter = flowsCollection.find().sort(new Document("created_on",1)).limit(1);;
                iter.projection(new Document("created_on", 1).append("_id", 0));
                first = df_Z.parse((String) iter.first().get("created_on"));
            }
            if(toDate != null) {
                last = df_Z.parse(toDate);
            } else {
                last = new Date();
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar c = Calendar.getInstance();
        c.setTime(last);
        Date currEnd = c.getTime();
        Date currBeg = null;

        ArrayList<String> weekLabels = new ArrayList<>();
        //Map<String, ArrayList<String>> weekQuestionLabels = new HashMap<String, ArrayList<String>>();

        while(currEnd.getTime() > first.getTime()) {
            Calendar cTemp = Calendar.getInstance();
            cTemp.setTime(currEnd);
            cTemp.set(Calendar.DAY_OF_WEEK, countryCode);
            cTemp.set(Calendar.HOUR_OF_DAY, countryHour);
            cTemp.set(Calendar.MINUTE, 0);
            cTemp.set(Calendar.SECOND, 0);
            cTemp.set(Calendar.MILLISECOND, 0);
            if (currEnd.equals(cTemp.getTime())) {
                cTemp.add(Calendar.DATE, -7);
                currBeg = cTemp.getTime();
            } else
                currBeg = cTemp.getTime();

            if(first.after(cTemp.getTime())) {
                currBeg = first;
            }

            currEnd = new Date(currEnd.getTime() -1);

            Calendar cWeek = Calendar.getInstance();
            cWeek.setMinimalDaysInFirstWeek(7);
            cWeek.setTime(currBeg);
            String label = cWeek.get(Calendar.YEAR) + " W" + String.format("%02d", cWeek.get(Calendar.WEEK_OF_YEAR))
                    + " : " + df_dm.format(currBeg) + " - " + df_dm.format(currEnd);
            cWeek.setTime(currEnd);
            //String nextLabel = cWeek.get(Calendar.YEAR) + " W" + String.format("%02d", cWeek.get(Calendar.WEEK_OF_YEAR))
            //+ " : " + df_dm.format(currBeg) + " - " + df_dm.format(currEnd);
            weekLabels.add(label);
            System.out.println(label);
            //weekQuestionLabels.put(label, new ArrayList<String>());

            Bson flowsFilter = null; // TODO apply flows filter

            ArrayList<Document> flowsIter = null;
            try {
                flowsIter = edu.indiana.d2i.textit.utils.TextItUtils.getFlowsByDeploymentDate(flowsCollection, runsCollection, flowsFilter, df_Z.format(currBeg), df_Z.format(currEnd));
            } catch (ParseException e) {
                e.printStackTrace();
            }

            currEnd = currBeg;
        }
        return true;
    }

}
