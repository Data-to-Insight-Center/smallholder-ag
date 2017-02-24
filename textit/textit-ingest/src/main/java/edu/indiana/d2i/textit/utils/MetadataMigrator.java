package edu.indiana.d2i.textit.utils;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;

/**
 * Created by charmadu on 11/21/16.
 */
public class MetadataMigrator {

    private static Logger logger = Logger.getLogger(MetadataMigrator.class);

    //DB constancts
    private static int db_port= 27017;
    private static String db_host= "localhost";

    //countries
    private static String ZAMBIA = "zambia";
    private static String KENYA = "kenya";
    private static String country;

    //metadata types
    private static String FLOWS = "flows";
    private static String CONTACTS = "contacts";
    private static String metadataType;
    private static String metadataFile;

    public static void main(String[] args) throws IOException {

        if(args.length < 3) {
            logger.error("Usage: [country] [metadata_type] [metadata_file_path]");
            System.exit(-1);
        }

        country = args[0].toLowerCase();
        if(!country.equals(ZAMBIA) && !country.equals(KENYA)) {
            logger.error("Error: Country should be either 'zambia' or 'kenya'");
            System.exit(-1);
        }

        metadataType = args[1].toLowerCase();
        if(!metadataType.equals(FLOWS) && !metadataType.equals(CONTACTS)) {
            logger.error("Error: Metadata type should be either 'flows' or 'contacts'");
            System.exit(-1);
        }

        metadataFile = args[2];

        MongoDB.createDatabase(db_host, db_port, country, null, null);

        if(metadataType.equals(FLOWS)) {
            MetadataMigrator.migrateFlowMetadata(metadataFile);
        } else if(metadataType.equals(CONTACTS)) {
            MetadataMigrator.migrateContactsMetadata(metadataFile);
        }
    }

    private static boolean migrateFlowMetadata(String path) throws IOException {
        logger.info("Migrating Flow Metadata...");

        logger.info("Input file name : " + path);

        File csvData = new File("/path/to/csv");
        CSVParser parser = CSVParser.parse(path, CSVFormat.RFC4180);
        for (CSVRecord csvRecord : parser) {
            logger.info(csvRecord.get(0));
            break;
        }

        logger.info("Done migrating Flow Metadata...");
        return true;
    }

    private static boolean migrateContactsMetadata(String path) throws IOException {

        MongoDatabase db = MongoDB.getDatabase();
        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.CONTACTS_COLLECTION_NAME);

        logger.info("Migrating Contacts Metadata...");

        logger.info("Input file name : " + path);

        File csvData = new File("/path/to/csv");
        CSVParser parser = CSVParser.parse(path, CSVFormat.RFC4180);
        Reader in = new FileReader(path);
        ArrayList<String> notFound= new ArrayList<String>();
        ArrayList<String> multipleRecords= new ArrayList<String>();
        ArrayList<String> nameMis= new ArrayList<String>();

        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
        for (CSVRecord record : records) {
            String phoneStr = record.get("Phone#");
            String cvsName = record.get("Name");
            String phone = phoneStr.length() >= 9 ? phoneStr.substring(phoneStr.length() - 9, phoneStr.length()) : null;

            if (phone == null) {
                //logger.info(phoneStr + ": Not in correct format");
                continue;
            }

            long contactsCount = contactsCollection.count(Filters.regex("phone", phone));
            if (contactsCount == 0) {
                //logger.info(phone + ": no match found");
                notFound.add(phone);
                continue;
            } else if (contactsCount > 1) {
                //logger.info(phone + ": multiple matches found -> record not updated");
                multipleRecords.add(phone);
                continue;
            }

            FindIterable<Document> contactsIter = contactsCollection.find(Filters.regex("phone", phone));
            Document contactsDocument = contactsIter.first();
            String name = contactsDocument.getString("name");

            if (StringUtils.isEmpty(name) && !StringUtils.isEmpty(cvsName)) {
                //logger.info(phone + ": Name mismatch -\"\"- -"+cvsName+"-");
                nameMis.add(phone + ","+""+","+cvsName+"\n");
            } else if (!StringUtils.isEmpty(name) && StringUtils.isEmpty(cvsName)) {
                //logger.info(phone + ": Name mismatch -"+name+"- -\"\"-");
                nameMis.add(phone + ","+name+","+""+"\n");
            } else if (StringUtils.isEmpty(name) && StringUtils.isEmpty(cvsName)){
                //Do nothing
            } else if(!name.toLowerCase().trim().equals(cvsName.toLowerCase().trim())) {
                //logger.info(phone + ": Name mismatch -"+name+"- -"+cvsName+"-");
                nameMis.add(phone + ","+name+","+cvsName+"\n");
            }

        }

        System.out.println("============Not Matched=============");
        System.out.println(notFound);
        System.out.println("============Multiple Matched=============");
        System.out.println(multipleRecords);
        System.out.println("============Name Mismatch=============");
        System.out.println(nameMis);

        logger.info("Done migrating Contacts Metadata...");
        return true;
    }

    /*private BasicDBObject buildContactsObject(Document contactsObj) throws RuntimeException {

        String flow_name = flowObject.getString("name").toLowerCase();

        List<String> flowTypes = Arrays.asList("test", "pilot", "regular", "unused");
        List<String> flowTestTypes = Arrays.asList("test", "copy", "join");
        List<String> seasons = Arrays.asList("planting", "harvest", "growing", "inter-season");
        BasicDBObject basicObject = new BasicDBObject();

        basicObject.append("creator", this.CREATOR);
        basicObject.append("country", this.COUNTRY);

        String flow_season = "";
        for (String season : seasons) {
            if (flow_name.contains(season)) {
                flow_season = season;
            }
        }
        basicObject.append("season", flow_season);


        String flow_type = "regular";
        for (String testType : flowTestTypes) {
            if (flow_name.contains(testType)) {
                flow_type = "test";
            }
        }
        basicObject.append("flow_type", flow_type);

        basicObject.append("run_start_date", df.format(start));
        basicObject.append("run_end_date", df.format(end));
        //basicObject.append("run_start_time", flowObject.getString("run_start_time"));
        //basicObject.append("run_end_time", flowObject.getString("run_end_time"));

        BasicDBObject newFlowDocument = new BasicDBObject();
        newFlowDocument.append("$set", basicObject);

        return newFlowDocument;
    }*/
}
