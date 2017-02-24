package edu.indiana.d2i.textit.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
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
import java.util.HashMap;
import java.util.Map;

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

        String primaryKey = null;
        String primaryDBKey = null;
        if(country.equals(ZAMBIA)) {
            primaryKey = "Phone#";
            primaryDBKey = "phone";
        }

        Map<String,String> keytoKeyMap = new HashMap<String,String>();
        if(country.equals(ZAMBIA)) {
            keytoKeyMap.put("Lat", "latitude");
            keytoKeyMap.put("Long", "longitude");
            keytoKeyMap.put("Province", "province");
            keytoKeyMap.put("District", "district");
            keytoKeyMap.put("Camp", "camp");
            keytoKeyMap.put("UID", "uid");
            keytoKeyMap.put("HICPS/COWS", "hicps_cows");
        }

        Map<String,String> keytoReplaceKey = new HashMap<String,String>();
        if(country.equals(ZAMBIA)) {
            keytoReplaceKey.put("Name", "name");
        }

        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.CONTACTS_COLLECTION_NAME);

        logger.info("Migrating Contacts Metadata...");

        logger.info("Input file name : " + path);

        Reader in = new FileReader(path);
        ArrayList<String> notFound= new ArrayList<String>();
        ArrayList<String> multipleRecords= new ArrayList<String>();
        ArrayList<String> nameMis= new ArrayList<String>();

        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
        int updatedCount = 0;

        for (CSVRecord record : records) {
            String phoneStr = record.get(primaryKey);
            String phone = phoneStr.length() >= 9 ? phoneStr.substring(phoneStr.length() - 9, phoneStr.length()) : null;

            if (phone == null) {
                continue;
            }

            long contactsCount = contactsCollection.count(Filters.regex(primaryDBKey, phone));
            if (contactsCount == 0) {
                notFound.add(phone);
                continue;
            } else if (contactsCount > 1) {
                multipleRecords.add(phone);
                continue;
            }

            FindIterable<Document> contactsIter = contactsCollection.find(Filters.regex(primaryDBKey, phone));
            Document contactsDocument = contactsIter.first();
            String uuid = contactsDocument.getString("uuid");

            BasicDBObject basicObject = new BasicDBObject();

            for(String key : keytoKeyMap.keySet()) {
                basicObject.append(keytoKeyMap.get(key), record.get(key));
            }
            BasicDBObject newContactsDocument = new BasicDBObject();
            newContactsDocument.append("$set", basicObject);

            for(String key : keytoReplaceKey.keySet()) {
                String dbKey = contactsDocument.getString(keytoReplaceKey.get(key));
                String cvskey = record.get(key);
                if (StringUtils.isEmpty(dbKey) && !StringUtils.isEmpty(cvskey)) {
                    basicObject.append(keytoReplaceKey.get(key), record.get(key));
                    nameMis.add(phone + "," + "" + "," + cvskey + "\n");
                } else if (!StringUtils.isEmpty(dbKey) && StringUtils.isEmpty(cvskey)) {
                    nameMis.add(phone + "," + dbKey + "," + "" + "\n");
                } else if (StringUtils.isEmpty(dbKey) && StringUtils.isEmpty(cvskey)) {
                    //Do nothing
                } else if (!dbKey.toLowerCase().trim().equals(cvskey.toLowerCase().trim())) {
                    nameMis.add(phone + "," + dbKey + "," + cvskey + "\n");
                }
            }

            UpdateResult updateResult = contactsCollection.updateOne(new BasicDBObject("uuid", uuid), newContactsDocument);
            if(updateResult.wasAcknowledged()) {
                updatedCount++;
            } else {
                logger.error("Contact with UUID " + uuid + " couldn't updated successfully");
            }

        }

        logger.info("Number of contacts updated : " + updatedCount);

        System.out.println("============Not Matched=============");
        System.out.println(notFound);
        System.out.println("============Multiple Matches=============");
        System.out.println(multipleRecords);
        //System.out.println("============Name Mismatch=============");
        //System.out.println(nameMis);

        logger.info("Done migrating Contacts Metadata...");
        return true;
    }
}
