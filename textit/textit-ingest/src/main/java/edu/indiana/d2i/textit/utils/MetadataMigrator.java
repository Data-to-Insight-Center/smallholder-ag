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
            primaryDBKey = "urns.0";
        }
        if(country.equals(KENYA)) { //Contacts_metadata_kenya_2.csv
            primaryKey = "UUID";
            primaryDBKey = "uuid";
        }
        /*if(country.equals(KENYA)) { //Contacts_metadata_kenya_1.csv
            primaryKey = "UID";
            primaryDBKey = "fields.uid";
        }*/

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
        if(country.equals(KENYA)) { // Contacts_metadata_kenya_2.csv
            keytoKeyMap.put("CWP", "cwp");
            keytoKeyMap.put("WRUA", "wrua");
            keytoKeyMap.put("cwp member?", "cwp_member");
            keytoKeyMap.put("1 Sept 16 enrolled?", "1_sep_16_enrolled");
            keytoKeyMap.put("Latitude", "latitude");
            keytoKeyMap.put("Longitude", "longitude");
        }
        /*if(country.equals(KENYA)) { // Contacts_metadata_kenya_1.csv
            keytoKeyMap.put("CWP_ID", "cwp_id");
            keytoKeyMap.put("WRUA_ID", "wrua_id");
            keytoKeyMap.put("ltime", "ltime");
            keytoKeyMap.put("altitude", "altitude");
            keytoKeyMap.put("x_proj", "x_proj");
            keytoKeyMap.put("y_proj", "y_proj");
            keytoKeyMap.put("ident", "ident");
        }*/

        Map<String,String> keytoReplaceKey = new HashMap<String,String>();
        if(country.equals(ZAMBIA)) {
            keytoReplaceKey.put("Name", "name");
        }
        //if(country.equals(KENYA)) { // Contacts_metadata_kenya_2.csv
        //    keytoReplaceKey.put("Locid", "fields.locid");
        //    keytoReplaceKey.put("Uid", "fields.uid");
        //}
        if(country.equals(KENYA)) { // Contacts_metadata_kenya_1.csv
            keytoReplaceKey.put("WRUA", "wrua");
            keytoReplaceKey.put("CWP", "cwp");
            keytoReplaceKey.put("Latitude", "latitude");
            keytoReplaceKey.put("Longitude", "longitude");
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
            String primaryKeyStr = record.get(primaryKey);
            String primaryKeycsv = primaryKeyStr;
            if(country.equals(ZAMBIA)) {// since its a phone number
                primaryKeycsv = primaryKeyStr.length() >= 9 ? primaryKeyStr.substring(primaryKeyStr.length() - 9, primaryKeyStr.length()) : null;
            }

            if (primaryKeycsv == null) {
                continue;
            }

            long contactsCount = contactsCollection.count(Filters.regex(primaryDBKey, primaryKeycsv));
            if (contactsCount == 0) {
                notFound.add(primaryKeycsv);
                continue;
            } else if (contactsCount > 1) {
                multipleRecords.add(primaryKeycsv);
                continue;
            }

            FindIterable<Document> contactsIter = contactsCollection.find(Filters.regex(primaryDBKey, primaryKeycsv));
            Document contactsDocument = contactsIter.first();
            String uuid = contactsDocument.getString("uuid");

            BasicDBObject basicObject = new BasicDBObject();

            for(String key : keytoKeyMap.keySet()) {
                basicObject.append(keytoKeyMap.get(key), record.get(key));
            }
            BasicDBObject newContactsDocument = new BasicDBObject();
            newContactsDocument.append("$set", basicObject);

            for(String key : keytoReplaceKey.keySet()) {
                String dbKey;
                String tempKey = keytoReplaceKey.get(key);
                if (tempKey.contains(".")) {
                    String[] array = tempKey.split("\\.");
                    Document obj = contactsDocument;
                    for(int i = 0 ; i < array.length -1 ; i++) {
                        obj = (Document) contactsDocument.get(array[i]);
                    }
                    dbKey = obj.getString(array[array.length -1]);
                } else {
                    dbKey = contactsDocument.getString(keytoReplaceKey.get(key));
                }
                String cvskey = record.get(key);
                if (StringUtils.isEmpty(dbKey) && !StringUtils.isEmpty(cvskey)) {
                    basicObject.append(keytoReplaceKey.get(key), record.get(key));
                    nameMis.add(primaryKeycsv + "," + "" + "," + cvskey + "\n");
                } else if (!StringUtils.isEmpty(dbKey) && StringUtils.isEmpty(cvskey)) {
                    nameMis.add(primaryKeycsv + "," + dbKey + "," + "" + "\n");
                } else if (StringUtils.isEmpty(dbKey) && StringUtils.isEmpty(cvskey)) {
                    //Do nothing
                } else if (!dbKey.toLowerCase().trim().equals(cvskey.toLowerCase().trim())) {
                    nameMis.add(primaryKeycsv + "," + dbKey + "," + cvskey + "==not_matched\n");
                } else if (dbKey.toLowerCase().trim().equals(cvskey.toLowerCase().trim())) {
                    nameMis.add(primaryKeycsv + "," + dbKey + "," + cvskey + "==matched\n");
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
        System.out.println("============Name Mismatch=============");
        //System.out.println(nameMis);

        logger.info("Done migrating Contacts Metadata...");
        return true;
    }
}
