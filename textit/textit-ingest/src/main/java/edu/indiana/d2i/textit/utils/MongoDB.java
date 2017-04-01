package edu.indiana.d2i.textit.utils;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;

public class MongoDB {

    private static Logger logger = Logger.getLogger(MongoDB.class);

    public static String FLOWS_COLLECTION_NAME = "flows";
    public static String RUNS_COLLECTION_NAME = "runs";
    public static String CONTACTS_COLLECTION_NAME = "contacts";
    public static String CONTACTS_STAT_COLLECTION_NAME = "contactsStats";
    public static String STATUS_COLLECTION_NAME = "status";
    public static String RAW_RUNS_COLLECTION_NAME = "raw_runs";
    public static String RAW_FLOWS_COLLECTION_NAME = "raw_flows";
    public static String RAW_CONTACTS_COLLECTION_NAME = "raw_contacts";

    //status elements
    public static final String TYPE = "type";
    public static final String ACTION = "action";
    public static final String DOWNLOAD = "download";
    public static final String WRITE_TO_MONGO = "writeToMongo";
    public static final String DATE = "date";
    public static final String STATUS = "status";
    public static final String SUCCESS = "success";
    public static final String FAILURE = "failure";
    public static final String MESSAGE = "message";
    public static final String INTERVAL = "interval";
    public static final String WEEKLY = "weekly";
    public static final String DAILY = "daily";
    public static final String DURATION = "dur";
    public static final String START_DATE = "start_date";
    public static final String END_DATE = "end_date";

    //contact status elements
    public static final String UPDATED = "updated";
    public static final String FROM_DATE = "fromDate";
    public static final String TO_DATE = "toDate";
    public static final String TOTAL = "total";


    private static MongoDatabase database = null;
    private static MongoDatabase integratedDatabase = null;
    private static DB rawDatabase = null;
    private static MongoCollection<Document> flowsCollection = null;
    private static MongoCollection<Document> contactsCollection = null;
    private static MongoCollection<Document> contactsStatCollection = null;
    private static MongoCollection<Document> runsCollection = null;
    private static MongoCollection<Document> statusCollection = null;
    private static GridFS rawRunsCollection = null;
    private static GridFS rawFlowsCollection = null;
    private static GridFS rawContactsCollection = null;

    public static synchronized MongoDatabase createDatabase(String host, int port, String dbName, String username, String password) {
        if (database == null) {
            MongoClient client = null;
            MongoClientOptions.Builder builder = MongoClientOptions.builder().serverSelectionTimeout(5000);
            if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
                MongoCredential credential = MongoCredential.createMongoCRCredential(username,
                        dbName, password.toCharArray());
                client = new MongoClient(new ServerAddress(host, port), Arrays.asList(credential), builder.build());
            } else {
                client = new MongoClient(new ServerAddress(host, port), builder.build());
            }
            client.getAddress();
            database = client.getDatabase(dbName);
            logger.info("Initialized database '" + database.getName() + "' with port " + port + " and host " + host);
        }
        return database;
    }

    public static synchronized MongoDatabase createIntegratedDatabase(String host, int port, String dbName, String username, String password) {
        if (integratedDatabase == null) {
            MongoClient client = null;
            MongoClientOptions.Builder builder = MongoClientOptions.builder().serverSelectionTimeout(5000);
            if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
                MongoCredential credential = MongoCredential.createMongoCRCredential(username,
                        dbName, password.toCharArray());
                client = new MongoClient(new ServerAddress(host, port), Arrays.asList(credential), builder.build());
            } else {
                client = new MongoClient(new ServerAddress(host, port), builder.build());
            }
            client.getAddress();
            integratedDatabase = client.getDatabase(dbName);
            logger.info("Initialized database '" + integratedDatabase.getName() + "' with port " + port + " and host " + host);
        }
        return integratedDatabase;
    }

    public static synchronized DB createRawDatabase(String host, int port, String dbName) {
        if (rawDatabase == null) {
            MongoClientOptions.Builder builder = MongoClientOptions.builder().serverSelectionTimeout(5000);
            MongoClient client = new MongoClient(new ServerAddress(host, port), builder.build());
            client.getAddress();
            rawDatabase = client.getDB(dbName);
            logger.info("Initialized database '" + rawDatabase.getName() + "' with port " + port + " and host " + host);
        }
        return rawDatabase;
    }

    public static MongoDatabase getIntegratedDatabase() {
        return integratedDatabase;
    }

    public static MongoDatabase getDatabase() {
        return database;
    }

    public static void addStatus(String status) {
        if(statusCollection == null) {
            statusCollection = database.getCollection(STATUS_COLLECTION_NAME);
        }
        statusCollection.insertOne(Document.parse(status));
    }

    public static void addFlow(String uuid, String flow) {
        if(flowsCollection == null) {
            flowsCollection = database.getCollection(FLOWS_COLLECTION_NAME);
            BasicDBObject index = new BasicDBObject();
            index.put("uuid", 1);
            flowsCollection.createIndex(index);
        }
        flowsCollection.updateOne(new Document("uuid", uuid), new Document("$set", Document.parse(flow)),
                (new UpdateOptions()).upsert(true));
    }

    public static void addContact(String uuid, String contacts) {
        if(contactsCollection == null) {
            contactsCollection = database.getCollection(CONTACTS_COLLECTION_NAME);
            BasicDBObject index = new BasicDBObject();
            index.put("uuid", 1);
            contactsCollection.createIndex(index);
        }
        contactsCollection.updateOne(new Document("uuid", uuid), new Document("$set", Document.parse(contacts)),
                (new UpdateOptions()).upsert(true));
    }

    public static void addContactStat(String contacts) {
        if(contactsStatCollection == null) {
            contactsStatCollection = database.getCollection(CONTACTS_STAT_COLLECTION_NAME);
        }
        contactsStatCollection.insertOne(Document.parse(contacts));
    }

    public static void addRun(String flow_uuid, String contact, String runs) {
        if(runsCollection == null) {
            runsCollection = database.getCollection(RUNS_COLLECTION_NAME);
            BasicDBObject index = new BasicDBObject();
            //index.put("flow_uuid", 1);
            //index.put("contact", 1);
            index.put("run", 1);
            runsCollection.createIndex(index);
        }
        //BasicDBObject query = new BasicDBObject("flow_uuid", flow_uuid).append("contact", contact);
        //runsCollection.replaceOne(query, Document.parse(runs), (new UpdateOptions()).upsert(true));

        JSONObject run = new JSONObject(runs);
        runsCollection.replaceOne(new Document("run", run.getLong("run")), Document.parse(runs),
                (new UpdateOptions()).upsert(true));
    }

    public static void addRawRuns(String folder, String fileName) throws FileNotFoundException {
        if(rawRunsCollection == null) {
            rawRunsCollection = new GridFS(rawDatabase, RAW_RUNS_COLLECTION_NAME);
        }
        FileInputStream inputStream = new FileInputStream(new File(folder + "/" + fileName));
        GridFSInputFile gfsFile = rawRunsCollection.createFile(inputStream, fileName, true);
        gfsFile.save();
    }

    public static void addRawFlows(String folder, String fileName) throws FileNotFoundException {
        if(rawFlowsCollection == null) {
            rawFlowsCollection = new GridFS(rawDatabase, RAW_FLOWS_COLLECTION_NAME);
        }
        FileInputStream inputStream = new FileInputStream(new File(folder + "/" + fileName));
        GridFSInputFile gfsFile = rawFlowsCollection.createFile(inputStream, fileName, true);
        gfsFile.save();
    }

    public static void addRawContacts(String folder, String fileName) throws FileNotFoundException {
        if(rawContactsCollection == null) {
            rawContactsCollection = new GridFS(rawDatabase, RAW_CONTACTS_COLLECTION_NAME);
        }
        FileInputStream inputStream = new FileInputStream(new File(folder + "/" + fileName));
        GridFSInputFile gfsFile = rawContactsCollection.createFile(inputStream, fileName, true);
        gfsFile.save();
    }
}
