package edu.indiana.d2i.textit.ingest.utils;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;

public class MongoDB {

    private static Logger logger = Logger.getLogger(MongoDB.class);

    public static String FLOWS_COLLECTION_NAME = "flows";
    public static String RUNS_COLLECTION_NAME = "runs";
    public static String CONTACTS_COLLECTION_NAME = "contacts";
    public static String STATUS_COLLECTION_NAME = "status";
    public static String RAW_RUNS_COLLECTION_NAME = "raw_runs";

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

    private static MongoDatabase database = null;
    private static DB rawDatabase = null;
    private static MongoCollection<Document> flowsCollection = null;
    private static MongoCollection<Document> contactsCollection = null;
    private static MongoCollection<Document> runsCollection = null;
    private static MongoCollection<Document> statusCollection = null;
    private static GridFS rawRunsCollection = null;

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

    public static synchronized DB createRawDatabase(String host, int port, String dbName) {
        if (rawDatabase == null) {
            MongoClientOptions.Builder builder = MongoClientOptions.builder().serverSelectionTimeout(5000);
            MongoClient client = client = new MongoClient(new ServerAddress(host, port), builder.build());
            client.getAddress();
            rawDatabase = client.getDB(dbName);
            logger.info("Initialized database '" + rawDatabase.getName() + "' with port " + port + " and host " + host);
        }
        return rawDatabase;
    }

    public static void addStatus(String status) {
        if(statusCollection == null) {
            statusCollection = database.getCollection(STATUS_COLLECTION_NAME);
        }
        statusCollection.insertOne(Document.parse(status));
    }

    public static void addFlow(String flow) {
        if(flowsCollection == null) {
            flowsCollection = database.getCollection(FLOWS_COLLECTION_NAME);
        }
        flowsCollection.insertOne(Document.parse(flow));
    }

    public static void addContact(String contacts) {
        if(contactsCollection == null) {
            contactsCollection = database.getCollection(CONTACTS_COLLECTION_NAME);
        }
        contactsCollection.insertOne(Document.parse(contacts));
    }

    public static void addRun(String runs) {
        if(runsCollection == null) {
            runsCollection = database.getCollection(RUNS_COLLECTION_NAME);
        }
        runsCollection.insertOne(Document.parse(runs));
    }

    public static void addRawRuns(String folder, String fileName) throws FileNotFoundException {
        if(rawRunsCollection == null) {
            rawRunsCollection = new GridFS(rawDatabase, RAW_RUNS_COLLECTION_NAME);
        }
        FileInputStream inputStream = new FileInputStream(new File(folder + "/" + fileName));
        GridFSInputFile gfsFile = rawRunsCollection.createFile(inputStream, fileName, true);
        gfsFile.save();
    }
}
