package edu.indiana.d2i.textit.ingest.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.util.Arrays;

public class MongoDB {

    private static Logger logger = Logger.getLogger(MongoDB.class);

    public static String FLOWS_COLLECTION_NAME = "flows";
    public static String RUNS_COLLECTION_NAME = "runs";
    public static String CONTACTS_COLLECTION_NAME = "contacts";
    public static String STATUS_COLLECTION_NAME = "status";

    private static MongoDatabase database = null;
    private static MongoCollection<Document> flowsCollection = null;
    private static MongoCollection<Document> contactsCollection = null;
    private static MongoCollection<Document> runsCollection = null;
    private static MongoCollection<Document> statusCollection = null;

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
            logger.info("Initialized Database : " + database.getName());
        }
        return database;
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

    public static void addContacts(String contacts) {
        if(contactsCollection == null) {
            contactsCollection = database.getCollection(CONTACTS_COLLECTION_NAME);
        }
        contactsCollection.insertOne(Document.parse(contacts));
    }

    public static void addRuns(String runs) {
        if(runsCollection == null) {
            runsCollection = database.getCollection(RUNS_COLLECTION_NAME);
        }
        runsCollection.insertOne(Document.parse(runs));
    }
}
