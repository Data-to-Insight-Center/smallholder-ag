package edu.indiana.d2i.textit.utils;

/**
 * Created by kunarath on 5/13/16.
 */

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoDB {

    static public MongoClient mongoClientInstance = null;
    public static String flowsObjects = "flows";
    public static String runsObjects = "runs";
    public static String contactsObjects = "contacts";

    public static synchronized MongoClient getMongoClientInstance() {
        if (mongoClientInstance == null) {
            try {
                mongoClientInstance = new MongoClient(Constants.mongoHost, Constants.mongoPort);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return mongoClientInstance;
    }

    static public MongoDatabase getServicesDB1() {
        MongoDatabase db1 = getMongoClientInstance().getDatabase(Constants.textitDbName1);
        return db1;
    }
    static public MongoDatabase getServicesDB2() {
        MongoDatabase db2 = getMongoClientInstance().getDatabase(Constants.textitDbName2);
        return db2;
    }
}
