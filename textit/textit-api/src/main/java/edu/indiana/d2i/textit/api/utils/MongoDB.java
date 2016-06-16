package edu.indiana.d2i.textit.api.utils;

/**
 * Created by kunarath on 5/13/16.
 */

import com.mongodb.MongoClient;
import org.apache.log4j.Logger;

public class MongoDB {

    static public MongoClient mongoClientInstance = null;
    public static String flowsCollectionName = "flows";
    public static String runsCollectionName = "runs";
    public static String contactsCollectionName = "contacts";
    public static String statusCollectionName = "status";
    private static Logger logger = Logger.getLogger(MongoDB.class);

    public static synchronized MongoClient getMongoClientInstance() {
        if (mongoClientInstance == null) {
            try {
                mongoClientInstance = new MongoClient(Constants.mongoHost, Constants.mongoPort);
            } catch (Exception e) {
                logger.error("Error initializing MongoClient: " + e.getMessage());
            }
        }
        return mongoClientInstance;
    }
}
