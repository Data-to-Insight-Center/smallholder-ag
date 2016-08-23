package edu.indiana.d2i.textit.api.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by kunarath on 5/13/16.
 */
public class Constants {

    public static String mongoHost;
    public static int mongoPort;
    public static int kenyaTime;
    public static int zambiaTime;
    private static Logger logger = Logger.getLogger(Constants.class);

    static {
        try {
            loadConfigurations();
            logger.info("Configuration loaded.");
        } catch (IOException e) {
            logger.error("Error while loading configuration : " + e.getMessage());
        }
    }

    private static void loadConfigurations() throws IOException {
        InputStream inputStream = Constants.class
                .getResourceAsStream("./default.properties");
        Properties props = new Properties();
        props.load(inputStream);
        mongoHost = props.getProperty("mongo.host", "localhost");
        mongoPort = Integer.parseInt(props.getProperty("mongo.port", "27017"));
        kenyaTime = Integer.parseInt(props.getProperty("kenya.time", "6"));
        zambiaTime = Integer.parseInt(props.getProperty("zambia.time", "11"));
    }
}
