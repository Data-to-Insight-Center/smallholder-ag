package edu.indiana.d2i.textit.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by kunarath on 5/13/16.
 */
public class Constants {

    public static String mongoHost;
    public static int mongoPort;

    public static String textitDbName1;
    public static String textitDbName2;


    static {
        try {
            loadConfigurations();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void loadConfigurations() throws IOException {
        InputStream inputStream = Constants.class
                .getResourceAsStream("./default.properties");
        Properties props = new Properties();
        props.load(inputStream);
        mongoHost = props.getProperty("mongo.host", "localhost");
        mongoPort = Integer.parseInt(props.getProperty("mongo.port", "27017"));
        textitDbName1 = props.getProperty("textit.db1.name", "zambia");
        textitDbName2 = props.getProperty("textit.db2.name", "kenya");
    }
}
