package edu.indiana.d2i.textit.api.util;

import java.io.InputStream;
import java.util.Properties;

public class Constants {

    public static String apiUrl;
    public static String apiUsername;
    public static String apiPassword;

    static {
        try {
            loadConfigurations();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void loadConfigurations() throws Exception {
        Properties properties = new Properties();
        InputStream inputStream = Constants.class.getResourceAsStream("default.properties");
        if (inputStream != null) {
            properties.load(inputStream);
        } else {
            throw new Exception("Error while reading Matchmaker properties");
        }
        apiUrl = properties.getProperty("api.url");
        apiUsername = properties.getProperty("api.username");
        apiPassword = properties.getProperty("api.password");
    }
}
