package edu.indiana.d2i.textit.api.util;

import java.io.InputStream;
import java.util.Properties;

public class Constants {

    public static String apiUrl;

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
    }
}
