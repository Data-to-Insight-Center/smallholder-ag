package edu.indiana.d2i.textit.analyze;

import edu.indiana.d2i.textit.analyze.impl.ResponsesByContactsAnalyzer;
import edu.indiana.d2i.textit.ingest.TextItClient;
import edu.indiana.d2i.textit.utils.MongoDB;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by charmadu on 10/11/16.
 */
public class TextItDataAnalyzer {

    private static Logger logger = Logger.getLogger(TextItDataAnalyzer.class);

    public static void main(String[] args) throws IOException {

        PropertyConfigurator.configure("./conf/log4j.properties");
        Properties properties = new Properties();
        InputStream stream = TextItClient.class.getClassLoader()
                .getResourceAsStream(args[0]);

        if (stream == null) {
            throw new RuntimeException("Error : " + args[0] + " is not found!");
        }
        properties.load(stream);
        stream.close();

        try {
            MongoDB.createDatabase(properties.getProperty("mongodb.host"), Integer.parseInt(properties.getProperty("mongodb.port"))
                    , properties.getProperty("mongodb.split.db.name"), properties.getProperty("mongodb.username")
                    , properties.getProperty("mongodb.password"));
            MongoDB.createIntegratedDatabase(properties.getProperty("mongodb.host"), Integer.parseInt(properties.getProperty("mongodb.port"))
                    , properties.getProperty("mongodb.integrated.db.name"), properties.getProperty("mongodb.username")
                    , properties.getProperty("mongodb.password"));
        } catch (Exception e) {
            logger.error("Error while initializing Mongo instances with properties, " + properties.toString());
            System.exit(-1);
        }

        Map<String, String> paramMap = new HashMap<String, String>();
        if(args.length%2 != 1){
            logger.error("Number of arguments should be a multiple of two" + args);
            System.exit(-1);
        }
        int count = 1;
        while(count < args.length){
            paramMap.put(args[count], args[count+1]);
            count += 2;
        }

        Context context = new Context(new ResponsesByContactsAnalyzer(properties));
        context.executeAnalyzer(paramMap);
    }
}
