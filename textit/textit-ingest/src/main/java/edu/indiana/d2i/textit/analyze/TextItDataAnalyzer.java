package edu.indiana.d2i.textit.analyze;

import edu.indiana.d2i.textit.analyze.impl.ContactQuestionResponseAnalyzer;
import edu.indiana.d2i.textit.ingest.TextItClient;
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

        Map<String, String> paramMap = new HashMap<String, String>();

        Context context = new Context(new ContactQuestionResponseAnalyzer());
        context.executeAnalyzer(properties, paramMap);
    }
}
