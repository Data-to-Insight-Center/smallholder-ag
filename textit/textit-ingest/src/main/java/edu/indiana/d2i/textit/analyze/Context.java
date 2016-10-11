package edu.indiana.d2i.textit.analyze;

import java.util.Map;
import java.util.Properties;

/**
 * Created by charmadu on 10/11/16.
 */
public class Context {
    private Analyzer analyzer;

    public Context(Analyzer analyzer){
        this.analyzer = analyzer;
    }

    public void executeAnalyzer(Properties properties, Map<String, String> paramMap){
        analyzer.analyze(properties, paramMap);
    }
}
