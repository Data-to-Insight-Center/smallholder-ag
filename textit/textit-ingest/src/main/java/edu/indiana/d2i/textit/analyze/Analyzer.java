package edu.indiana.d2i.textit.analyze;

import java.util.Map;
import java.util.Properties;

/**
 * Created by charmadu on 10/11/16.
 */
public interface Analyzer {
    public void analyze(Properties properties, Map<String, String> paramMap);
}
