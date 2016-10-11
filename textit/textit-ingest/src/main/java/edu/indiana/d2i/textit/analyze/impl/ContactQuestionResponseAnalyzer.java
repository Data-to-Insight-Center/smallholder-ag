package edu.indiana.d2i.textit.analyze.impl;

import edu.indiana.d2i.textit.analyze.Analyzer;

import java.util.Map;
import java.util.Properties;

/**
 * Created by charmadu on 10/11/16.
 */
public class ContactQuestionResponseAnalyzer implements Analyzer {
    @Override
    public void analyze(Properties properties, Map<String, String> paramMap) {
        System.out.println("QuestionResponseAnalyzer started...");
        System.out.println("notification.email.addresses : " + properties.getProperty("notification.email.addresses"));


    }
}
