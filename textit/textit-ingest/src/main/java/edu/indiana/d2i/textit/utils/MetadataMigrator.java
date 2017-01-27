package edu.indiana.d2i.textit.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;

/**
 * Created by charmadu on 11/21/16.
 */
public class MetadataMigrator {
    public static void main(String[] args) throws IOException {
        String filename = args[0];

        System.out.println("Input file name : " + args[0]);

        File csvData = new File("/path/to/csv");
        CSVParser parser = CSVParser.parse(filename, CSVFormat.RFC4180);
        for (CSVRecord csvRecord : parser) {
            System.out.println(csvRecord.get(0));
            break;
        }
    }
}
