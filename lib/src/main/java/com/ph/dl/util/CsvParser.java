package com.ph.dl.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.*;

public class CsvParser {
    public static List<Map<String, Object>> parse(String filePath) throws Exception {
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("CSV file path is empty");
        }
        // Validate CSV extension
        File f = new File(filePath);
        if (!f.exists()) throw new IllegalArgumentException("CSV file not found: " + filePath);
        String lower = f.getName().toLowerCase(Locale.ROOT);
        if (!lower.endsWith(".csv")) {
            throw new IllegalArgumentException("Input must be a CSV file: " + filePath);
        }
        try (Reader reader = new FileReader(f);
             CSVParser parser = CSVFormat.DEFAULT
                     .withFirstRecordAsHeader()
                     .withIgnoreHeaderCase()
                     .withTrim()
                     .parse(reader)) {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (CSVRecord record : parser) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (Map.Entry<String, Integer> header : parser.getHeaderMap().entrySet()) {
                    String key = header.getKey();
                    String val = record.get(key);
                    row.put(key, val);
                }
                rows.add(row);
            }
            return rows;
        }
    }
}