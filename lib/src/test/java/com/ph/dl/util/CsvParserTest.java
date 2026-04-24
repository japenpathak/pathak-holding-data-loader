package com.ph.dl.util;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CsvParserTest {
    @Test
    void parsesSimpleCsv() throws Exception {
        File tmp = File.createTempFile("income-test", ".csv");
        try (FileWriter fw = new FileWriter(tmp)) {
            fw.write("Transaction ID,Status\n");
            fw.write("ABC123,Paid\n");
        }
        List<Map<String,Object>> rows = CsvParser.parse(tmp.getAbsolutePath());
        assertEquals(1, rows.size());
        Map<String,Object> row = rows.get(0);
        assertEquals("ABC123", row.get("transaction id"));
        assertEquals("Paid", row.get("status"));
        tmp.delete();
    }

    @Test
    void trimsAndNormalizesHeaders() throws Exception {
        File tmp = File.createTempFile("units-test", ".csv");
        try (FileWriter fw = new FileWriter(tmp)) {
            fw.write("ID, group_reference\n");
            fw.write("1,5\n");
        }
        List<Map<String, Object>> rows = CsvParser.parse(tmp.getAbsolutePath());
        assertEquals(1, rows.size());
        assertEquals("1", rows.get(0).get("id"));
        assertEquals("5", rows.get(0).get("group_reference"));
        tmp.delete();
    }

    @Test
    void rejectsNonCsv() {
        Exception ex = assertThrows(IllegalArgumentException.class, () -> CsvParser.parse("C:/fake/path.txt"));
        assertTrue(ex.getMessage().toLowerCase().contains("csv"));
    }
}