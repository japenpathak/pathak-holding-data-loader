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
        assertEquals("ABC123", row.get("Transaction ID"));
        assertEquals("Paid", row.get("Status"));
        tmp.delete();
    }

    @Test
    void rejectsNonCsv() {
        Exception ex = assertThrows(IllegalArgumentException.class, () -> CsvParser.parse("C:/fake/path.txt"));
        assertTrue(ex.getMessage().toLowerCase().contains("csv"));
    }
}
