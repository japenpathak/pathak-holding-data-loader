package com.ph.dl.service;

import com.ph.dl.database.DatabaseManager;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;

public class BankTransactionsLoaderTest {

    @Test
    void extractsBankNameBetweenOrigCoNameAndOrigId() {
        String desc = "ORIG CO NAME:First Keystone C       ORIG ID:031307125  DESC DATE:121825";
        assertEquals("First Keystone C", BankTransactionsLoader.extractBankName(desc));
    }

    @Test
    void loadsRawRowsAndGeneratesBankPayments() throws Exception {
        // Ensure schema exists
        DatabaseManager db = DatabaseManager.getInstance();

        // Reset relevant tables (order matters due to FKs)
        try (Connection conn = db.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Bank_transaction_staging")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Bank_Payment")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM File_Processing")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Units")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Property")) { ps.executeUpdate(); }

            // Ensure Property reference rows exist for FK checks (id/name mapping used by loader)
            try (PreparedStatement ps = conn.prepareStatement(
                    "MERGE INTO Property (ID, Name, Full_Address, Number_of_units, Acquired_Date) KEY(ID) VALUES (?,?,?,?,?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "414-416 E Main St");
                ps.setString(3, "414-416 E Main St");
                ps.setInt(4, 4);
                ps.setNull(5, java.sql.Types.DATE);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "MERGE INTO Property (ID, Name, Full_Address, Number_of_units, Acquired_Date) KEY(ID) VALUES (?,?,?,?,?)")) {
                ps.setInt(1, 2);
                ps.setString(2, "408-410 E Main St");
                ps.setString(3, "408-410 E Main St");
                ps.setInt(4, 3);
                ps.setNull(5, java.sql.Types.DATE);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "MERGE INTO Property (ID, Name, Full_Address, Number_of_units, Acquired_Date) KEY(ID) VALUES (?,?,?,?,?)")) {
                ps.setInt(1, 3);
                ps.setString(2, "104-108 Madison St");
                ps.setString(3, "104-106-108 Madison St");
                ps.setInt(4, 8);
                ps.setNull(5, java.sql.Types.DATE);
                ps.executeUpdate();
            }

            conn.commit();
        }

        String csv = String.join("\n",
                "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #",
                "DEBIT,12/18/2025,\"ORIG CO NAME:First Keystone C       ORIG ID:031307125  DESC DATE:121825 ... FKCB loan Pymt - 50   01223 3 ...\",-543.41,ACH_DEBIT,0.00,",
                "DEBIT,12/08/2025,\"ORIG CO NAME:First Keystone C       ORIG ID:031307125  DESC DATE:120725 ... FKCB Loan Pymt - 50   01207 4 ...\",-2253.81,ACH_DEBIT,0.00,",
                "DEBIT,12/31/2025,\"ORIG CO NAME:NSM DBAMR.COOPER       ORIG ID:123 DESC DATE:...\",-10.00,ACH_DEBIT,0.00,",
                "CREDIT,12/31/2025,\"ORIG CO NAME:FLAGSTAR BANK NA       ORIG ID:123 DESC DATE:...\",10.00,ACH_CREDIT,0.00,"
        );

        Path tmp = Files.createTempFile("bank_tx", ".csv");
        Files.writeString(tmp, csv);

        // Point loader at temp CSV via system property override of DB_URL is already in gradle test.
        // For config, use our YamlConfig reading application.yml; we'll bypass by calling private methods? Instead,
        // we directly exercise loader with CsvParser rows by using public entry (it reads config) is hard.
        // To keep test self-contained, set system properties and create a minimal application.yml is out of scope.
        // So we do a narrow integration: parse CSV, then call loader internals via reflection.

        BankTransactionsLoader loader = new BankTransactionsLoader();
        var insertMethod = BankTransactionsLoader.class.getDeclaredMethod("insertRawIntoStaging", Connection.class, java.util.List.class, int.class, int.class);
        insertMethod.setAccessible(true);
        var fpMethod = BankTransactionsLoader.class.getDeclaredMethod("upsertFileProcessingAndGetId", Connection.class, String.class, int.class, String.class);
        fpMethod.setAccessible(true);
        var processMethod = BankTransactionsLoader.class.getDeclaredMethod("processStagingForFile", Connection.class, int.class, int.class);
        processMethod.setAccessible(true);
        var updateCountsMethod = BankTransactionsLoader.class.getDeclaredMethod("updateFileProcessingCounts", Connection.class, int.class);
        updateCountsMethod.setAccessible(true);

        var rows = com.ph.dl.util.CsvParser.parse(tmp.toString());

        try (Connection conn = db.getConnection()) {
            conn.setAutoCommit(false);
            int fileId = (int) fpMethod.invoke(loader, conn, tmp.toString(), 2025, "bank_transactions_raw_test");
            insertMethod.invoke(loader, conn, rows, 2025, fileId);
            processMethod.invoke(loader, conn, fileId, 2025);
            updateCountsMethod.invoke(loader, conn, fileId);
            conn.commit();

            // Staging has all 4 rows linked to fileId
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM Bank_transaction_staging WHERE Year=?")) {
                ps.setInt(1, 2025);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(4, rs.getInt(1));
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM Bank_transaction_staging WHERE FILE_ID=?")) {
                ps.setInt(1, fileId);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(4, rs.getInt(1));
                }
            }

            // Matching DEBIT rows are processed successfully (3 rows -> Y)
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM Bank_transaction_staging WHERE FILE_ID=? AND IS_PROCESSED='Y'")) {
                ps.setInt(1, fileId);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1));
                }
            }

            // File_Processing counts updated (total=4, success=3, failed=0)
            try (PreparedStatement ps = conn.prepareStatement("SELECT Total_Rows, Success_Rows, Failed_Rows FROM File_Processing WHERE ID=?")) {
                ps.setInt(1, fileId);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(4, rs.getInt(1));
                    assertEquals(3, rs.getInt(2));
                    assertEquals(0, rs.getInt(3));
                }
            }

            // Bank_Payment should have 3 rows (only DEBIT rows with matching ORIG CO NAME prefixes)
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM Bank_Payment WHERE Year=?")) {
                ps.setInt(1, 2025);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1));
                }
            }
        }
    }
}