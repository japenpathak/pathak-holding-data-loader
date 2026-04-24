package com.ph.dl.service;

import com.ph.dl.database.DatabaseManager;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;

class InternetUtilityProcessorTest {

    @Test
    void extractsTrnFromDescriptionLastToken() {
        assertEquals("12345", InternetUtilityProcessor.extractTrnFromDescription("ORIG CO NAME:SERVICE ELECTRIC INTERNET TRN:12345"));
        assertEquals("ABC", InternetUtilityProcessor.extractTrnFromDescription("x y z TRN:ABC"));
        assertEquals("777", InternetUtilityProcessor.extractTrnFromDescription("TRN:777 trailing words"));
        assertNull(InternetUtilityProcessor.extractTrnFromDescription("x y z"));
        assertNull(InternetUtilityProcessor.extractTrnFromDescription("TRN:"));
        assertNull(InternetUtilityProcessor.extractTrnFromDescription("x y TRN: "));
    }

    @Test
    void createsExpenseFromMatchingStagingRow() throws Exception {
        DatabaseManager db = DatabaseManager.getInstance();
        int year = 2025;

        int propertyMadisonId;
        int propertyScottId;
        int providerId;
        int categoryId;

        try (Connection conn = db.getConnection()) {
            conn.setAutoCommit(false);
            // clean minimal tables used
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Expense")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Bank_transaction_staging")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM File_Processing")) { ps.executeUpdate(); }
            conn.commit();
        }

        // Ensure reference rows exist (Property/Provider/Category)
        try (Connection conn = db.getConnection()) {
            conn.setAutoCommit(false);

            // Properties referenced by logic
            try (PreparedStatement ps = conn.prepareStatement("MERGE INTO Property (ID, Name, Full_Address, Number_of_units, Acquired_Date) KEY(ID) VALUES (?,?,?,?,?)")) {
                ps.setInt(1, 3);
                ps.setString(2, "104-108 Madison St");
                ps.setString(3, "104-106-108 Madison St");
                ps.setInt(4, 8);
                ps.setNull(5, java.sql.Types.DATE);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("MERGE INTO Property (ID, Name, Full_Address, Number_of_units, Acquired_Date) KEY(ID) VALUES (?,?,?,?,?)")) {
                ps.setInt(1, 4);
                ps.setString(2, "423 Scott St");
                ps.setString(3, "423 Scott St");
                ps.setInt(4, 6);
                ps.setNull(5, java.sql.Types.DATE);
                ps.executeUpdate();
            }

            // Category Utility
            try (PreparedStatement ps = conn.prepareStatement("MERGE INTO Expense_Category (Category_Id, Category_Name) KEY(Category_Id) VALUES (?, ?)")) {
                ps.setInt(1, 17);
                ps.setString(2, "Utility");
                ps.executeUpdate();
            }

            // Provider Service Electric
            try (PreparedStatement ps = conn.prepareStatement("MERGE INTO Provider (Provider_Id, Provider_Name) KEY(Provider_Id) VALUES (?, ?)")) {
                ps.setInt(1, 54);
                ps.setString(2, "Service Electric");
                ps.executeUpdate();
            }

            conn.commit();
        }

        try (Connection conn = db.getConnection()) {
            // read ids back
            try (PreparedStatement ps = conn.prepareStatement("SELECT ID FROM Property WHERE Name='104-108 Madison St'")) {
                try (ResultSet rs = ps.executeQuery()) { rs.next(); propertyMadisonId = rs.getInt(1); }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT ID FROM Property WHERE Name='423 Scott St'")) {
                try (ResultSet rs = ps.executeQuery()) { rs.next(); propertyScottId = rs.getInt(1); }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT Provider_Id FROM Provider WHERE Provider_Name='Service Electric'")) {
                try (ResultSet rs = ps.executeQuery()) { rs.next(); providerId = rs.getInt(1); }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT Category_Id FROM Expense_Category WHERE Category_Name='Utility'")) {
                try (ResultSet rs = ps.executeQuery()) { rs.next(); categoryId = rs.getInt(1); }
            }
        }

        int fileId;
        int stagingIdHigh;
        int stagingIdLow;

        try (Connection conn = db.getConnection()) {
            conn.setAutoCommit(false);

            // Insert File_Processing row to back payment account derivation
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO File_Processing (File_Path, Processed_At, Year, File_Type, Total_Rows, Success_Rows, Failed_Rows) VALUES (?, CURRENT_TIMESTAMP(), ?, ?, 0, 0, 0)",
                    PreparedStatement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, "C:/tmp/Chase_7996_ALL_Activity_20251227.CSV");
                ps.setInt(2, year);
                ps.setString(3, "bank_transactions_raw");
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    assertTrue(rs.next());
                    fileId = rs.getInt(1);
                }
            }

            // Insert two staging rows (one amount > 50, one <= 50)
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO Bank_transaction_staging (FILE_ID, Details, Posting_Date, Description, Amount, Type, Check_Num, Year, IS_PROCESSED) VALUES (?,?,?,?,?,?,?,?,?)",
                    PreparedStatement.RETURN_GENERATED_KEYS)) {
                ps.setInt(1, fileId);
                ps.setString(2, "DEBIT");
                ps.setDate(3, java.sql.Date.valueOf("2025-01-02"));
                ps.setString(4, "ORIG CO NAME:SERVICE ELECTRIC INTERNET TRN:99999");
                ps.setBigDecimal(5, new java.math.BigDecimal("75.00"));
                ps.setString(6, "ACH");
                ps.setNull(7, java.sql.Types.VARCHAR);
                ps.setInt(8, year);
                ps.setNull(9, java.sql.Types.VARCHAR);
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    assertTrue(rs.next());
                    stagingIdHigh = rs.getInt(1);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO Bank_transaction_staging (FILE_ID, Details, Posting_Date, Description, Amount, Type, Check_Num, Year, IS_PROCESSED) VALUES (?,?,?,?,?,?,?,?,?)",
                    PreparedStatement.RETURN_GENERATED_KEYS)) {
                ps.setInt(1, fileId);
                ps.setString(2, "DEBIT");
                ps.setDate(3, java.sql.Date.valueOf("2025-01-03"));
                ps.setString(4, "ORIG CO NAME:SERVICE ELECTRIC INTERNET TRN:11111");
                ps.setBigDecimal(5, new java.math.BigDecimal("25.00"));
                ps.setString(6, "ACH");
                ps.setNull(7, java.sql.Types.VARCHAR);
                ps.setInt(8, year);
                ps.setNull(9, java.sql.Types.VARCHAR);
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    assertTrue(rs.next());
                    stagingIdLow = rs.getInt(1);
                }
            }

            conn.commit();
        }

        // Run processor
        InternetUtilityProcessor p = new InternetUtilityProcessor();
        try (Connection conn = db.getConnection()) {
            conn.setAutoCommit(false);
            int processed = p.process(conn, year);
            conn.commit();

            assertEquals(2, processed);

            // Verify Expense rows
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM Expense WHERE Transaction IN (?,?)")) {
                ps.setString(1, "SE-" + year + "-99999");
                ps.setString(2, "SE-" + year + "-11111");
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(2, rs.getInt(1));
                }
            }

            // High amount selects Madison property
            try (PreparedStatement ps = conn.prepareStatement("SELECT Property, Unit, Category, Provider, Payment_Account, Details, Payment_Mode, Exclude, Exclude_Zaky, Year FROM Expense WHERE Transaction=?")) {
                ps.setString(1, "SE-" + year + "-99999");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(propertyMadisonId, rs.getInt("Property"));
                    assertTrue(rs.getObject("Unit") == null, "Unit should be NULL");
                    assertEquals(categoryId, rs.getInt("Category"));
                    assertEquals(providerId, rs.getInt("Provider"));
                    assertEquals("Chase-7996", rs.getString("Payment_Account"));
                    assertEquals("Service Eletcric Internet Service", rs.getString("Details"));
                    assertEquals("ACH", rs.getString("Payment_Mode"));
                    assertEquals("N", rs.getString("Exclude"));
                    assertEquals("N", rs.getString("Exclude_Zaky"));
                    assertEquals(year, rs.getInt("Year"));
                }
            }

            // Low amount selects Scott property
            try (PreparedStatement ps = conn.prepareStatement("SELECT Property FROM Expense WHERE Transaction=?")) {
                ps.setString(1, "SE-" + year + "-11111");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(propertyScottId, rs.getInt("Property"));
                }
            }

            // Verify staging flags were marked
            try (PreparedStatement ps = conn.prepareStatement("SELECT IS_PROCESSED FROM Bank_transaction_staging WHERE ID=?")) {
                ps.setInt(1, stagingIdHigh);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("Y", rs.getString(1));
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT IS_PROCESSED FROM Bank_transaction_staging WHERE ID=?")) {
                ps.setInt(1, stagingIdLow);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("Y", rs.getString(1));
                }
            }
        }
    }
}