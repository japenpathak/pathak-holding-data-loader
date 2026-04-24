package com.ph.dl.service;

import com.ph.dl.database.DatabaseManager;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;

public class VacancyLossGeneratorTest {

    @Test
    void generatesVacancyRowsForMissingRentMonths() throws Exception {
        // Ensure DB is initialized
        DatabaseManager db = DatabaseManager.getInstance();

        int year = 2025;
        int propertyId = 1;
        int unitId = 101;

        // Reset tables to isolate this test (DatabaseManager may keep a persistent DB across tests)
        try (Connection conn = db.getConnection()) {
            conn.setAutoCommit(false);
            // Order matters due to FKs (delete children first)
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Income")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Expense")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Bank_transaction_staging")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Bank_Payment")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM File_Processing")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Lease_Tenant")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Lease")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Units")) { ps.executeUpdate(); }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Property")) { ps.executeUpdate(); }
            conn.commit();
        }

        // Seed minimal required rows
        try (Connection conn = db.getConnection()) {
            conn.setAutoCommit(false);

            // Property + Unit
            try (PreparedStatement ps = conn.prepareStatement("MERGE INTO Property (ID, Name, Full_Address, Number_of_units, Acquired_Date) KEY(ID) VALUES (?,?,?,?,?)")) {
                ps.setInt(1, propertyId);
                ps.setString(2, "P1");
                ps.setString(3, "A1");
                ps.setInt(4, 1);
                ps.setNull(5, java.sql.Types.DATE);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("MERGE INTO Units (ID, Name, Property, Bedrooms, Bathrooms, Has_Porch, Has_Back_Yard, Washer_Dryer, IS_Unit_Group, Group_reference) KEY(ID) VALUES (?,?,?,?,?,?,?,?,?,?)")) {
                ps.setInt(1, unitId);
                ps.setString(2, "U1");
                ps.setInt(3, propertyId);
                ps.setInt(4, 1);
                ps.setInt(5, 1);
                ps.setString(6, "N");
                ps.setString(7, "N");
                ps.setString(8, "N");
                ps.setString(9, "N");
                ps.setNull(10, java.sql.Types.INTEGER);
                ps.executeUpdate();
            }

            // Lease for FK (Income.LeaseNum -> Lease.Lease_Number)
            try (PreparedStatement ps = conn.prepareStatement("MERGE INTO Lease (Lease_Number, Property, Unit, Start_date, End_date, Year) KEY(Lease_Number) VALUES (?,?,?,?,?,?)")) {
                ps.setInt(1, 7);
                ps.setInt(2, propertyId);
                ps.setInt(3, unitId);
                ps.setDate(4, java.sql.Date.valueOf(year + "-01-01"));
                ps.setDate(5, java.sql.Date.valueOf(year + "-12-31"));
                ps.setInt(6, year);
                ps.executeUpdate();
            }

            // Insert rent for January only (so 11 months missing)
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO Income (Transaction_ID, Status, Date_created, Due_date, Date_paid, Type, Transaction_category, Rent_Month, Original_amount, Payment, Balance, Vacancy, Method_of_payment, Payer, LeaseNum, Property, Unit, Transaction_details, Exclude_Zaky, SD_Applied, Year) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                ps.setString(1, year + "-TXN-1");
                ps.setString(2, "Paid");
                ps.setDate(3, java.sql.Date.valueOf(year + "-01-01"));
                ps.setDate(4, java.sql.Date.valueOf(year + "-01-01"));
                ps.setDate(5, java.sql.Date.valueOf(year + "-01-05"));
                ps.setString(6, "Income");
                ps.setString(7, "Rent");
                ps.setString(8, "January");
                ps.setBigDecimal(9, new java.math.BigDecimal("1000.00"));
                ps.setBigDecimal(10, new java.math.BigDecimal("1000.00"));
                ps.setBigDecimal(11, new java.math.BigDecimal("0.00"));
                ps.setBigDecimal(12, new java.math.BigDecimal("0.00"));
                ps.setString(13, "ACH");
                ps.setNull(14, java.sql.Types.INTEGER);
                ps.setInt(15, 7);
                ps.setInt(16, propertyId);
                ps.setInt(17, unitId);
                ps.setString(18, "");
                ps.setString(19, "");
                ps.setString(20, "");
                ps.setInt(21, year);
                ps.executeUpdate();
            }

            conn.commit();
        }

        // Run vacancy generator
        VacancyLossService service = new VacancyLossService();
        try (Connection conn = db.getConnection()) {
            conn.setAutoCommit(false);
            int inserted = service.generateForYear(conn, year);
            conn.commit();

            assertEquals(11, inserted);

            // Verify those rows exist and have expected shape
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM Income WHERE Year=? AND Status='Vacancy' AND Property=? AND Unit=? AND Transaction_category='Rent'")) {
                ps.setInt(1, year);
                ps.setInt(2, propertyId);
                ps.setInt(3, unitId);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(11, rs.getInt(1));
                }
            }

            // Check transaction id pattern and payer/lease/method are null
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT Transaction_ID, Payer, LeaseNum, Method_of_payment, Vacancy, Original_amount, Date_created, Due_date, Date_paid FROM Income WHERE Transaction_ID LIKE ?")) {
                ps.setString(1, year + "-Vacant-%");
                try (ResultSet rs = ps.executeQuery()) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                        assertNotNull(rs.getString(1));
                        assertTrue(rs.getString(1).startsWith(year + "-Vacant-"));

                        rs.getInt(2);
                        assertTrue(rs.wasNull(), "Payer should be NULL");
                        rs.getInt(3);
                        assertTrue(rs.wasNull(), "LeaseNum should be NULL");

                        assertNull(rs.getString(4));
                        assertEquals(rs.getBigDecimal(6), rs.getBigDecimal(5));
                        assertEquals(rs.getDate(7), rs.getDate(8));
                        assertNull(rs.getDate(9));
                    }
                    assertEquals(11, count);
                }
            }
        }
    }
}