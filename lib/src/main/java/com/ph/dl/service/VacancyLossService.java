package com.ph.dl.service;

import com.ph.dl.database.DatabaseManager;
import com.ph.dl.model.Income;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Generates synthetic Income rows representing vacancy loss.
 */
@Component
public class VacancyLossService {

    @Value("${income.year:0}")
    int year;
    
    @Value("${income.vacancy.recompute:false}")
    boolean recomputeVacancyLoss;

    /**
     * Generate vacancy-loss rows for the configured year.
     */
    public int generateForYear() throws SQLException {
        if (year == 0) {
            throw new IllegalStateException("income.year not configured (check application.yml)");
        }

        try (Connection conn = DatabaseManager.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            try {
                int inserted = generateForYear(conn, year);
                conn.commit();
                return inserted;
            } catch (SQLException ex) {
                conn.rollback();
                throw ex;
            }
        }
    }

    /**
     * Generate vacancy-loss rows for the given year using the provided connection.
     * Callers control transaction boundaries.
     */
    public int generateForYear(Connection conn, int year) throws SQLException {
    	
    	if (recomputeVacancyLoss) {
			String deleteSql = "DELETE FROM Income WHERE Year=? AND Transaction_ID LIKE ?";
			String prefix = year + "-Vacant-%";
			try (PreparedStatement ps = conn.prepareStatement(deleteSql)) {
				ps.setInt(1, year);
				ps.setString(2, prefix);
				ps.executeUpdate();
			}
		}
    	
    	int nextSeq = nextVacantSequence(conn, year);

        int inserted = 0;
        for (int[] pu : distinctPropertyUnitsWithIncome(conn, year)) {
            int propertyId = pu[0];
            int unitId = pu[1];

            BigDecimal baseRent = lowestRentAmount(conn, year, propertyId, unitId);
            if (baseRent == null) {
                continue;
            }

            int startMonth = startMonthAfterAcquisition(conn, year, propertyId);
            if (startMonth > 12) {
                continue;
            }

            boolean[] hasRent = monthsWithCategory(conn, year, propertyId, unitId, "Rent");

            for (int month = startMonth; month <= 12; month++) {
                boolean missingRent = !hasRent[month];
                if (!missingRent) continue;

                java.sql.Date firstOfMonth = java.sql.Date.valueOf(java.time.LocalDate.of(year, month, 1));
                if (Income.vacancyRowExists(conn, year, propertyId, unitId, firstOfMonth)) {
                    continue;
                }

                String txnId = year + "-Vacant-" + (nextSeq++);
                inserted += Income.insertVacancyRow(conn, txnId, year, propertyId, unitId, firstOfMonth, baseRent,
                        Income.calcRentMonth(firstOfMonth));
            }
        }

        return inserted;
    }

    /**
     * Vacancy should be generated starting from the month AFTER the acquisition month.
     *
     * Examples for a given year:
     * - acquiredDate = 2025-03-10 -> startMonth = 4
     * - acquiredDate = 2024-03-10 -> startMonth = 1 (already acquired before the year)
     * - acquiredDate = null -> startMonth = 1
     * - acquiredDate = 2026-01-01 (future) -> startMonth = 13 (no vacancy for this year)
     */
    private int startMonthAfterAcquisition( Connection conn, int year, int propertyId) throws SQLException {
        java.sql.Date acquired = getPropertyAcquiredDate( conn, propertyId);
        if (acquired == null) {
            return 1;
        }

        java.time.LocalDate ad = acquired.toLocalDate();
        if (ad.getYear() < year) {
            return 1;
        }
        if (ad.getYear() > year) {
            return 13;
        }
        // same year, start month is month after acquisition month values 0-11 + 1 for right format  +1 for next month
        return ad.getMonthValue() + 2;
    }

    private  java.sql.Date getPropertyAcquiredDate( Connection conn, int propertyId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT Acquired_Date FROM Property WHERE ID=?")) {
            ps.setInt(1, propertyId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getDate(1);
                }
            }
        }
        return null;
    }

    private  java.util.List<int[]> distinctPropertyUnitsWithIncome( Connection conn, int year) throws SQLException {
        String sql = "SELECT DISTINCT Property, Unit FROM Income WHERE Year = ? AND Property IS NOT NULL AND Unit IS NOT NULL";
        java.util.ArrayList<int[]> out = new java.util.ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, year);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new int[]{rs.getInt(1), rs.getInt(2)});
                }
            }
        }
        return out;
    }

    private  int nextVacantSequence( Connection conn, int year) throws SQLException {
        String sql = "SELECT MAX(CAST(SUBSTRING(Transaction_ID, LENGTH(?) + 1) AS INT)) FROM Income WHERE Transaction_ID LIKE ?";
        String prefix = year + "-Vacant-";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, prefix);
            ps.setString(2, prefix + "%");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int max = rs.getInt(1);
                    if (rs.wasNull()) return 1;
                    return max + 1;
                }
            }
        }
        return 1;
    }

    private  BigDecimal lowestRentAmount( Connection conn, int year, int propertyId, int unitId) throws SQLException {
        String sql = "SELECT MIN(Original_amount) FROM Income WHERE Year=? AND Property=? AND Unit=? AND Transaction_category='Rent' AND Original_amount IS NOT NULL";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, year);
            ps.setInt(2, propertyId);
            ps.setInt(3, unitId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getBigDecimal(1);
            }
        }
        return null;
    }

    private  boolean[] monthsWithCategory( Connection conn, int year, int propertyId, int unitId, String category) throws SQLException {
        boolean[] present = new boolean[13];
        String sql = "SELECT Due_date FROM Income WHERE Year=? AND Property=? AND Unit=? AND Transaction_category=? AND Due_date IS NOT NULL";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, year);
            ps.setInt(2, propertyId);
            ps.setInt(3, unitId);
            ps.setString(4, category);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    java.sql.Date d = rs.getDate(1);
                    if (d == null) continue;
                    int m = d.toLocalDate().getMonthValue();
                    if (m >= 1 && m <= 12) present[m] = true;
                }
            }
        }
        return present;
    }
}