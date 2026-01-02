package com.ph.dl.model;

import com.ph.dl.database.DatabaseManager;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class BankPayment {
    private String transactionId;
    private Date date;
    private int property;
    private String bank;
    private BigDecimal totalAmount;
    private BigDecimal interest;
    private BigDecimal principal;
    private int year;

    public BankPayment() {}

    public BankPayment(String transactionId, Date date, int property, String bank, BigDecimal totalAmount, BigDecimal interest, BigDecimal principal, int year) {
        this.transactionId = transactionId;
        this.date = date;
        this.property = property;
        this.bank = bank;
        this.totalAmount = totalAmount;
        this.interest = interest;
        this.principal = principal;
        this.year = year;
    }

    // Getters and Setters
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public Date getDate() { return date; }
    public void setDate(Date date) { this.date = date; }
    public int getProperty() { return property; }
    public void setProperty(int property) { this.property = property; }
    public String getBank() { return bank; }
    public void setBank(String bank) { this.bank = bank; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }
    public BigDecimal getInterest() { return interest; }
    public void setInterest(BigDecimal interest) { this.interest = interest; }
    public BigDecimal getPrincipal() { return principal; }
    public void setPrincipal(BigDecimal principal) { this.principal = principal; }
    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    // Insert
    public void insert() {
        String sql = "INSERT INTO Bank_Payment (Transaction_Id, Date, Property, Bank, Total_Amount, Interest, Principal, Year) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, transactionId);
            pstmt.setDate(2, date);
            pstmt.setInt(3, property);
            pstmt.setString(4, bank);
            pstmt.setBigDecimal(5, totalAmount);
            pstmt.setBigDecimal(6, interest);
            pstmt.setBigDecimal(7, principal);
            pstmt.setInt(8, year);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Update
    public void update() {
        String sql = "UPDATE Bank_Payment SET Date = ?, Property = ?, Bank = ?, Total_Amount = ?, Interest = ?, Principal = ?, Year = ? WHERE Transaction_Id = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDate(1, date);
            pstmt.setInt(2, property);
            pstmt.setString(3, bank);
            pstmt.setBigDecimal(4, totalAmount);
            pstmt.setBigDecimal(5, interest);
            pstmt.setBigDecimal(6, principal);
            pstmt.setInt(7, year);
            pstmt.setString(8, transactionId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Find by ID
    public static BankPayment findById(String transactionId) {
        String sql = "SELECT * FROM Bank_Payment WHERE Transaction_Id = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, transactionId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                BankPayment bp = new BankPayment();
                bp.transactionId = rs.getString("Transaction_Id");
                bp.date = rs.getDate("Date");
                bp.property = rs.getInt("Property");
                bp.bank = rs.getString("Bank");
                bp.totalAmount = rs.getBigDecimal("Total_Amount");
                bp.interest = rs.getBigDecimal("Interest");
                bp.principal = rs.getBigDecimal("Principal");
                bp.year = rs.getInt("Year");
                return bp;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Find all
    public static List<BankPayment> findAll() {
        List<BankPayment> list = new ArrayList<>();
        String sql = "SELECT * FROM Bank_Payment";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                BankPayment bp = new BankPayment();
                bp.transactionId = rs.getString("Transaction_Id");
                bp.date = rs.getDate("Date");
                bp.property = rs.getInt("Property");
                bp.bank = rs.getString("Bank");
                bp.totalAmount = rs.getBigDecimal("Total_Amount");
                bp.interest = rs.getBigDecimal("Interest");
                bp.principal = rs.getBigDecimal("Principal");
                bp.year = rs.getInt("Year");
                list.add(bp);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}