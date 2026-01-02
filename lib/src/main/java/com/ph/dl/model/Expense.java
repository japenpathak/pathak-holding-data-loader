package com.ph.dl.model;

import com.ph.dl.database.DatabaseManager;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Expense {
    private String transaction;
    private Date date;
    private BigDecimal amount;
    private String details;
    private String paymentMode;
    private String checkNum;
    private String paymentAccount;
    private int property;
    private int unit;
    private int category;
    private int provider;
    private String type;
    private String exclude;
    private String excludeZaky;
    private int year;

    public Expense() {}

    // Constructor
    public Expense(String transaction, Date date, BigDecimal amount, String details, String paymentMode, String checkNum, String paymentAccount, int property, int unit, int category, int provider, String type, String exclude, String excludeZaky, int year) {
        this.transaction = transaction;
        this.date = date;
        this.amount = amount;
        this.details = details;
        this.paymentMode = paymentMode;
        this.checkNum = checkNum;
        this.paymentAccount = paymentAccount;
        this.property = property;
        this.unit = unit;
        this.category = category;
        this.provider = provider;
        this.type = type;
        this.exclude = exclude;
        this.excludeZaky = excludeZaky;
        this.year = year;
    }

    // Getters and Setters
    public String getTransaction() { return transaction; }
    public void setTransaction(String transaction) { this.transaction = transaction; }
    public Date getDate() { return date; }
    public void setDate(Date date) { this.date = date; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getDetails() { return details; }
    public void setDetails(String details) { this.details = details; }
    public String getPaymentMode() { return paymentMode; }
    public void setPaymentMode(String paymentMode) { this.paymentMode = paymentMode; }
    public String getCheckNum() { return checkNum; }
    public void setCheckNum(String checkNum) { this.checkNum = checkNum; }
    public String getPaymentAccount() { return paymentAccount; }
    public void setPaymentAccount(String paymentAccount) { this.paymentAccount = paymentAccount; }
    public int getProperty() { return property; }
    public void setProperty(int property) { this.property = property; }
    public int getUnit() { return unit; }
    public void setUnit(int unit) { this.unit = unit; }
    public int getCategory() { return category; }
    public void setCategory(int category) { this.category = category; }
    public int getProvider() { return provider; }
    public void setProvider(int provider) { this.provider = provider; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getExclude() { return exclude; }
    public void setExclude(String exclude) { this.exclude = exclude; }
    public String getExcludeZaky() { return excludeZaky; }
    public void setExcludeZaky(String excludeZaky) { this.excludeZaky = excludeZaky; }
    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    // Insert
    public void insert() {
        String sql = "INSERT INTO Expense (Transaction, Date, Amount, Details, Payment_Mode, Check_num, Payment_Account, Property, Unit, Category, Provider, Type, Exclude, Exclude_Zaky, Year) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, transaction);
            pstmt.setDate(2, date);
            pstmt.setBigDecimal(3, amount);
            pstmt.setString(4, details);
            pstmt.setString(5, paymentMode);
            pstmt.setString(6, checkNum);
            pstmt.setString(7, paymentAccount);
            pstmt.setInt(8, property);
            pstmt.setInt(9, unit);
            pstmt.setInt(10, category);
            pstmt.setInt(11, provider);
            pstmt.setString(12, type);
            pstmt.setString(13, exclude);
            pstmt.setString(14, excludeZaky);
            pstmt.setInt(15, year);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Update
    public void update() {
        String sql = "UPDATE Expense SET Date = ?, Amount = ?, Details = ?, Payment_Mode = ?, Check_num = ?, Payment_Account = ?, Property = ?, Unit = ?, Category = ?, Provider = ?, Type = ?, Exclude = ?, Exclude_Zaky = ?, Year = ? WHERE Transaction = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDate(1, date);
            pstmt.setBigDecimal(2, amount);
            pstmt.setString(3, details);
            pstmt.setString(4, paymentMode);
            pstmt.setString(5, checkNum);
            pstmt.setString(6, paymentAccount);
            pstmt.setInt(7, property);
            pstmt.setInt(8, unit);
            pstmt.setInt(9, category);
            pstmt.setInt(10, provider);
            pstmt.setString(11, type);
            pstmt.setString(12, exclude);
            pstmt.setString(13, excludeZaky);
            pstmt.setInt(14, year);
            pstmt.setString(15, transaction);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Find by ID
    public static Expense findById(String transaction) {
        String sql = "SELECT * FROM Expense WHERE Transaction = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, transaction);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Expense e = new Expense();
                e.transaction = rs.getString("Transaction");
                e.date = rs.getDate("Date");
                e.amount = rs.getBigDecimal("Amount");
                e.details = rs.getString("Details");
                e.paymentMode = rs.getString("Payment_Mode");
                e.checkNum = rs.getString("Check_num");
                e.paymentAccount = rs.getString("Payment_Account");
                e.property = rs.getInt("Property");
                e.unit = rs.getInt("Unit");
                e.category = rs.getInt("Category");
                e.provider = rs.getInt("Provider");
                e.type = rs.getString("Type");
                e.exclude = rs.getString("Exclude");
                e.excludeZaky = rs.getString("Exclude_Zaky");
                e.year = rs.getInt("Year");
                return e;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Find all
    public static List<Expense> findAll() {
        List<Expense> list = new ArrayList<>();
        String sql = "SELECT * FROM Expense";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Expense e = new Expense();
                e.transaction = rs.getString("Transaction");
                e.date = rs.getDate("Date");
                e.amount = rs.getBigDecimal("Amount");
                e.details = rs.getString("Details");
                e.paymentMode = rs.getString("Payment_Mode");
                e.checkNum = rs.getString("Check_num");
                e.paymentAccount = rs.getString("Payment_Account");
                e.property = rs.getInt("Property");
                e.unit = rs.getInt("Unit");
                e.category = rs.getInt("Category");
                e.provider = rs.getInt("Provider");
                e.type = rs.getString("Type");
                e.exclude = rs.getString("Exclude");
                e.excludeZaky = rs.getString("Exclude_Zaky");
                e.year = rs.getInt("Year");
                list.add(e);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}