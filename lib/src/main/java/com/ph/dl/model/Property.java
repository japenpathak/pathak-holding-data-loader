package com.ph.dl.model;

import com.ph.dl.database.DatabaseManager;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Property {
    private int id;
    private String name;
    private String fullAddress;
    private int numberOfUnits;
    private Date acquiredDate;

    public Property() {}

    public Property(String name, String fullAddress, int numberOfUnits) {
        this.name = name;
        this.fullAddress = fullAddress;
        this.numberOfUnits = numberOfUnits;
    }

    public Property(String name, String fullAddress, int numberOfUnits, Date acquiredDate) {
        this.name = name;
        this.fullAddress = fullAddress;
        this.numberOfUnits = numberOfUnits;
        this.acquiredDate = acquiredDate;
    }

    // Getters and Setters
    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getFullAddress() { return fullAddress; }
    public void setFullAddress(String fullAddress) { this.fullAddress = fullAddress; }
    public int getNumberOfUnits() { return numberOfUnits; }
    public void setNumberOfUnits(int numberOfUnits) { this.numberOfUnits = numberOfUnits; }
    public Date getAcquiredDate() { return acquiredDate; }
    public void setAcquiredDate(Date acquiredDate) { this.acquiredDate = acquiredDate; }

    // Insert
    public void insert() {
        String sql = "INSERT INTO Property (Name, Full_Address, Number_of_units, Acquired_Date) VALUES (?, ?, ?, ?)";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, name);
            pstmt.setString(2, fullAddress);
            pstmt.setInt(3, numberOfUnits);
            pstmt.setDate(4, acquiredDate);
            pstmt.executeUpdate();
            ResultSet rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                id = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Update
    public void update() {
        String sql = "UPDATE Property SET Name = ?, Full_Address = ?, Number_of_units = ?, Acquired_Date = ? WHERE ID = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, name);
            pstmt.setString(2, fullAddress);
            pstmt.setInt(3, numberOfUnits);
            pstmt.setDate(4, acquiredDate);
            pstmt.setInt(5, id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Find by ID
    public static Property findById(int id) {
        String sql = "SELECT * FROM Property WHERE ID = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, id);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Property p = new Property();
                p.id = rs.getInt("ID");
                p.name = rs.getString("Name");
                p.fullAddress = rs.getString("Full_Address");
                p.numberOfUnits = rs.getInt("Number_of_units");
                p.acquiredDate = rs.getDate("Acquired_Date");
                return p;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Find all
    public static List<Property> findAll() {
        List<Property> list = new ArrayList<>();
        String sql = "SELECT * FROM Property";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Property p = new Property();
                p.id = rs.getInt("ID");
                p.name = rs.getString("Name");
                p.fullAddress = rs.getString("Full_Address");
                p.numberOfUnits = rs.getInt("Number_of_units");
                p.acquiredDate = rs.getDate("Acquired_Date");
                list.add(p);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}