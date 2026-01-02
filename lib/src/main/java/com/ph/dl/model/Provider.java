package com.ph.dl.model;

import com.ph.dl.database.DatabaseManager;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Provider {
    private int providerId;
    private String providerName;
    private String providerAddress;
    private String email;
    private String phone;
    private String entity;

    public Provider() {}

    public Provider(String providerName, String providerAddress, String email, String phone, String entity) {
        this.providerName = providerName;
        this.providerAddress = providerAddress;
        this.email = email;
        this.phone = phone;
        this.entity = entity;
    }

    // Getters and Setters
    public int getProviderId() { return providerId; }
    public void setProviderId(int providerId) { this.providerId = providerId; }
    public String getProviderName() { return providerName; }
    public void setProviderName(String providerName) { this.providerName = providerName; }
    public String getProviderAddress() { return providerAddress; }
    public void setProviderAddress(String providerAddress) { this.providerAddress = providerAddress; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }
    public String getEntity() { return entity; }
    public void setEntity(String entity) { this.entity = entity; }

    // Insert
    public void insert() {
        String sql = "INSERT INTO Provider (Provider_Name, Provider_Address, Email, Phone, Entity) VALUES (?, ?, ?, ?, ?)";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, providerName);
            pstmt.setString(2, providerAddress);
            pstmt.setString(3, email);
            pstmt.setString(4, phone);
            pstmt.setString(5, entity);
            pstmt.executeUpdate();
            ResultSet rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                providerId = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Update
    public void update() {
        String sql = "UPDATE Provider SET Provider_Name = ?, Provider_Address = ?, Email = ?, Phone = ?, Entity = ? WHERE Provider_Id = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, providerName);
            pstmt.setString(2, providerAddress);
            pstmt.setString(3, email);
            pstmt.setString(4, phone);
            pstmt.setString(5, entity);
            pstmt.setInt(6, providerId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Find by ID
    public static Provider findById(int providerId) {
        String sql = "SELECT * FROM Provider WHERE Provider_Id = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, providerId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Provider p = new Provider();
                p.providerId = rs.getInt("Provider_Id");
                p.providerName = rs.getString("Provider_Name");
                p.providerAddress = rs.getString("Provider_Address");
                p.email = rs.getString("Email");
                p.phone = rs.getString("Phone");
                p.entity = rs.getString("Entity");
                return p;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Find all
    public static List<Provider> findAll() {
        List<Provider> list = new ArrayList<>();
        String sql = "SELECT * FROM Provider";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Provider p = new Provider();
                p.providerId = rs.getInt("Provider_Id");
                p.providerName = rs.getString("Provider_Name");
                p.providerAddress = rs.getString("Provider_Address");
                p.email = rs.getString("Email");
                p.phone = rs.getString("Phone");
                p.entity = rs.getString("Entity");
                list.add(p);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}