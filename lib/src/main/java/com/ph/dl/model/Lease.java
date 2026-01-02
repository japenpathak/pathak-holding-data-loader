package com.ph.dl.model;

import com.ph.dl.database.DatabaseManager;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class Lease {
    private int leaseNumber; // now the PK
    private List<String> tenants;
    private int property;
    private int unit;
    private Date startDate;
    private Date endDate;
    private int year;

    public Lease() {
        this.tenants = new ArrayList<>();
    }

    public Lease(int leaseNumber, int property, int unit, Date startDate, Date endDate, int year) {
        this.leaseNumber = leaseNumber;
        this.tenants = new ArrayList<>();
        this.property = property;
        this.unit = unit;
        this.startDate = startDate;
        this.endDate = endDate;
        this.year = year;
    }

    // Getters and Setters
    public int getLeaseNumber() { return leaseNumber; }
    public void setLeaseNumber(int leaseNumber) { this.leaseNumber = leaseNumber; }
    public List<String> getTenants() { return tenants; }
    public void setTenants(List<String> tenants) { this.tenants = tenants; }
    public int getProperty() { return property; }
    public void setProperty(int property) { this.property = property; }
    public int getUnit() { return unit; }
    public void setUnit(int unit) { this.unit = unit; }
    public Date getStartDate() { return startDate; }
    public void setStartDate(Date startDate) { this.startDate = startDate; }
    public Date getEndDate() { return endDate; }
    public void setEndDate(Date endDate) { this.endDate = endDate; }
    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    // Insert
    public void insert() {
        String sql = "INSERT INTO Lease (Lease_Number, Property, Unit, Start_date, End_date, Year) VALUES (?, ?, ?, ?, ?, ?)";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, leaseNumber);
            pstmt.setInt(2, property);
            pstmt.setInt(3, unit);
            pstmt.setDate(4, startDate);
            pstmt.setDate(5, endDate);
            pstmt.setInt(6, year);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Update
    public void update() {
        String sql = "UPDATE Lease SET Property = ?, Unit = ?, Start_date = ?, End_date = ?, Year = ? WHERE Lease_Number = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, property);
            pstmt.setInt(2, unit);
            pstmt.setDate(3, startDate);
            pstmt.setDate(4, endDate);
            pstmt.setInt(5, year);
            pstmt.setInt(6, leaseNumber);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Find by ID (lease number)
    public static Lease findById(int leaseNumber) {
        String sql = "SELECT * FROM Lease WHERE Lease_Number = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, leaseNumber);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Lease l = new Lease();
                l.leaseNumber = rs.getInt("Lease_Number");
                l.property = rs.getInt("Property");
                l.unit = rs.getInt("Unit");
                l.startDate = rs.getDate("Start_date");
                l.endDate = rs.getDate("End_date");
                l.year = rs.getInt("Year");
                l.loadTenants();
                return l;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Find all
    public static List<Lease> findAll() {
        List<Lease> list = new ArrayList<>();
        String sql = "SELECT * FROM Lease";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Lease l = new Lease();
                l.leaseNumber = rs.getInt("Lease_Number");
                l.property = rs.getInt("Property");
                l.unit = rs.getInt("Unit");
                l.startDate = rs.getDate("Start_date");
                l.endDate = rs.getDate("End_date");
                l.year = rs.getInt("Year");
                l.loadTenants();
                list.add(l);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    // Get all leases by number
    public static Map<String, Lease> getAllLeasesByNumber() {
        Map<String, Lease> map = new HashMap<>();
        String sql = "SELECT Lease_Number, Property, Unit, Start_Date, End_Date, Year FROM Lease";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                int leaseNum = rs.getInt("Lease_Number");
                if (!rs.wasNull()) {
                    Lease l = new Lease(leaseNum, rs.getInt("Property"), rs.getInt("Unit"), rs.getDate("Start_Date"), rs.getDate("End_Date"), rs.getInt("Year"));
                    map.put(String.valueOf(leaseNum), l);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    // Load tenants from Lease_Tenant table
    private void loadTenants() {
        tenants = new ArrayList<>();
        String sql = "SELECT Tenant_Id FROM Lease_Tenant WHERE Lease_Id = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, leaseNumber);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                tenants.add(String.valueOf(rs.getInt("Tenant_Id")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Add tenant to lease
    public void addTenant(String tenantId) {
        if (!tenants.contains(tenantId)) {
            String sql = "INSERT INTO Lease_Tenant (Lease_Id, Tenant_Id) VALUES (?, ?)";
            try (Connection conn = DatabaseManager.getInstance().getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, leaseNumber);
                pstmt.setInt(2, Integer.parseInt(tenantId));
                pstmt.executeUpdate();
                tenants.add(tenantId);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // Get lease summary
    public Map<String, Object> getSummary() {
        Map<String, Object> summary = new HashMap<>();
        summary.put("Lease_Number", leaseNumber);
        summary.put("Property", property);
        summary.put("Unit", unit);
        summary.put("Start_date", startDate);
        summary.put("End_date", endDate);
        summary.put("Year", year);
        summary.put("Tenants", tenants);
        return summary;
    }
}