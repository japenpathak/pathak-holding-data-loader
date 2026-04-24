package com.ph.dl.model;

import com.ph.dl.database.DatabaseManager;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MaintenanceRequest {
    private String ticketId;
    private int property;
    private int unit;
    private String tenant;
    private BigDecimal amount;
    private int year;

    public MaintenanceRequest() {}

    public MaintenanceRequest(String ticketId, int property, int unit, String tenant, BigDecimal amount, int year) {
        this.ticketId = ticketId;
        this.property = property;
        this.unit = unit;
        this.tenant = tenant;
        this.amount = amount;
        this.year = year;
    }

    // Getters and Setters
    public String getTicketId() { return ticketId; }
    public void setTicketId(String ticketId) { this.ticketId = ticketId; }
    public int getProperty() { return property; }
    public void setProperty(int property) { this.property = property; }
    public int getUnit() { return unit; }
    public void setUnit(int unit) { this.unit = unit; }
    public String getTenant() { return tenant; }
    public void setTenant(String tenant) { this.tenant = tenant; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    // Insert
    public void insert() {
        String sql = "INSERT INTO Maintenance_Request (Ticket_Id, Property, Unit, Tenant, Amount, Year) VALUES (?, ?, ?, ?, ?, ?)";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, ticketId);
            pstmt.setInt(2, property);
            pstmt.setInt(3, unit);
            pstmt.setString(4, tenant);
            pstmt.setBigDecimal(5, amount);
            pstmt.setInt(6, year);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Update
    public void update() {
        String sql = "UPDATE Maintenance_Request SET Property = ?, Unit = ?, Tenant = ?, Amount = ?, Year = ? WHERE Ticket_Id = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, property);
            pstmt.setInt(2, unit);
            pstmt.setString(3, tenant);
            pstmt.setBigDecimal(4, amount);
            pstmt.setInt(5, year);
            pstmt.setString(6, ticketId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Find by ID
    public static MaintenanceRequest findById(String ticketId) {
        String sql = "SELECT * FROM Maintenance_Request WHERE Ticket_Id = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, ticketId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                MaintenanceRequest mr = new MaintenanceRequest();
                mr.ticketId = rs.getString("Ticket_Id");
                mr.property = rs.getInt("Property");
                mr.unit = rs.getInt("Unit");
                mr.tenant = rs.getString("Tenant");
                mr.amount = rs.getBigDecimal("Amount");
                mr.year = rs.getInt("Year");
                return mr;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Find all
    public static List<MaintenanceRequest> findAll() {
        List<MaintenanceRequest> list = new ArrayList<>();
        String sql = "SELECT * FROM Maintenance_Request";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                MaintenanceRequest mr = new MaintenanceRequest();
                mr.ticketId = rs.getString("Ticket_Id");
                mr.property = rs.getInt("Property");
                mr.unit = rs.getInt("Unit");
                mr.tenant = rs.getString("Tenant");
                mr.amount = rs.getBigDecimal("Amount");
                mr.year = rs.getInt("Year");
                list.add(mr);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}