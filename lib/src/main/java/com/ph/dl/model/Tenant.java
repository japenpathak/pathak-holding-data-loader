package com.ph.dl.model;

import com.ph.dl.database.DatabaseManager;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class Tenant {
    private int tenantId; // now int to match INT Tenant_ID
    private String name;
    private String email;
    private String phone;

    public Tenant() {}

    public Tenant(Integer tenantId, String name, String email, String phone) {
        this.tenantId = tenantId == null ? 0 : tenantId;
        this.name = name;
        this.email = email;
        this.phone = phone;
    }

    // Getters and Setters
    public int getTenantId() { return tenantId; }
    public void setTenantId(int tenantId) { this.tenantId = tenantId; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }

    // Insert
    public void insert() {
        String sql = "INSERT INTO Tenant (Tenant_ID, Name, Email, Phone) VALUES (?, ?, ?, ?)";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            // If tenantId not set or zero, generate a new one within this connection/transaction
            if (tenantId == 0) {
                this.tenantId = getNextTenantId(conn);
            }
            pstmt.setInt(1, tenantId);
            pstmt.setString(2, name);
            pstmt.setString(3, email);
            pstmt.setString(4, phone);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Update
    public void update() {
        String sql = "UPDATE Tenant SET Name = ?, Email = ?, Phone = ? WHERE Tenant_ID = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, name);
            pstmt.setString(2, email);
            pstmt.setString(3, phone);
            pstmt.setInt(4, tenantId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Find by ID
    public static Tenant findById(int tenantId) {
        String sql = "SELECT * FROM Tenant WHERE Tenant_ID = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, tenantId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Tenant t = new Tenant();
                t.tenantId = rs.getInt("Tenant_ID");
                t.name = rs.getString("Name");
                t.email = rs.getString("Email");
                t.phone = rs.getString("Phone");
                return t;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Find by Email
    public static Tenant findByEmail(String email) {
        String sql = "SELECT * FROM Tenant WHERE Email = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, email);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Tenant t = new Tenant();
                t.tenantId = rs.getInt("Tenant_ID");
                t.name = rs.getString("Name");
                t.email = rs.getString("Email");
                t.phone = rs.getString("Phone");
                return t;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Find all
    public static List<Tenant> findAll() {
        List<Tenant> list = new ArrayList<>();
        String sql = "SELECT * FROM Tenant";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Tenant t = new Tenant();
                t.tenantId = rs.getInt("Tenant_ID");
                t.name = rs.getString("Name");
                t.email = rs.getString("Email");
                t.phone = rs.getString("Phone");
                list.add(t);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    // Get all tenants by email
    public static Map<String, Tenant> getAllTenantsByEmail() {
        Map<String, Tenant> map = new HashMap<>();
        String sql = "SELECT Tenant_ID, Name, Email, Phone FROM Tenant";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String email = rs.getString("Email");
                if (email != null && !email.isEmpty()) {
                    Tenant t = new Tenant(rs.getInt("Tenant_ID"), rs.getString("Name"), email, rs.getString("Phone"));
                    map.put(email, t);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    // Get next tenant ID using a new connection (backwards compatible, but prefer the Connection-based overload)
    public static int getNextTenantId() {
        try (Connection conn = DatabaseManager.getInstance().getConnection()) {
            return getNextTenantId(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 1;
    }

    // Get next tenant ID within an existing connection/transaction
    private static int getNextTenantId(Connection conn) throws SQLException {
        String sql = "SELECT MAX(Tenant_ID) FROM Tenant";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                int max = rs.getInt(1);
                if (rs.wasNull()) return 1;
                return max + 1;
            }
        }
        return 1;
    }

    // Convert to Map
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        map.put("Tenant_ID", String.valueOf(tenantId));
        map.put("Name", name);
        map.put("Email", email);
        map.put("Phone", phone);
        return map;
    }
}