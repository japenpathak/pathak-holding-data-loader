package com.ph.dl.model;

import com.ph.dl.database.DatabaseManager;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Units {
    private int id;
    private String name;
    private int property;
    private int bedrooms;
    private int bathrooms;
    private String hasPorch;
    private String hasBackYard;
    private String washerDryer;
    private String isUnitGroup;
    private Integer groupReference;

    public Units() {}

    public Units(String name, int property, int bedrooms, int bathrooms, String hasPorch, String hasBackYard, String washerDryer) {
        this.name = name;
        this.property = property;
        this.bedrooms = bedrooms;
        this.bathrooms = bathrooms;
        this.hasPorch = hasPorch;
        this.hasBackYard = hasBackYard;
        this.washerDryer = washerDryer;
    }

    // Getters and Setters
    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public int getProperty() { return property; }
    public void setProperty(int property) { this.property = property; }
    public int getBedrooms() { return bedrooms; }
    public void setBedrooms(int bedrooms) { this.bedrooms = bedrooms; }
    public int getBathrooms() { return bathrooms; }
    public void setBathrooms(int bathrooms) { this.bathrooms = bathrooms; }
    public String getHasPorch() { return hasPorch; }
    public void setHasPorch(String hasPorch) { this.hasPorch = hasPorch; }
    public String getHasBackYard() { return hasBackYard; }
    public void setHasBackYard(String hasBackYard) { this.hasBackYard = hasBackYard; }
    public String getWasherDryer() { return washerDryer; }
    public void setWasherDryer(String washerDryer) { this.washerDryer = washerDryer; }
    public String getIsUnitGroup() { return isUnitGroup; }
    public void setIsUnitGroup(String isUnitGroup) { this.isUnitGroup = isUnitGroup; }
    public Integer getGroupReference() { return groupReference; }
    public void setGroupReference(Integer groupReference) { this.groupReference = groupReference; }

    // Insert
    public void insert() {
        String sql = "INSERT INTO Units (Name, Property, Bedrooms, Bathrooms, Has_Porch, Has_Back_Yard, Washer_Dryer, IS_Unit_Group, Group_reference) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, name);
            pstmt.setInt(2, property);
            pstmt.setInt(3, bedrooms);
            pstmt.setInt(4, bathrooms);
            pstmt.setString(5, hasPorch);
            pstmt.setString(6, hasBackYard);
            pstmt.setString(7, washerDryer);
            pstmt.setString(8, isUnitGroup);
            if (groupReference == null) {
                pstmt.setNull(9, java.sql.Types.INTEGER);
            } else {
                pstmt.setInt(9, groupReference);
            }
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
        String sql = "UPDATE Units SET Name = ?, Property = ?, Bedrooms = ?, Bathrooms = ?, Has_Porch = ?, Has_Back_Yard = ?, Washer_Dryer = ?, IS_Unit_Group = ?, Group_reference = ? WHERE ID = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, name);
            pstmt.setInt(2, property);
            pstmt.setInt(3, bedrooms);
            pstmt.setInt(4, bathrooms);
            pstmt.setString(5, hasPorch);
            pstmt.setString(6, hasBackYard);
            pstmt.setString(7, washerDryer);
            pstmt.setString(8, isUnitGroup);
            if (groupReference == null) {
                pstmt.setNull(9, java.sql.Types.INTEGER);
            } else {
                pstmt.setInt(9, groupReference);
            }
            pstmt.setInt(10, id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Find by ID
    public static Units findById(int id) {
        String sql = "SELECT * FROM Units WHERE ID = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, id);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Units u = new Units();
                u.id = rs.getInt("ID");
                u.name = rs.getString("Name");
                u.property = rs.getInt("Property");
                u.bedrooms = rs.getInt("Bedrooms");
                u.bathrooms = rs.getInt("Bathrooms");
                u.hasPorch = rs.getString("Has_Porch");
                u.hasBackYard = rs.getString("Has_Back_Yard");
                u.washerDryer = rs.getString("Washer_Dryer");
                u.isUnitGroup = rs.getString("IS_Unit_Group");
                int gr = rs.getInt("Group_reference");
                u.groupReference = rs.wasNull() ? null : gr;
                return u;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Find all
    public static List<Units> findAll() {
        List<Units> list = new ArrayList<>();
        String sql = "SELECT * FROM Units";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Units u = new Units();
                u.id = rs.getInt("ID");
                u.name = rs.getString("Name");
                u.property = rs.getInt("Property");
                u.bedrooms = rs.getInt("Bedrooms");
                u.bathrooms = rs.getInt("Bathrooms");
                u.hasPorch = rs.getString("Has_Porch");
                u.hasBackYard = rs.getString("Has_Back_Yard");
                u.washerDryer = rs.getString("Washer_Dryer");
                u.isUnitGroup = rs.getString("IS_Unit_Group");
                int gr = rs.getInt("Group_reference");
                u.groupReference = rs.wasNull() ? null : gr;
                list.add(u);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}