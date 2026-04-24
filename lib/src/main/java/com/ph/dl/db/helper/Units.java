package com.ph.dl.db.helper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * DB helper methods around Property / Units lookup and placeholder creation.
 *
 * Note: this is intentionally a simple JDBC helper to match the rest of the codebase
 * (models/services directly use JDBC via DatabaseManager).
 */
public final class Units {

    private static final int PLACEHOLDER_PROPERTY_ID = 0;

    private Units() {
        // utility
    }

    public static int resolvePropertyId(Connection conn, String name) throws SQLException {
        if (name == null || name.isBlank()) {
            return ensurePlaceholderProperty(conn);
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT ID FROM Property WHERE LOWER(Name)=LOWER(?)")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return ensurePlaceholderProperty(conn);
    }

    public static int ensurePlaceholderProperty(Connection conn) throws SQLException {
        // Use a stable explicit ID (Property.ID is not auto-increment in this schema)
        try (PreparedStatement ps = conn.prepareStatement("SELECT ID FROM Property WHERE ID=?")) {
            ps.setInt(1, PLACEHOLDER_PROPERTY_ID);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        try (PreparedStatement ins = conn.prepareStatement(
                "INSERT INTO Property (ID, Name, Full_Address, Number_of_units) VALUES (?, ?, ?, ?)")) {
            ins.setInt(1, PLACEHOLDER_PROPERTY_ID);
            ins.setString(2, "Unknown Property");
            ins.setString(3, "");
            ins.setInt(4, 0);
            ins.executeUpdate();
            return PLACEHOLDER_PROPERTY_ID;
        }
    }

    /**
     * Resolve a Unit id by property + unit name. Returns NULL if not found.
     */
    public static Integer resolveUnitId(Connection conn, int propertyId, String unitName) throws SQLException {
        if (propertyId == 0) return 0;
        if (unitName == null || unitName.isBlank()) {
            return null;
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT ID FROM Units WHERE Property=? AND LOWER(Name)=LOWER(?)")) {
            ps.setInt(1, propertyId);
            ps.setString(2, unitName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    public static int ensurePlaceholderUnit(Connection conn, int propertyId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT ID FROM Units WHERE Property=? AND Name=?")) {
            ps.setInt(1, propertyId);
            ps.setString(2, "Unknown Unit");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        try (PreparedStatement ins = conn.prepareStatement(
                "INSERT INTO Units (Name, Property, Bedrooms, Bathrooms, Has_Porch, Has_Back_Yard, Washer_Dryer, IS_Unit_Group, Group_reference) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ins.setString(1, "Unknown Unit");
            ins.setInt(2, propertyId);
            ins.setInt(3, 0);
            ins.setInt(4, 0);
            ins.setString(5, "N");
            ins.setString(6, "N");
            ins.setString(7, "N");
            ins.setString(8, "N");
            ins.setNull(9, java.sql.Types.INTEGER);
            ins.executeUpdate();
            try (ResultSet keys = ins.getGeneratedKeys()) {
                if (keys.next()) return keys.getInt(1);
            }
        }
        return 0;
    }
}