package com.ph.dl.model;

import com.ph.dl.database.DatabaseManager;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ExpenseCategory {
    private int categoryId;
    private String categoryName;

    public ExpenseCategory() {}

    public ExpenseCategory(String categoryName) {
        this.categoryName = categoryName;
    }

    // Getters and Setters
    public int getCategoryId() { return categoryId; }
    public void setCategoryId(int categoryId) { this.categoryId = categoryId; }
    public String getCategoryName() { return categoryName; }
    public void setCategoryName(String categoryName) { this.categoryName = categoryName; }

    // Insert
    public void insert() {
        String sql = "INSERT INTO Expense_Category (Category_Name) VALUES (?)";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, categoryName);
            pstmt.executeUpdate();
            ResultSet rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                categoryId = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Update
    public void update() {
        String sql = "UPDATE Expense_Category SET Category_Name = ? WHERE Category_Id = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, categoryName);
            pstmt.setInt(2, categoryId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Find by ID
    public static ExpenseCategory findById(int categoryId) {
        String sql = "SELECT * FROM Expense_Category WHERE Category_Id = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, categoryId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                ExpenseCategory ec = new ExpenseCategory();
                ec.categoryId = rs.getInt("Category_Id");
                ec.categoryName = rs.getString("Category_Name");
                return ec;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Find all
    public static List<ExpenseCategory> findAll() {
        List<ExpenseCategory> list = new ArrayList<>();
        String sql = "SELECT * FROM Expense_Category";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ExpenseCategory ec = new ExpenseCategory();
                ec.categoryId = rs.getInt("Category_Id");
                ec.categoryName = rs.getString("Category_Name");
                list.add(ec);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}