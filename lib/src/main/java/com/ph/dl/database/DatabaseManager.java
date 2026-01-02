package com.ph.dl.database;

import com.ph.dl.util.CsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class DatabaseManager {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseManager.class);
    private static DatabaseManager instance;
    private static final String DB_DIR = "C:/Users/Bachu-PC/database";
    //private static final String DB_URL = "jdbc:h2:file:" + DB_DIR + "/accounting";
    private static final String DB_URL = "jdbc:h2:tcp://localhost:9092/accounting";
    private static final String DB_USER = "sa";
    private static final String DB_PASS = "";

    private DatabaseManager() {
        try {
            Class.forName("org.h2.Driver");
            // Always recreate the database at startup: delete existing files
            File dbDir = new File(DB_DIR);
            dbDir.mkdirs();
            File mainDb = new File(DB_DIR + "/accounting.mv.db");
            File traceDb = new File(DB_DIR + "/accounting.trace.db");
            //if (mainDb.exists()) mainDb.delete();
            //if (traceDb.exists()) traceDb.delete();
            // Initialize fresh schema
            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                 Statement stmt = conn.createStatement()) {
                createTables(stmt);
                alterTables(stmt);
                loadReferenceData(conn);
                createViews(stmt);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DatabaseManager getInstance() {
        if (instance == null) {
            instance = new DatabaseManager();
        }
        return instance;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }

    private void createTables(Statement stmt) throws SQLException {
        String createProperty = "CREATE TABLE IF NOT EXISTS PROPERTY (" +
            "ID INT PRIMARY KEY," +
            "Name VARCHAR(100)," +
            "Full_Address VARCHAR(255)," +
            "Number_of_units INT" +
            ")";
        stmt.execute(createProperty);

        String createUnits = "CREATE TABLE IF NOT EXISTS UNITS (" +
            "ID INT PRIMARY KEY," +
            "Name VARCHAR(10)," +
            "Property INT," +
            "Bedrooms INT," +
            "Bathrooms INT," +
            "Has_Porch VARCHAR(1)," +
            "Has_Back_Yard VARCHAR(1)," +
            "Washer_Dryer VARCHAR(1)," +
            "FOREIGN KEY (Property) REFERENCES Property(ID)" +
            ")";
        stmt.execute(createUnits);

        String createTenant = "CREATE TABLE IF NOT EXISTS TENANT (" +
            "Tenant_ID INT PRIMARY KEY," +
            "Name VARCHAR(100)," +
            "Email VARCHAR(100)," +
            "Phone VARCHAR(15)" +
            ")";
        stmt.execute(createTenant);

        // Lease: Lease_Number is INT PRIMARY KEY; Unique_Id removed completely
        String createLease = "CREATE TABLE IF NOT EXISTS LEASE (" +
            "Lease_Number INT PRIMARY KEY," +
            "Property INT," +
            "Unit INT," +
            "Start_date DATE," +
            "End_date DATE," +
            "YEAR INT," +
            "FOREIGN KEY (Property) REFERENCES Property(ID)," +
            "FOREIGN KEY (Unit) REFERENCES Units(ID)" +
            ")";

        // Lease_Tenant: Lease_Id now references Lease_Number INT
        String createLeaseTenant = "CREATE TABLE IF NOT EXISTS LEASE_TENANT (" +
            "Lease_Id INT," +
            "Tenant_Id INT," +
            "PRIMARY KEY (Lease_Id, Tenant_Id)," +
            "FOREIGN KEY (Lease_Id) REFERENCES Lease(Lease_Number)," +
            "FOREIGN KEY (Tenant_Id) REFERENCES Tenant(Tenant_ID)" +
            ")";
        stmt.execute(createLease);
        stmt.execute(createLeaseTenant);

        String createExpenseCategory = "CREATE TABLE IF NOT EXISTS EXPENSE_CATEGORY (" +
            "Category_Id INT PRIMARY KEY," +
            "Category_Name VARCHAR(25) UNIQUE" +
            ")";
        stmt.execute(createExpenseCategory);

        String createProvider = "CREATE TABLE IF NOT EXISTS PROVIDER (" +
            "Provider_Id INT PRIMARY KEY," +
            "Provider_Name VARCHAR(50)," +
            "Provider_Address VARCHAR(255)," +
            "Email VARCHAR(100)," +
            "Phone VARCHAR(15)," +
            "Entity VARCHAR(255)" +
            ")";
        stmt.execute(createProvider);

        String createIncome = "CREATE TABLE IF NOT EXISTS INCOME (" +
            "Transaction_ID VARCHAR(32) PRIMARY KEY," +
            "Status VARCHAR(10)," +
            "Date_created DATE," +
            "Due_date DATE," +
            "Date_paid DATE," +
            "YEAR INT," +
            "Type VARCHAR(255)," +
            "Transaction_category VARCHAR(255)," +
            "Rent_Month VARCHAR(10)," +
            "Original_amount DECIMAL(10,2)," +
            "Payment DECIMAL(10,2)," +
            "Balance DECIMAL(10,2)," +
            "Vacancy DECIMAL(10,2)," +
            "Method_of_payment VARCHAR(255)," +
            "Payer INT," +
            "LeaseNum INT," +
            "Property INT," +
            "Unit INT," +
            "Transaction_details VARCHAR(255)," +
            "Exclude_Zaky VARCHAR(1)," +
            "SD_Applied VARCHAR(1)," +
            "FOREIGN KEY (Payer) REFERENCES Tenant(Tenant_ID)," +
            "FOREIGN KEY (LeaseNum) REFERENCES Lease(Lease_Number)," +
            "FOREIGN KEY (Property) REFERENCES Property(ID)," +
            "FOREIGN KEY (Unit) REFERENCES Units(ID)" +
            ")";
        stmt.execute(createIncome);

        String createExpense = "CREATE TABLE IF NOT EXISTS EXPENSE (" +
            "Transaction VARCHAR(50) PRIMARY KEY," +
            "Date DATE," +
            "YEAR INT," +
            "Amount DECIMAL(10,2)," +
            "Details VARCHAR(255)," +
            "Payment_Mode VARCHAR(255)," +
            "Check_num VARCHAR(10)," +
            "Payment_Account VARCHAR(255)," +
            "Property INT," +
            "Unit INT," +
            "Category INT," +
            "Provider INT," +
            "Type VARCHAR(255)," +
            "Exclude VARCHAR(1)," +
            "Exclude_Zaky VARCHAR(1)," +
            "FOREIGN KEY (Property) REFERENCES Property(ID)," +
            "FOREIGN KEY (Unit) REFERENCES Units(ID)," +
            "FOREIGN KEY (Category) REFERENCES Expense_Category(Category_Id)," +
            "FOREIGN KEY (Provider) REFERENCES Provider(Provider_Id)" +
            ")";
        stmt.execute(createExpense);

        String createMaintenanceRequest = "CREATE TABLE IF NOT EXISTS Maintenance_Request (" +
            "Ticket_Id VARCHAR(255) PRIMARY KEY," +
            "Property INT," +
            "Unit INT," +
            "Tenant INT," +
            "Amount DECIMAL(10,2)," +
            "FOREIGN KEY (Property) REFERENCES Property(ID)," +
            "FOREIGN KEY (Unit) REFERENCES Units(ID)," +
            "FOREIGN KEY (Tenant) REFERENCES Tenant(Tenant_ID)" +
            ")";
        stmt.execute(createMaintenanceRequest);

        String createBankPayment = "CREATE TABLE IF NOT EXISTS Bank_Payment (" +
            "Transaction_Id VARCHAR(25) PRIMARY KEY," +
            "Date DATE," +
            "Property INT," +
            "Bank VARCHAR(255)," +
            "Total_Amount DECIMAL(10,2)," +
            "Interest DECIMAL(10,2)," +
            "Principal DECIMAL(10,2)," +
            "FOREIGN KEY (Property) REFERENCES Property(ID)" +
            ")";
        stmt.execute(createBankPayment);

        // Ensure File_Processing exists before any loader runs
        String fileProcessing = "CREATE TABLE IF NOT EXISTS File_Processing (" +
            "ID INT AUTO_INCREMENT PRIMARY KEY," +
            "File_Path VARCHAR(512) UNIQUE," +
            "Processed_At TIMESTAMP," +
            "Year INT," +
            "File_Type VARCHAR(64)" +
            ")";
        stmt.execute(fileProcessing);
    }

    private void alterTables(Statement stmt) throws SQLException {
        // Alter Lease table
        stmt.execute("ALTER TABLE Lease ADD COLUMN IF NOT EXISTS YEAR INT");
        // Alter Income table
        stmt.execute("ALTER TABLE Income ADD COLUMN IF NOT EXISTS YEAR INT");
        // Alter Expense table
        stmt.execute("ALTER TABLE Expense ADD COLUMN IF NOT EXISTS YEAR INT");
        // Alter Maintenance_Request table
        stmt.execute("ALTER TABLE Maintenance_Request ADD COLUMN IF NOT EXISTS YEAR INT");
        // Alter Bank_Payment table
        stmt.execute("ALTER TABLE Bank_Payment ADD COLUMN IF NOT EXISTS YEAR INT");
    }

    private void loadReferenceData(Connection conn) throws Exception {
        // Load properties
        String propPath = getClass().getClassLoader().getResource("referenceData/properties.csv").getPath();
        List<Map<String, Object>> propRows = CsvParser.parse(propPath);
        if(!checkCount(conn, "Property", propRows.size())) {
        	LOG.info("Reloading Property reference data.");
	        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Property (ID, Name, Full_Address, Number_of_units) VALUES (?, ?, ?, ?)")) {
	            for (Map<String, Object> row : propRows) {
	                ps.setInt(1, Integer.parseInt(row.get("ID").toString()));
	                ps.setString(2, row.get("Name").toString());
	                ps.setString(3, row.get("Full_Address").toString());
	                ps.setInt(4, Integer.parseInt(row.get("Number_of_units").toString()));
	                ps.executeUpdate();
	            }
	        }
        }
        

        // Load units
        String unitPath = getClass().getClassLoader().getResource("referenceData/units.csv").getPath();
        List<Map<String, Object>> unitRows = CsvParser.parse(unitPath);
        if(!checkCount(conn, "Units", unitRows.size())) {
			LOG.info("Reloading Units reference data.");
	        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Units (ID, Name, Property, Bedrooms, Bathrooms, Has_Porch, Has_Back_Yard, Washer_Dryer) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
	            for (Map<String, Object> row : unitRows) {
	                ps.setInt(1, Integer.parseInt(row.get("ID").toString()));
	                ps.setString(2, row.get("Name").toString());
	                ps.setInt(3, Integer.parseInt(row.get("Property").toString()));
	                ps.setInt(4, Integer.parseInt(row.get("Bedrooms").toString()));
	                ps.setInt(5, Integer.parseInt(row.get("Bathrooms").toString()));
	                ps.setString(6, row.get("Has_Porch").toString());
	                ps.setString(7, row.get("Has_Back_Yard").toString());
	                ps.setString(8, row.get("Washer_Dryer").toString());
	                ps.executeUpdate();
	            }
	        }
        }

        // Load expense categories
        String catPath = getClass().getClassLoader().getResource("referenceData/expense_categories.csv").getPath();
        List<Map<String, Object>> catRows = CsvParser.parse(catPath);
        if(!checkCount(conn, "Expense_Category", catRows.size())) {
        	LOG.info("Reloading Expense_Category reference data.");
	        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Expense_Category (Category_Id, Category_Name) VALUES (?, ?)")) {
	            for (Map<String, Object> row : catRows) {
	                ps.setInt(1, Integer.parseInt(row.get("Category_Id").toString()));
	                ps.setString(2, row.get("Category_Name").toString());
	                ps.executeUpdate();
	            }
	        }
        }

        // Load providers
        String provPath = getClass().getClassLoader().getResource("referenceData/providers.csv").getPath();
        List<Map<String, Object>> provRows = CsvParser.parse(provPath);
        if(!checkCount(conn, "Provider", provRows.size())) {
			LOG.info("Reloading Provider reference data.");
		
	        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Provider (Provider_Id, Provider_Name) VALUES (?, ?)")) {
	            for (Map<String, Object> row : provRows) {
	                ps.setInt(1, Integer.parseInt(row.get("Provider_Id").toString()));
	                ps.setString(2, row.get("Provider_Name").toString());
	                ps.executeUpdate();
	            }
	        }
        }
        
    }

    private Boolean checkCount(Connection conn, String table, int expected) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + table)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int actual = rs.getInt(1);
                    if (actual != expected) {
                        LOG.error("Count mismatch for {}: expected {}, actual {}", table, expected, actual);
                        PreparedStatement ps1 = conn.prepareStatement("DELETE FROM " + table);
                        ps.execute();
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private void createViews(Statement stmt) throws SQLException {
        String incomeView = "CREATE VIEW IF NOT EXISTS Income_View AS " +
            "SELECT i.*, i.YEAR AS Income_Year, t.Name AS Payer_Name, p.Name AS Property_Name, u.Name AS Unit_Name, l.Lease_Number " +
            "FROM Income i " +
            "LEFT JOIN Tenant t ON i.Payer = t.Tenant_ID " +
            "LEFT JOIN Property p ON i.Property = p.ID " +
            "LEFT JOIN Units u ON i.Unit = u.ID " +
            "LEFT JOIN Lease l ON i.LeaseNum = l.Lease_Number";
        stmt.execute(incomeView);

        String expenseView = "CREATE VIEW IF NOT EXISTS Expense_View AS " +
            "SELECT  e.YEAR AS Expense_Year, p.Name AS Property_Name, u.Name AS Unit_Name, ec.Category_Name, pr.Provider_Name " +
            "FROM Expense e " +
            "LEFT JOIN Property p ON e.Property = p.ID " +
            "LEFT JOIN Units u ON e.Unit = u.ID " +
            "LEFT JOIN Expense_Category ec ON e.Category = ec.Category_Id " +
            "LEFT JOIN Provider pr ON e.Provider = pr.Provider_Id";
        stmt.execute(expenseView);
    }
}
