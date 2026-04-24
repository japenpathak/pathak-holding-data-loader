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
    // Default to embedded file DB so unit tests don't require a running H2 TCP server.
    // Can be overridden with -Dph.dl.dbUrl=... (e.g. jdbc:h2:tcp://localhost:9092/accounting)
    //private static final String DB_DEFAULT_URL = "jdbc:h2:file:" + DB_DIR + "/accounting";
    private static final String DB_DEFAULT_URL = "jdbc:h2:tcp://localhost:9092/accounting";
    private static final String DB_URL = System.getProperty("ph.dl.dbUrl", DB_DEFAULT_URL);
    private static final String DB_USER = "sa";
    private static final String DB_PASS = "";

    private static volatile boolean schemaInitialized = false;

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
        Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
        ensureSchemaInitialized(conn);
        return conn;
    }

    private void ensureSchemaInitialized(Connection conn) {
        if (schemaInitialized) return;
        synchronized (DatabaseManager.class) {
            if (schemaInitialized) return;
            try (Statement stmt = conn.createStatement()) {
                createTables(stmt);
                alterTables(stmt);
                loadReferenceData(conn);
                createViews(stmt);
                schemaInitialized = true;
            } catch (Exception e) {
                // Don't fail hard here; callers will see SQL errors if schema isn't present.
                e.printStackTrace();
            }
        }
    }

    private void createTables(Statement stmt) throws SQLException {
        String createProperty = "CREATE TABLE IF NOT EXISTS PROPERTY (" +
            "ID INT PRIMARY KEY," +
            "Name VARCHAR(100)," +
            "Full_Address VARCHAR(255)," +
            "Number_of_units INT," +
            "Acquired_Date DATE" +
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
            "IS_Unit_Group VARCHAR(1)," +
            "Group_reference INT," +
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

        // Ensure File_Processing exists before any loader runs (must exist before FK references)
        String fileProcessing = "CREATE TABLE IF NOT EXISTS File_Processing (" +
            "ID INT AUTO_INCREMENT PRIMARY KEY," +
            "File_Path VARCHAR(512) UNIQUE," +
            "Processed_At TIMESTAMP," +
            "Year INT," +
            "File_Type VARCHAR(64)," +
            "Total_Rows INT," +
            "Success_Rows INT," +
            "Failed_Rows INT" +
            ")";
        stmt.execute(fileProcessing);

        String createBankTransactionStaging = "CREATE TABLE IF NOT EXISTS Bank_transaction_staging (" +
                "ID INT AUTO_INCREMENT PRIMARY KEY," +
                "FILE_ID INT," +
                "Details VARCHAR(10)," +
                "Posting_Date DATE," +
                "Description VARCHAR(400)," +
                "Amount DECIMAL(10,2)," +
                "Type VARCHAR(20)," +
                "Check_Num VARCHAR(40)," +
                "Year INT," +
                "IS_PROCESSED VARCHAR(1)," +
                "FOREIGN KEY (FILE_ID) REFERENCES File_Processing(ID)" +
                ")";
        stmt.execute(createBankTransactionStaging);
    }

    private void alterTables(Statement stmt) throws SQLException {
        // Ensure new Property column exists
        stmt.execute("ALTER TABLE Property ADD COLUMN IF NOT EXISTS Acquired_Date DATE");

        // Ensure Units can be evolved without dropping the table (older DBs may not have these columns)
        stmt.execute("ALTER TABLE Units ADD COLUMN IF NOT EXISTS IS_Unit_Group VARCHAR(1)");
        stmt.execute("ALTER TABLE Units ADD COLUMN IF NOT EXISTS Group_reference INT");

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

        // File processing metrics
        stmt.execute("ALTER TABLE File_Processing ADD COLUMN IF NOT EXISTS Total_Rows INT");
        stmt.execute("ALTER TABLE File_Processing ADD COLUMN IF NOT EXISTS Success_Rows INT");
        stmt.execute("ALTER TABLE File_Processing ADD COLUMN IF NOT EXISTS Failed_Rows INT");

        // Staging table evolvability
        stmt.execute("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS IS_PROCESSED VARCHAR(1)");
        stmt.execute("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS FILE_ID INT");
        stmt.execute("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS Year INT");
        stmt.execute("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS Details VARCHAR(10)");
        stmt.execute("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS Posting_Date DATE");
        stmt.execute("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS Description VARCHAR(400)");
        stmt.execute("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS Amount DECIMAL(10,2)");
        stmt.execute("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS Type VARCHAR(20)");
        stmt.execute("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS Check_Num VARCHAR(40)");

        // FK might already exist; ignore error if it does.
        try {
            stmt.execute("ALTER TABLE Bank_transaction_staging ADD CONSTRAINT IF NOT EXISTS FK_BTS_FILE_ID FOREIGN KEY (FILE_ID) REFERENCES File_Processing(ID)");
        } catch (SQLException ignore) {
            // H2 versions vary in supported IF NOT EXISTS for constraints; safe to ignore if it already exists.
        }
    }

    private void loadReferenceData(Connection conn) throws Exception {
        // Load properties
        String propPath = getClass().getClassLoader().getResource("referenceData/properties.csv").getPath();
        List<Map<String, Object>> propRows = CsvParser.parse(propPath);

        // Upsert properties (never mass-delete; other tables may FK to Property)
        LOG.info("Upserting Property reference data ({} rows).", propRows.size());
        try (PreparedStatement ps = conn.prepareStatement(
                "MERGE INTO Property (ID, Name, Full_Address, Number_of_units, Acquired_Date) KEY(ID) VALUES (?, ?, ?, ?, ?)")) {
            for (Map<String, Object> row : propRows) {
                ps.setInt(1, Integer.parseInt(row.get("id").toString()));
                ps.setString(2, row.get("name").toString());
                ps.setString(3, row.get("full_address").toString());
                ps.setInt(4, Integer.parseInt(row.get("number_of_units").toString()));

                // CSV may not have this yet; keep NULL unless provided
                Object acquiredDate = row.get("acquired_date");
                if (acquiredDate == null || String.valueOf(acquiredDate).isBlank()) {
                    ps.setNull(5, java.sql.Types.DATE);
                } else {
                    // Expecting yyyy-MM-dd
                    ps.setDate(5, java.sql.Date.valueOf(String.valueOf(acquiredDate).trim()));
                }

                ps.executeUpdate();
            }
        }

        // Load units
        String unitPath = getClass().getClassLoader().getResource("referenceData/units.csv").getPath();
        List<Map<String, Object>> unitRows = CsvParser.parse(unitPath);

        // Upsert units (never mass-delete; other tables FK to Units)
        LOG.info("Upserting Units reference data ({} rows).", unitRows.size());
        try (PreparedStatement ps = conn.prepareStatement(
                "MERGE INTO Units (ID, Name, Property, Bedrooms, Bathrooms, Has_Porch, Has_Back_Yard, Washer_Dryer, IS_Unit_Group, Group_reference) " +
                        "KEY(ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            for (Map<String, Object> row : unitRows) {
                ps.setInt(1, Integer.parseInt(row.get("id").toString()));
                ps.setString(2, row.get("name").toString());
                ps.setInt(3, Integer.parseInt(row.get("property").toString()));
                ps.setInt(4, Integer.parseInt(row.get("bedrooms").toString()));
                ps.setInt(5, Integer.parseInt(row.get("bathrooms").toString()));
                ps.setString(6, row.get("has_porch").toString());
                ps.setString(7, row.get("has_back_yard").toString());
                ps.setString(8, row.get("washer_dryer").toString());

                Object isGrp = row.get("is_unit_group");
                ps.setString(9, isGrp == null || String.valueOf(isGrp).isBlank() ? null : String.valueOf(isGrp).trim());

                Object grpRef = row.get("group_reference");
                if (grpRef == null || String.valueOf(grpRef).isBlank()) {
                    ps.setNull(10, java.sql.Types.INTEGER);
                } else {
                    ps.setInt(10, Integer.parseInt(String.valueOf(grpRef).trim()));
                }

                ps.executeUpdate();
            }
        }

        // Load expense categories
        String catPath = getClass().getClassLoader().getResource("referenceData/expense_categories.csv").getPath();
        List<Map<String, Object>> catRows = CsvParser.parse(catPath);

        LOG.info("Upserting Expense_Category reference data ({} rows).", catRows.size());
        try (PreparedStatement ps = conn.prepareStatement(
                "MERGE INTO Expense_Category (Category_Id, Category_Name) KEY(Category_Id) VALUES (?, ?)")) {
            for (Map<String, Object> row : catRows) {
                ps.setInt(1, Integer.parseInt(row.get("category_id").toString()));
                ps.setString(2, row.get("category_name").toString());
                ps.executeUpdate();
            }
        }

        // Load providers
        String provPath = getClass().getClassLoader().getResource("referenceData/providers.csv").getPath();
        List<Map<String, Object>> provRows = CsvParser.parse(provPath);

        LOG.info("Upserting Provider reference data ({} rows).", provRows.size());
        try (PreparedStatement ps = conn.prepareStatement(
                "MERGE INTO Provider (Provider_Id, Provider_Name) KEY(Provider_Id) VALUES (?, ?)")) {
            for (Map<String, Object> row : provRows) {
                ps.setInt(1, Integer.parseInt(row.get("provider_id").toString()));
                ps.setString(2, row.get("provider_name").toString());
                ps.executeUpdate();
            }
        }
    }

    private Boolean checkCount(Connection conn, String table, int expected) throws SQLException {
        // With upserts we no longer rely on count-mismatch delete/reload.
        // Keep this method for compatibility, but make it a no-op that returns true.
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

        // View focused on vacancy rows
        String vacancyView = "CREATE VIEW IF NOT EXISTS Vacancy_View AS " +
            "SELECT i.Transaction_ID, i.Status, i.Date_created, i.Year, i.Rent_Month, i.Vacancy, " +
            "p.Name AS Property, u.Name AS Unit " +
            "FROM Income i " +
            "LEFT JOIN Property p ON i.Property = p.ID " +
            "LEFT JOIN Units u ON i.Unit = u.ID " +
            "WHERE i.Status = 'Vacancy'";
        stmt.execute(vacancyView);

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
