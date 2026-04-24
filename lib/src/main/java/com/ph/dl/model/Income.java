package com.ph.dl.model;

import com.ph.dl.database.DatabaseManager;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class Income {
    private String transactionId;
    private String status;
    private Date dateCreated;
    private Date dueDate;
    private Date datePaid;
    private String type;
    private String transactionCategory;
    private String rentMonth;
    private BigDecimal originalAmount;
    private BigDecimal payment;
    private BigDecimal balance;
    private BigDecimal vacancy;
    private String methodOfPayment;
    private int payer; // int FK to Tenant
    private int leaseNum; // int FK to Lease.Lease_Number
    private int property;
    private Integer unit;
    private String transactionDetails;
    private String excludeZaky;
    private String sdApplied;
    private int year;

    public Income() {}

    // Constructor with all fields
    public Income(String transactionId, String status, Date dateCreated, Date dueDate, Date datePaid, String type, String transactionCategory, String rentMonth, BigDecimal originalAmount, BigDecimal payment, BigDecimal balance, BigDecimal vacancy, String methodOfPayment, int payer, int leaseNum, int property, Integer unit, String transactionDetails, String excludeZaky, String sdApplied, int year) {
        this.transactionId = transactionId;
        this.status = status;
        this.dateCreated = dateCreated;
        this.dueDate = dueDate;
        this.datePaid = datePaid;
        this.type = type;
        this.transactionCategory = transactionCategory;
        this.rentMonth = rentMonth;
        this.originalAmount = originalAmount;
        this.payment = payment;
        this.balance = balance;
        this.vacancy = vacancy;
        this.methodOfPayment = methodOfPayment;
        this.payer = payer;
        this.leaseNum = leaseNum;
        this.property = property;
        this.unit = unit;
        this.transactionDetails = transactionDetails;
        this.excludeZaky = excludeZaky;
        this.sdApplied = sdApplied;
        this.year = year;
    }

    // Getters and Setters
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Date getDateCreated() { return dateCreated; }
    public void setDateCreated(Date dateCreated) { this.dateCreated = dateCreated; }
    public Date getDueDate() { return dueDate; }
    public void setDueDate(Date dueDate) { this.dueDate = dueDate; }
    public Date getDatePaid() { return datePaid; }
    public void setDatePaid(Date datePaid) { this.datePaid = datePaid; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getTransactionCategory() { return transactionCategory; }
    public void setTransactionCategory(String transactionCategory) { this.transactionCategory = transactionCategory; }
    public String getRentMonth() { return rentMonth; }
    public void setRentMonth(String rentMonth) { this.rentMonth = rentMonth; }
    public BigDecimal getOriginalAmount() { return originalAmount; }
    public void setOriginalAmount(BigDecimal originalAmount) { this.originalAmount = originalAmount; }
    public BigDecimal getBalance() { return balance; }
    public void setBalance(BigDecimal balance) { this.balance = balance; }
    public BigDecimal getPayment() { return payment; }
    public void setPayment(BigDecimal payment) { this.payment = payment; }
    public BigDecimal getVacancy() { return vacancy; }
    public void setVacancy(BigDecimal vacancy) { this.vacancy = vacancy; }
    public String getMethodOfPayment() { return methodOfPayment; }
    public void setMethodOfPayment(String methodOfPayment) { this.methodOfPayment = methodOfPayment; }

    public int getPayer() { return payer; }
    public void setPayer(String payer) { this.payer = (payer == null || payer.isEmpty()) ? 0 : Integer.parseInt(payer); }
    public void setPayer(int payer) { this.payer = payer; }

    public int getLeaseNum() { return leaseNum; }
    public void setLeaseNum(String leaseNum) {
        if (leaseNum == null || leaseNum.trim().isEmpty()) {
            this.leaseNum = 0;
            return;
        }
        this.leaseNum = Integer.parseInt(leaseNum.trim());
    }
    public void setLeaseNum(int leaseNum) { this.leaseNum = leaseNum; }

    public int getProperty() { return property; }
    public void setProperty(int property) { this.property = property; }
    public Integer getUnit() { return unit; }
    public void setUnit(Integer unit) { this.unit = unit; }
    public String getTransactionDetails() { return transactionDetails; }
    public void setTransactionDetails(String transactionDetails) { this.transactionDetails = transactionDetails; }
    public String getExcludeZaky() { return excludeZaky; }
    public void setExcludeZaky(String excludeZaky) { this.excludeZaky = excludeZaky; }
    public String getSdApplied() { return sdApplied; }
    public void setSdApplied(String sdApplied) { this.sdApplied = sdApplied; }
    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    // Insert
    public void insert() {
        String sql = "INSERT INTO Income (Transaction_ID, Status, Date_created, Due_date, Date_paid, Type, Transaction_category, Rent_Month, Original_amount, Payment, Balance, Vacancy, Method_of_payment, Payer, LeaseNum, Property, Unit, Transaction_details, Exclude_Zaky, SD_Applied, Year) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, transactionId);
            pstmt.setString(2, status);
            pstmt.setDate(3, dateCreated);
            pstmt.setDate(4, dueDate);
            pstmt.setDate(5, datePaid);
            pstmt.setString(6, type);
            pstmt.setString(7, transactionCategory);
            pstmt.setString(8, rentMonth);
            pstmt.setBigDecimal(9, originalAmount);
            pstmt.setBigDecimal(10, payment);
            pstmt.setBigDecimal(11, balance);
            pstmt.setBigDecimal(12, vacancy);
            pstmt.setString(13, methodOfPayment);
            if (payer == 0) pstmt.setNull(14, Types.INTEGER); else pstmt.setInt(14, payer);
            if (leaseNum == 0) pstmt.setNull(15, Types.INTEGER); else pstmt.setInt(15, leaseNum);
            pstmt.setInt(16, property);
            if (unit == null) pstmt.setNull(17, Types.INTEGER); else pstmt.setInt(17, unit);
            pstmt.setString(18, transactionDetails);
            pstmt.setString(19, excludeZaky);
            pstmt.setString(20, sdApplied);
            pstmt.setInt(21, year);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Update
    public void update() {
        String sql = "UPDATE Income SET Status = ?, Date_created = ?, Due_date = ?, Date_paid = ?, Type = ?, Transaction_category = ?, Rent_Month = ?, Original_amount = ?, Payment = ?, Balance = ?, Vacancy = ?, Method_of_payment = ?, Payer = ?, LeaseNum = ?, Property = ?, Unit = ?, Transaction_details = ?, Exclude_Zaky = ?, SD_Applied = ?, Year = ? WHERE Transaction_ID = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, status);
            pstmt.setDate(2, dateCreated);
            pstmt.setDate(3, dueDate);
            pstmt.setDate(4, datePaid);
            pstmt.setString(5, type);
            pstmt.setString(6, transactionCategory);
            pstmt.setString(7, rentMonth);
            pstmt.setBigDecimal(8, originalAmount);
            pstmt.setBigDecimal(9, payment);
            pstmt.setBigDecimal(10, balance);
            pstmt.setBigDecimal(11, vacancy);
            pstmt.setString(12, methodOfPayment);
            if (payer == 0) pstmt.setNull(13, Types.INTEGER); else pstmt.setInt(13, payer);
            if (leaseNum == 0) pstmt.setNull(14, Types.INTEGER); else pstmt.setInt(14, leaseNum);
            pstmt.setInt(15, property);
            if (unit == null) pstmt.setNull(16, Types.INTEGER); else pstmt.setInt(16, unit);
            pstmt.setString(17, transactionDetails);
            pstmt.setString(18, excludeZaky);
            pstmt.setString(19, sdApplied);
            pstmt.setInt(20, year);
            pstmt.setString(21, transactionId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Find by ID
    public static Income findById(String transactionId) {
        String sql = "SELECT * FROM Income WHERE Transaction_ID = ?";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, transactionId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Income i = new Income();
                i.transactionId = rs.getString("Transaction_ID");
                i.status = rs.getString("Status");
                i.dateCreated = rs.getDate("Date_created");
                i.dueDate = rs.getDate("Due_date");
                i.datePaid = rs.getDate("Date_paid");
                i.type = rs.getString("Type");
                i.transactionCategory = rs.getString("Transaction_category");
                i.rentMonth = rs.getString("Rent_Month");
                i.originalAmount = rs.getBigDecimal("Original_amount");
                i.balance = rs.getBigDecimal("Balance");
                i.payment = rs.getBigDecimal("Payment");
                i.vacancy = rs.getBigDecimal("Vacancy");
                i.methodOfPayment = rs.getString("Method_of_payment");
                i.payer = rs.getInt("Payer");
                if (rs.wasNull()) i.payer = 0;
                i.leaseNum = rs.getInt("LeaseNum");
                if (rs.wasNull()) i.leaseNum = 0;
                i.property = rs.getInt("Property");
                i.unit = rs.getInt("Unit");
                i.transactionDetails = rs.getString("Transaction_details");
                i.excludeZaky = rs.getString("Exclude_Zaky");
                i.sdApplied = rs.getString("SD_Applied");
                i.year = rs.getInt("Year");
                return i;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Find all
    public static List<Income> findAll() {
        List<Income> list = new ArrayList<>();
        String sql = "SELECT * FROM Income";
        try (Connection conn = DatabaseManager.getInstance().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Income i = new Income();
                i.transactionId = rs.getString("Transaction_ID");
                i.status = rs.getString("Status");
                i.dateCreated = rs.getDate("Date_created");
                i.dueDate = rs.getDate("Due_date");
                i.datePaid = rs.getDate("Date_paid");
                i.type = rs.getString("Type");
                i.transactionCategory = rs.getString("Transaction_category");
                i.rentMonth = rs.getString("Rent_Month");
                i.originalAmount = rs.getBigDecimal("Original_amount");
                i.balance = rs.getBigDecimal("Balance");
                i.payment = rs.getBigDecimal("Payment");
                i.vacancy = rs.getBigDecimal("Vacancy");
                i.methodOfPayment = rs.getString("Method_of_payment");
                i.payer = rs.getInt("Payer");
                if (rs.wasNull()) i.payer = 0;
                i.leaseNum = rs.getInt("LeaseNum");
                if (rs.wasNull()) i.leaseNum = 0;
                i.property = rs.getInt("Property");
                i.unit = rs.getInt("Unit");
                i.transactionDetails = rs.getString("Transaction_details");
                i.excludeZaky = rs.getString("Exclude_Zaky");
                i.sdApplied = rs.getString("SD_Applied");
                i.year = rs.getInt("Year");
                list.add(i);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * Shared rent-month calculation used by vacancy-loss generation.
     * Matches the previous IncomeLoader behavior: dueDate + 5 days, then month name.
     */
    public static String calcRentMonth(java.sql.Date dueDate) {
        if (dueDate == null) return "";
        LocalDate ld = new java.util.Date(dueDate.getTime()).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().plusDays(5);
        return ld.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH);
    }

    public static boolean vacancyRowExists(Connection conn, int year, int propertyId, int unitId, java.sql.Date dueDate) throws SQLException {
        String sql = "SELECT 1 FROM Income WHERE Year=? AND Property=? AND Unit=? AND Status='Vacancy' AND Due_date=? FETCH FIRST 1 ROWS ONLY";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, year);
            ps.setInt(2, propertyId);
            ps.setInt(3, unitId);
            ps.setDate(4, dueDate);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Inserts a vacancy-loss synthetic Income row.
     *
     * @return 1 if inserted
     */
    public static int insertVacancyRow(Connection conn,
                                      String transactionId,
                                      int year,
                                      int propertyId,
                                      int unitId,
                                      java.sql.Date firstOfMonth,
                                      BigDecimal baseRent,
                                      String rentMonth) throws SQLException {
        try (PreparedStatement ins = conn.prepareStatement(
                "INSERT INTO Income (Transaction_ID, Status, Date_created, Due_date, Date_paid, Type, Transaction_category, Rent_Month, Original_amount, Payment, Balance, Vacancy, Method_of_payment, Payer, LeaseNum, Property, Unit, Transaction_details, Exclude_Zaky, SD_Applied, Year) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            ins.setString(1, transactionId);
            ins.setString(2, "Vacancy");
            ins.setDate(3, firstOfMonth);
            ins.setDate(4, firstOfMonth);
            ins.setNull(5, Types.DATE);
            ins.setString(6, "Income");
            ins.setString(7, "Rent");
            ins.setString(8, rentMonth);
            ins.setBigDecimal(9, baseRent);
            ins.setNull(10, Types.DECIMAL);
            ins.setNull(11, Types.DECIMAL);
            ins.setBigDecimal(12, baseRent);
            ins.setNull(13, Types.VARCHAR);
            ins.setNull(14, Types.INTEGER);
            ins.setNull(15, Types.INTEGER);
            ins.setInt(16, propertyId);
            ins.setInt(17, unitId);
            ins.setNull(18, Types.VARCHAR);
            ins.setNull(19, Types.VARCHAR);
            ins.setNull(20, Types.VARCHAR);
            ins.setInt(21, year);
            ins.executeUpdate();
            return 1;
        }
    }
}