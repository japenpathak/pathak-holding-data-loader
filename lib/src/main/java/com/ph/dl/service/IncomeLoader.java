package com.ph.dl.service;

import com.ph.dl.config.YamlConfig;
import com.ph.dl.database.DatabaseManager;
import com.ph.dl.db.helper.Units;
import com.ph.dl.model.Lease;
import com.ph.dl.model.Tenant;
import com.ph.dl.model.Income;
import com.ph.dl.util.CsvParser;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.TextStyle;
import java.util.*;

@Component
public class IncomeLoader {

    private static final Logger LOG = LoggerFactory.getLogger(IncomeLoader.class);

    private Map<String, Tenant> tenantsByEmail = new HashMap<>();
    private Map<String, Lease> leaseMap = new HashMap<>();
    
    @Value("${income.year:0}")
    int year;


    public void loadOnStartup() throws Exception {
        YamlConfig cfg = new YamlConfig();
        String path = cfg.getIncomeCsvPath();
        boolean forceOverwrite = cfg.isForceOverwrite();
        if (year == 0) {
            throw new IllegalStateException("income.year not configured (check application.yml)");
        }
        Map<String, String> map = cfg.getIncomeColumnMapping();

        // Initialize caches from database
        tenantsByEmail = Tenant.getAllTenantsByEmail();
        LOG.info("Loaded tenant Count :"+tenantsByEmail.size());

        leaseMap = Lease.getAllLeasesByNumber();
        LOG.info("Loaded Lease Count :"+leaseMap.size());

        List<Map<String, Object>> rows = CsvParser.parse(path);
        // Pre-check processed files
        try (Connection conn = DatabaseManager.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            try {
				/*
				 * if (!forceOverwrite && isFileProcessed(conn, path, year)) { throw new
				 * RuntimeException("File already processed for year " + year + ": " + path); }
				 */
            	
                // Process rows and insert in bulk
                processAndInsert(conn, rows, map, year, forceOverwrite);

                // Mark file processed
                try (PreparedStatement ps = conn.prepareStatement("MERGE INTO File_Processing (File_Path, Processed_At, Year, File_Type) KEY(File_Path) VALUES (?, CURRENT_TIMESTAMP(), ?, ?)")) {
                    ps.setString(1, path);
                    ps.setInt(2, year);
                    ps.setString(3, "tc_Income");
                    ps.executeUpdate();
                }
                conn.commit();
            } catch (Exception ex) {
                conn.rollback();
                LOG.error("Error processing file: " + path + " => " + ex.getMessage(), ex);
                throw ex;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private boolean isFileProcessed(Connection conn, String path, int year) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM File_Processing WHERE File_Path = ? AND Year = ?")) {
            ps.setString(1, path);
            ps.setInt(2, year);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private void processAndInsert(Connection conn, List<Map<String, Object>> rows, Map<String, String> map, int year, boolean forceOverwrite) throws SQLException {
        // Caches to avoid repeated DB queries
    	int counter = 1;
        
        for (Map<String, Object> row : rows) {
            LOG.debug("Processing row: "+ counter++ +" with Data:" + row.get("transaction id"));
        	String status = get(row, map.get("Status"));
            if (status == null) status = "";
            if (equalsIgnoreCase(status, "Void") || equalsIgnoreCase(status, "Waived")) {
                LOG.info("Skipped VOID/WAIVED income Transaction_ID=" + get(row, map.get("Transaction_ID")));
                continue;
            }
            String txnCat = get(row, map.get("Transaction_category"));
            if (txnCat == null) txnCat = "";
            if (equalsIgnoreCase(txnCat, "Credits")) {
                LOG.info("Skipped Credits income Transaction_ID=" + get(row, map.get("Transaction_ID")));
                continue;
            }

            String type = normalizeType(get(row, map.get("Type")));
            // Category transformations
            String normalizedCat = normalizeCategoryAndAdjustType(txnCat, type);
            type = normalizedCat.split("\u0000")[1]; // hack: we return cat\0type
            String cat = normalizedCat.split("\u0000")[0];

            // Method of payment cleanup
            String method = cleanupMethod(get(row, map.get("Method_of_payment")));

            String details = opt(row, map.get("Transaction_details"));
            String excludeZaky = "";
            String sdApplied = "";
            if (containsAny(details, true, "Security deposit applied", "deposit applied")) {
                sdApplied = "Y";
                excludeZaky = "Y";
                method = "Deposit Applied";
            }
            if (containsAny(details, true, "Teller deposit at PNC") || containsAny(details, true, "Teller", "PNC", "certified check")) {
                method = "Teller Deposit";
            }
            if (equalsIgnoreCase(method, "Cash")) {
                excludeZaky = "Y";
            }

            // Tenant and Lease
            String payer = opt(row, map.get("Payer"));
            String payerEmail = opt(row, "Payer/payee email"); // may not be mapped; fallback header used
            String leaseNum = opt(row, map.get("LeaseNum"));
            if ("-".equals(leaseNum)) {
                leaseNum = null;
            }
            String tenantId = ensureTenantAndLease(conn, payer, payerEmail,
                    leaseNum,
                    opt(row, map.get("Property")),
                    opt(row, map.get("Unit")), year, tenantsByEmail, leaseMap);

            // Rent month calc
            String rentMonth = opt(row, map.get("Rent_Month"));
            String dueDateStr = opt(row, map.get("Due_date"));
            if (equalsIgnoreIgnoreNull(cat, "Rent") 
            		|| equalsIgnoreIgnoreNull(cat, "Prorated rent") 
            		||  equalsIgnoreIgnoreNull(cat, "Pet Rent") ) {
                rentMonth = calcRentMonth(dueDateStr);
            }

            // Build Income
            Income income = new Income();
            income.setTransactionId(get(row, map.get("Transaction_ID")));
            income.setStatus(status);
            income.setDateCreated(parseDate(opt(row, map.get("Date_created"))));
            income.setDueDate(parseDate(dueDateStr));
            income.setDatePaid(parseDate(opt(row, map.get("Date_paid"))));
            income.setType(type);
            income.setTransactionCategory(cat);
            income.setRentMonth(rentMonth);
            income.setOriginalAmount(parseMoney(get(row, map.get("Original_amount"))));
            income.setPayment(parseMoney(opt(row, map.get("Payment"))));
            income.setBalance(parseMoney(opt(row, map.get("Balance"))));
            income.setVacancy(parseMoney(opt(row, map.get("Vacancy"))));
            income.setMethodOfPayment(method);
            income.setPayer(tenantId);
            income.setLeaseNum(leaseNum);
            income.setProperty(resolvePropertyId(conn, opt(row, map.get("Property"))));
            income.setUnit(resolveUnitId(conn, income.getProperty(), opt(row, map.get("Unit"))));
            income.setTransactionDetails(details);
            income.setExcludeZaky(excludeZaky);
            income.setSdApplied(sdApplied);
            income.setYear(year);

            upsertIncome(conn, income, forceOverwrite, year);
        }
    }

    private void upsertIncome(Connection conn, Income income, boolean forceOverwrite, int yearScope) throws SQLException {
        // Check existing by transaction_id and year
        try (PreparedStatement check = conn.prepareStatement("SELECT Year FROM Income WHERE Transaction_ID = ?")) {
            check.setString(1, income.getTransactionId());
            try (ResultSet rs = check.executeQuery()) {
                if (rs.next()) {
                    int existingYear = rs.getInt(1);
                    if (existingYear != yearScope) {
                        throw new SQLException("Attempt to modify income outside year scope: " + income.getTransactionId());
                    }
                    if (!forceOverwrite) {
                        LOG.info("Duplicate income skipped: Transaction_ID={}", income.getTransactionId());
                        return;
                    }
                    // Update
                    income.update();
                    return;
                }
            }
        }
        income.insert();
    }

    private String ensureTenantAndLease(Connection conn, String tenantName, String email, String leaseNum, String propertyName, String unitName, int year, Map<String, Tenant> tenantsByEmail, Map<String, Lease> leaseMap) throws SQLException {
        if (tenantName == null || tenantName.isEmpty()) return "0";
        Tenant t = null;
        if (email != null && !email.isEmpty()) {
            t = tenantsByEmail.get(email);
        }
        if (t == null) {
            LOG.info("Creating new tenant: name=" + tenantName + ", email=" + email);
            t = new Tenant(null, tenantName, email, "");
            t.insert();
            if (email != null && !email.isEmpty()) {
                tenantsByEmail.put(email, t);
            }
        }
        int tenantId = t.getTenantId();

        int propertyId = Units.resolvePropertyId(conn, propertyName);
        Integer unitId = Units.resolveUnitId(conn, propertyId, unitName);

        if (leaseNum != null && !leaseNum.isEmpty()) {
            int leaseNumberInt;
            try {
                leaseNumberInt = Integer.parseInt(leaseNum.trim());
            } catch (NumberFormatException nfe) {
                throw new SQLException("Lease number is not a valid integer: '" + leaseNum + "'", nfe);
            }

            Lease l = leaseMap.get(String.valueOf(leaseNumberInt));
            if (l == null) {
                LOG.info("Creating new lease: " + leaseNumberInt + " for tenant " + tenantName);
                l = new Lease(leaseNumberInt, propertyId, unitId == null ? 0 : unitId,
                        parseDate(opt(null, null)), parseDate(opt(null, null)), year);
                l.insert();
                // also associate the tenant to the lease
                l.addTenant(String.valueOf(tenantId));
                leaseMap.put(String.valueOf(leaseNumberInt), l);
            } else {
                LOG.info("Found existing lease: " + leaseNumberInt);
                if (!l.getTenants().contains(String.valueOf(tenantId))) {
                    l.getTenants().add(String.valueOf(tenantId));
                    l.addTenant(String.valueOf(tenantId));
                }
            }
        }
        return String.valueOf(tenantId);
    }

    // Replace local property/unit resolution with helper
    private int resolvePropertyId(Connection conn, String name) throws SQLException {
        return Units.resolvePropertyId(conn, name);
    }

    private Integer resolveUnitId(Connection conn, int propertyId, String unitName) throws SQLException {
        return Units.resolveUnitId(conn, propertyId, unitName);
    }

    private String calcRentMonth(String dueDateStr) {
        java.sql.Date d = parseDate(dueDateStr);
        if (d == null) return "";
        LocalDate ld = new java.util.Date(d.getTime()).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().plusDays(5);
        return ld.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH);
    }

    private String cleanupMethod(String method) {
        if (method == null) return "";
        String[] parts = method.split(",");
        if (parts.length > 1) {
            String firstTrim = parts[0].trim();
            boolean allSame = true;
            for (String p : parts) {
                if (!firstTrim.equalsIgnoreCase(p.trim())) { allSame = false; break; }
            }
            if (allSame) return firstTrim;
        }
        return method;
    }

    private String normalizeType(String type) {
        if (type == null) return "";
        String t = type.trim();
        if (t.equalsIgnoreCase("Income / Recurring Monthly")) return "Recurring Monthly";
        if (t.equalsIgnoreCase("Income / One Time")) return "One Time";
        return t;
    }

    private String normalizeCategoryAndAdjustType(String category, String type) {
        String cat = category == null ? "" : category.trim();
        String t = type;
        if (cat.toLowerCase(Locale.ROOT).startsWith("tenant charges & fees /")) {
            cat = cat.substring("tenant charges & fees /".length()).trim();
            if (equalsIgnoreIgnoreNull(cat, "Water fee") || equalsIgnoreIgnoreNull(cat, "Gas fee") || equalsIgnoreIgnoreNull(cat, "Electricity fee")) {
                t = "Reimbursement";
            }
            if (equalsIgnoreIgnoreNull(cat, "Pet charge")) {
                cat = "Pet Rent";
            }
        } else if (cat.toLowerCase(Locale.ROOT).startsWith("deposit /")) {
            cat = cat.substring("deposit /".length()).trim();
            t = "Deposit";
        } else if (cat.toLowerCase(Locale.ROOT).startsWith("rent /")) {
            cat = cat.substring("rent /".length()).trim();
        }
        return cat + "\u0000" + t;
    }

    private boolean equalsIgnoreCase(String a, String b) {
        return a != null && b != null && a.trim().equalsIgnoreCase(b.trim());
    }
    private boolean equalsIgnoreIgnoreNull(String a, String b) {
        return a != null && b != null && a.trim().equalsIgnoreCase(b.trim());
    }

    private boolean containsAny(String s, boolean caseInsensitive, String... tokens) {
        if (s == null) return false;
        String hay = caseInsensitive ? s.toLowerCase(Locale.ROOT) : s;
        for (String tok : tokens) {
            String needle = caseInsensitive ? tok.toLowerCase(Locale.ROOT) : tok;
            if (hay.contains(needle)) return true;
        }
        return false;
    }

    private String get(Map<String, Object> row, String header) {
        if (header == null) return null;
        Object val = row.get(header);
        if (val == null) return null;
        String s = String.valueOf(val).trim();
        if ("-".equals(s)) return null;
        return s;
    }

    private String opt(Map<String, Object> row, String header) {
        if (row == null || header == null) return null;
        Object val = row.get(header);
        if (val == null) return null;
        String s = String.valueOf(val).trim();
        if (s.isEmpty() || "-".equals(s)) return null;
        return s;
    }

    private java.sql.Date parseDate(String s) {
        if (s == null || s.isEmpty()) return null;
        try {
            // Try ISO yyyy-MM-dd first
            return java.sql.Date.valueOf(s);
        } catch (IllegalArgumentException ex) {
            // Fallback: MM/dd/yyyy
            try {
                java.text.SimpleDateFormat fmt = new java.text.SimpleDateFormat("MM/dd/yyyy");
                fmt.setLenient(true);
                java.util.Date d = fmt.parse(s);
                return new java.sql.Date(d.getTime());
            } catch (Exception e) {
                // Fallback: dd-MMM-yy
                try {
                    java.text.SimpleDateFormat fmt = new java.text.SimpleDateFormat("dd-MMM-yy");
                    fmt.setLenient(true);
                    java.util.Date d = fmt.parse(s);
                    return new java.sql.Date(d.getTime());
                } catch (Exception e2) {
                    return null;
                }
            }
        }
    }

    private BigDecimal parseMoney(String s) {
        if (s == null || s.isEmpty()) return null;
        String clean = s.replaceAll(",", "").replaceAll("[$]", "").trim();
        if (clean.isEmpty()) return null;
        try {
            return new BigDecimal(clean);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
