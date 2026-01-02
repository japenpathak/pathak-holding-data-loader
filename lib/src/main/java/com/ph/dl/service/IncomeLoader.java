package com.ph.dl.service;

import com.ph.dl.config.YamlConfig;
import com.ph.dl.database.DatabaseManager;
import com.ph.dl.model.Lease;
import com.ph.dl.model.Tenant;
import com.ph.dl.model.Income;
import com.ph.dl.util.CsvParser;
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
    

    public void loadOnStartup() throws Exception {
        YamlConfig cfg = new YamlConfig();
        String path = cfg.getIncomeCsvPath();
        boolean forceOverwrite = cfg.isForceOverwrite();
        int year = cfg.getYear();
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
                if (!forceOverwrite && isFileProcessed(conn, path, year)) {
                    throw new RuntimeException("File already processed for year " + year + ": " + path);
                }
                // Process rows and insert in bulk
                processAndInsert(conn, rows, map, year, forceOverwrite);

                // Generate vacancy loss rows after income load
                int insertedVacancy = generateVacancyLossForYear(conn, year);
                LOG.info("Vacancy loss rows inserted for year {}: {}", year, insertedVacancy);

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
            LOG.debug("Processing row: "+ counter++ +" with Data:" + row.get("Transaction ID"));
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
                        System.out.println("Duplicate income skipped: " + income.getTransactionId());
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

        int propertyId = resolvePropertyId(conn, propertyName);
        int unitId = resolveUnitId(conn, propertyId, unitName);

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
                l = new Lease(leaseNumberInt, propertyId, unitId,
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

    private int resolvePropertyId(Connection conn, String name) throws SQLException {
        if (name == null || name.isEmpty()) {
            return ensurePlaceholderProperty(conn);
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT ID FROM Property WHERE LOWER(Name)=LOWER(?)")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        LOG.warn("Property not found, creating placeholder for name=" + name);
        return ensurePlaceholderProperty(conn);
    }

    private int ensurePlaceholderProperty(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT ID FROM Property WHERE Name=?")) {
            ps.setString(1, "Unknown Property");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        try (PreparedStatement ins = conn.prepareStatement("INSERT INTO Property (Name, Full_Address, Number_of_units) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {
            ins.setString(1, "Unknown Property");
            ins.setString(2, "");
            ins.setInt(3, 0);
            ins.executeUpdate();
            try (ResultSet keys = ins.getGeneratedKeys()) {
                if (keys.next()) return keys.getInt(1);
            }
        }
        return 0;
    }

    private Integer resolveUnitId(Connection conn, int propertyId, String unitName) throws SQLException {
        if (propertyId == 0) return 0;
        if (unitName == null || unitName.isEmpty()) {
        	LOG.error("Unit not found, creating placeholder for propertyId=" + propertyId + ", unitName=" + unitName);
        	return null;
            //return ensurePlaceholderUnit(conn, propertyId);
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT ID FROM Units WHERE Property=? AND LOWER(Name)=LOWER(?)")) {
            ps.setInt(1, propertyId);
            ps.setString(2, unitName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) 
                	return rs.getInt(1);
            }
        }
        LOG.error("Unit not found, creating placeholder for propertyId=" + propertyId + ", unitName=" + unitName);
       return null;
    }

    private int ensurePlaceholderUnit(Connection conn, int propertyId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT ID FROM Units WHERE Property=? AND Name=?")) {
            ps.setInt(1, propertyId);
            ps.setString(2, "Unknown Unit");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        try (PreparedStatement ins = conn.prepareStatement("INSERT INTO Units (Name, Property, Bedrooms, Bathrooms, Has_Porch, Has_Back_Yard, Washer_Dryer) VALUES (?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {
            ins.setString(1, "Unknown Unit");
            ins.setInt(2, propertyId);
            ins.setInt(3, 0);
            ins.setInt(4, 0);
            ins.setString(5, "N");
            ins.setString(6, "N");
            ins.setString(7, "N");
            //LOG.info("Inserting Unit:"+ );
            ins.executeUpdate();
            try (ResultSet keys = ins.getGeneratedKeys()) {
                if (keys.next()) return keys.getInt(1);
            }
        }
        return 0;
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

    private int nextVacantSequence(Connection conn, int year) throws SQLException {
        // Transaction_ID format: <Year>-Vacant-<n>
        // Get max n.
        String sql = "SELECT MAX(CAST(SUBSTRING(Transaction_ID, LENGTH(?) + 1) AS INT)) " +
                "FROM Income WHERE Transaction_ID LIKE ?";
        // prefix e.g. "2025-Vacant-"
        String prefix = year + "-Vacant-";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, prefix);
            ps.setString(2, prefix + "%");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int max = rs.getInt(1);
                    if (rs.wasNull()) return 1;
                    return max + 1;
                }
            }
        }
        return 1;
    }

    /**
     * Generate synthetic Income rows representing vacancy loss.
     *
     * Rules (per your spec):
     * - Analyze Income rows per (Property, Unit) for the given year.
     * - Check Jan..Dec for presence of Transaction_category "Rent".
     * - Also check "Pet Rent" only if that unit has any Pet Rent rows in that year.
     * - If a month is missing required category, insert a row:
     *   - Transaction_ID: <Year>-Vacant-1,2,3...
     *   - Status: "Vacancy"
     *   - Type: "Income"
     *   - Transaction_category: "Rent"
     *   - Rent_Month: derived from due date using existing calcRentMonth logic
     *   - Date_created & Due_date: first day of that month
     *   - Date_paid: NULL
     *   - Payer, LeaseNum, Method_of_payment: NULL
     *   - Original_amount: lowest available Original_amount for category "Rent" for that unit/year
     *   - Vacancy: same as Original_amount
     */
    public int generateVacancyLossForYear(Connection conn, int year) throws SQLException {
        int nextSeq = nextVacantSequence(conn, year);
        String unitSql = "SELECT DISTINCT Property, Unit FROM Income WHERE Year = ? AND Property IS NOT NULL AND Unit IS NOT NULL";

        int inserted = 0;
        try (PreparedStatement unitPs = conn.prepareStatement(unitSql)) {
            unitPs.setInt(1, year);
            try (ResultSet unitRs = unitPs.executeQuery()) {
                while (unitRs.next()) {
                    int propertyId = unitRs.getInt(1);
                    int unitId = unitRs.getInt(2);

                    java.math.BigDecimal baseRent = lowestRentAmount(conn, year, propertyId, unitId);
                    if (baseRent == null) {
                        continue;
                    }

                    boolean requiresPetRent = unitHasPetRent(conn, year, propertyId, unitId);

                    boolean[] hasRent = monthsWithCategory(conn, year, propertyId, unitId, "Rent");
                    boolean[] hasPetRent = requiresPetRent ? monthsWithCategory(conn, year, propertyId, unitId, "Pet Rent") : null;

                    for (int month = 1; month <= 12; month++) {
                        boolean missingRent = !hasRent[month];
                        boolean missingPet = requiresPetRent && (hasPetRent == null || !hasPetRent[month]);

                        // Per spec: add vacancy row if ANY required category is missing.
                        if (missingRent || missingPet) {
                            java.sql.Date firstOfMonth = java.sql.Date.valueOf(java.time.LocalDate.of(year, month, 1));

                            // Don't insert duplicates if this is re-run.
                            if (vacancyRowExists(conn, year, propertyId, unitId, firstOfMonth)) {
                                continue;
                            }

                            String rentMonth = calcRentMonth(firstOfMonth.toString());
                            String txnId = year + "-Vacant-" + (nextSeq++);

                            try (PreparedStatement ins = conn.prepareStatement(
                                    "INSERT INTO Income (Transaction_ID, Status, Date_created, Due_date, Date_paid, Type, Transaction_category, Rent_Month, Original_amount, Payment, Balance, Vacancy, Method_of_payment, Payer, LeaseNum, Property, Unit, Transaction_details, Exclude_Zaky, SD_Applied, Year) " +
                                            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                                ins.setString(1, txnId);
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
                                inserted++;
                            }
                        }
                    }
                }
            }
        }

        return inserted;
    }

    private boolean vacancyRowExists(Connection conn, int year, int propertyId, int unitId, java.sql.Date dueDate) throws SQLException {
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

    private java.math.BigDecimal lowestRentAmount(Connection conn, int year, int propertyId, int unitId) throws SQLException {
        String sql = "SELECT MIN(Original_amount) FROM Income WHERE Year=? AND Property=? AND Unit=? AND Transaction_category='Rent' AND Original_amount IS NOT NULL";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, year);
            ps.setInt(2, propertyId);
            ps.setInt(3, unitId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    java.math.BigDecimal v = rs.getBigDecimal(1);
                    return v;
                }
            }
        }
        return null;
    }

    private boolean unitHasPetRent(Connection conn, int year, int propertyId, int unitId) throws SQLException {
        String sql = "SELECT 1 FROM Income WHERE Year=? AND Property=? AND Unit=? AND Transaction_category='Pet Rent' FETCH FIRST 1 ROWS ONLY";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, year);
            ps.setInt(2, propertyId);
            ps.setInt(3, unitId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean[] monthsWithCategory(Connection conn, int year, int propertyId, int unitId, String category) throws SQLException {
        // index 1..12
        boolean[] present = new boolean[13];
        String sql = "SELECT Due_date FROM Income WHERE Year=? AND Property=? AND Unit=? AND Transaction_category=? AND Due_date IS NOT NULL";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, year);
            ps.setInt(2, propertyId);
            ps.setInt(3, unitId);
            ps.setString(4, category);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    java.sql.Date d = rs.getDate(1);
                    if (d == null) continue;
                    int m = d.toLocalDate().getMonthValue();
                    if (m >= 1 && m <= 12) present[m] = true;
                }
            }
        }
        return present;
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
