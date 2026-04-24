package com.ph.dl.service;

import com.ph.dl.config.YamlConfig;
import com.ph.dl.database.DatabaseManager;
import com.ph.dl.db.helper.Units;
import com.ph.dl.util.CsvParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Loads raw bank transactions from a Chase export CSV into Bank_transaction_staging,
 * then generates Bank_Payment rows for specific loan payments.
 *
 * Contract:
 * - Input: CSV path from application.yml (bank.transactions.input)
 * - Output:
 *   1) Insert every CSV row into Bank_transaction_staging with minimal type parsing.
 *   2) For matching DEBIT rows with ORIG CO NAME prefix rules, insert into Bank_Payment.
 */
@Component
public class BankTransactionsLoader {

    private static final Logger LOG = LoggerFactory.getLogger(BankTransactionsLoader.class);

    private static final DateTimeFormatter POSTING_DATE_FMT = DateTimeFormatter.ofPattern("M/d/yyyy", Locale.US);

    public void loadOnStartup() throws Exception {
        YamlConfig cfg = new YamlConfig();
        String path = cfg.getBankTransactionsCsvPath();
        int year = cfg.getBankTransactionsYear();
        if (year == 0) {
            throw new IllegalStateException("bank.transactions.year not configured (check application.yml)");
        }

        List<Map<String, Object>> rows = CsvParser.parse(path);

        try (Connection conn = DatabaseManager.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            try {
                ensureStagingTable(conn);

                int fileId = upsertFileProcessingAndGetId(conn, path, year, "bank_transactions_raw");

                insertRawIntoStaging(conn, rows, year, fileId);
                processStagingForFile(conn, fileId, year);
                updateFileProcessingCounts(conn, fileId);

                conn.commit();
            } catch (Exception ex) {
                conn.rollback();
                LOG.error("Error processing bank transactions file: " + path + " => " + ex.getMessage(), ex);
                throw ex;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private int upsertFileProcessingAndGetId(Connection conn, String path, int year, String fileType) throws SQLException {
        // Ensure a row exists
        try (PreparedStatement ps = conn.prepareStatement(
                "MERGE INTO File_Processing (File_Path, Processed_At, Year, File_Type) KEY(File_Path) VALUES (?, CURRENT_TIMESTAMP(), ?, ?)")) {
            ps.setString(1, path);
            ps.setInt(2, year);
            ps.setString(3, fileType);
            ps.executeUpdate();
        }
        // Fetch its ID
        try (PreparedStatement ps = conn.prepareStatement("SELECT ID FROM File_Processing WHERE File_Path=?")) {
            ps.setString(1, path);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        throw new SQLException("Unable to resolve File_Processing.ID for file: " + path);
    }

    private void ensureStagingTable(Connection conn) throws SQLException {
        // Defense-in-depth for tests/older schemas.
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS Bank_transaction_staging (" +
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
                    ")");

            // evolve if table exists without the new columns
            stmt.execute("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS FILE_ID INT");
            stmt.execute("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS IS_PROCESSED VARCHAR(1)");
            stmt.execute("ALTER TABLE File_Processing ADD COLUMN IF NOT EXISTS Total_Rows INT");
            stmt.execute("ALTER TABLE File_Processing ADD COLUMN IF NOT EXISTS Success_Rows INT");
            stmt.execute("ALTER TABLE File_Processing ADD COLUMN IF NOT EXISTS Failed_Rows INT");
        }
    }

    private void insertRawIntoStaging(Connection conn, List<Map<String, Object>> rows, int year, int fileId) throws SQLException {
        String sql = "INSERT INTO Bank_transaction_staging (FILE_ID, Details, Posting_Date, Description, Amount, Type, Check_Num, Year, IS_PROCESSED) VALUES (?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Map<String, Object> row : rows) {
                // CsvParser normalizes headers (trim + lowercase)
                String details = opt(row, "details");
                java.sql.Date postingDate = parsePostingDate(opt(row, "posting date"));
                String desc = opt(row, "description");
                if (desc != null && desc.length() > 400) {
                    desc = desc.substring(0, 400);
                }
                BigDecimal amount = parseMoney(opt(row, "amount"));
                String type = opt(row, "type");
                String checkNum = opt(row, "check or slip #");

                ps.setInt(1, fileId);
                ps.setString(2, emptyToNull(details));
                ps.setDate(3, postingDate);
                ps.setString(4, emptyToNull(desc));
                if (amount == null) {
                    ps.setNull(5, java.sql.Types.DECIMAL);
                } else {
                    ps.setBigDecimal(5, amount);
                }
                ps.setString(6, emptyToNull(type));
                ps.setString(7, emptyToNull(checkNum));
                ps.setInt(8, year);
                ps.setNull(9, java.sql.Types.VARCHAR); // default NULL until processed
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /**
     * Process staging rows for the given file. For each row:
     * - if it matches the Bank_Payment rules and inserts successfully -> IS_PROCESSED='Y'
     * - if it matches but errors during processing -> IS_PROCESSED='N'
     * - otherwise leave NULL (not applicable)
     */
    private void processStagingForFile(Connection conn, int fileId, int year) throws SQLException {
        String selectSql = "SELECT ID, Details, Posting_Date, Description, Amount FROM Bank_transaction_staging WHERE FILE_ID=?";
        String markSql = "UPDATE Bank_transaction_staging SET IS_PROCESSED=? WHERE ID=?";

        try (PreparedStatement sel = conn.prepareStatement(selectSql);
             PreparedStatement mark = conn.prepareStatement(markSql)) {

            sel.setInt(1, fileId);
            try (ResultSet rs = sel.executeQuery()) {
                int seq = nextBankPaymentSequence(conn, year);

                while (rs.next()) {
                    int stagingId = rs.getInt("ID");
                    String details = rs.getString("Details");
                    java.sql.Date postingDate = rs.getDate("Posting_Date");
                    String description = rs.getString("Description");
                    BigDecimal amount = rs.getBigDecimal("Amount");

                    if (description == null || !equalsIgnoreCase(details, "DEBIT")) {
                        continue;
                    }

                    String upper = description.toUpperCase(Locale.ROOT);
                    boolean matches = upper.startsWith("ORIG CO NAME:FIRST KEYSTONE C")
                            || upper.startsWith("ORIG CO NAME:NSM DBAMR.COOPER")
                            || upper.startsWith("ORIG CO NAME:FLAGSTAR BANK NA");
                    if (!matches) {
                        continue;
                    }

                    try {
                        int propertyId = resolvePropertyForLoan(descStarts(upper), upper, conn);
                        String bankName = extractBankName(description);
                        if (amount == null) {
                            throw new SQLException("Amount is NULL for staging row ID=" + stagingId);
                        }
                        String txnId = year + "-BANKPAY-" + (seq++);

                        try (PreparedStatement ins = conn.prepareStatement(
                                "MERGE INTO Bank_Payment (Transaction_Id, Date, Property, Bank, Total_Amount, Interest, Principal, Year) KEY(Transaction_Id) VALUES (?,?,?,?,?,?,?,?)")) {
                            ins.setString(1, txnId);
                            ins.setDate(2, postingDate);
                            ins.setInt(3, propertyId);
                            ins.setString(4, bankName);
                            ins.setBigDecimal(5, amount);
                            ins.setBigDecimal(6, BigDecimal.ZERO);
                            ins.setBigDecimal(7, BigDecimal.ZERO);
                            ins.setInt(8, year);
                            ins.executeUpdate();
                        }

                        mark.setString(1, "Y");
                        mark.setInt(2, stagingId);
                        mark.addBatch();
                    } catch (Exception ex) {
                        LOG.warn("Failed processing staging row ID={} for fileId={}: {}", stagingId, fileId, ex.getMessage());
                        mark.setString(1, "N");
                        mark.setInt(2, stagingId);
                        mark.addBatch();
                    }
                }
            }

            mark.executeBatch();
        }
    }

    private void updateFileProcessingCounts(Connection conn, int fileId) throws SQLException {
        int total;
        int success;
        int failed;

        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM Bank_transaction_staging WHERE FILE_ID=?")) {
            ps.setInt(1, fileId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                total = rs.getInt(1);
            }
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM Bank_transaction_staging WHERE FILE_ID=? AND IS_PROCESSED='Y'")) {
            ps.setInt(1, fileId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                success = rs.getInt(1);
            }
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM Bank_transaction_staging WHERE FILE_ID=? AND IS_PROCESSED='N'")) {
            ps.setInt(1, fileId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                failed = rs.getInt(1);
            }
        }

        try (PreparedStatement ps = conn.prepareStatement("UPDATE File_Processing SET Total_Rows=?, Success_Rows=?, Failed_Rows=? WHERE ID=?")) {
            ps.setInt(1, total);
            ps.setInt(2, success);
            ps.setInt(3, failed);
            ps.setInt(4, fileId);
            ps.executeUpdate();
        }
    }

    private int resolvePropertyForLoan(String prefix, String descriptionUpper, Connection conn) throws SQLException {
        // Per requirements
        // - First Keystone C: check specific loan markers to decide property
        // - NSM DBAMR.COOPER / FLAGSTAR BANK NA: property "414-416 E Main St"
        if ("ORIG CO NAME:FIRST KEYSTONE C".equals(prefix)) {
            if (descriptionUpper.contains("FKCB LOAN PYMT - 50   01223")) {
                return Units.resolvePropertyId(conn, "408-410 E Main St");
            }
            if (descriptionUpper.contains("FKCB LOAN PYMT - 50   01207 4")) {
                return Units.resolvePropertyId(conn, "104-108 Madison St");
            }
            // If unknown First Keystone loan, keep placeholder property
            return Units.resolvePropertyId(conn, null);
        }
        if ("ORIG CO NAME:NSM DBAMR.COOPER".equals(prefix) || "ORIG CO NAME:FLAGSTAR BANK NA".equals(prefix)) {
            return Units.resolvePropertyId(conn, "414-416 E Main St");
        }
        return Units.resolvePropertyId(conn, null);
    }

    private String descStarts(String descriptionUpper) {
        if (descriptionUpper.startsWith("ORIG CO NAME:FIRST KEYSTONE C")) return "ORIG CO NAME:FIRST KEYSTONE C";
        if (descriptionUpper.startsWith("ORIG CO NAME:NSM DBAMR.COOPER")) return "ORIG CO NAME:NSM DBAMR.COOPER";
        if (descriptionUpper.startsWith("ORIG CO NAME:FLAGSTAR BANK NA")) return "ORIG CO NAME:FLAGSTAR BANK NA";
        return "";
    }

    private int nextBankPaymentSequence(Connection conn, int year) throws SQLException {
        String prefix = year + "-BANKPAY-";
        String sql = "SELECT MAX(CAST(SUBSTRING(Transaction_Id, LENGTH(?) + 1) AS INT)) FROM Bank_Payment WHERE Transaction_Id LIKE ?";
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
     * Bank Name is text between "ORIG CO NAME:" and "ORIG ID:" trimmed.
     */
    static String extractBankName(String description) {
        if (description == null) return null;
        String startKey = "ORIG CO NAME:";
        String endKey = "ORIG ID:";
        int start = description.toUpperCase(Locale.ROOT).indexOf(startKey);
        if (start < 0) return null;
        start += startKey.length();
        int end = description.toUpperCase(Locale.ROOT).indexOf(endKey, start);
        String raw = end >= 0 ? description.substring(start, end) : description.substring(start);
        return raw.trim();
    }

    private static java.sql.Date parsePostingDate(String raw) {
        if (raw == null || raw.isBlank()) return null;
        try {
            LocalDate ld = LocalDate.parse(raw.trim(), POSTING_DATE_FMT);
            return java.sql.Date.valueOf(ld);
        } catch (DateTimeParseException ex) {
            // Keep null if unparseable
            return null;
        }
    }

    private static BigDecimal parseMoney(String raw) {
        if (raw == null) return null;
        String s = raw.trim();
        if (s.isEmpty()) return null;
        // Chase export uses -123.45, but be tolerant of $ and commas.
        s = s.replace("$", "").replace(",", "");
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException nfe) {
            return null;
        }
    }

    private static String opt(Map<String, Object> row, String key) {
        if (row == null || key == null) return null;
        Object v = row.get(key);
        return v == null ? null : String.valueOf(v);
    }

    private static String emptyToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static boolean equalsIgnoreCase(String a, String b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equalsIgnoreCase(b);
    }
}