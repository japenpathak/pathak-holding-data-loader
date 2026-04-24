package com.ph.dl.service;

import com.ph.dl.config.YamlConfig;
import com.ph.dl.database.DatabaseManager;
import com.ph.dl.db.helper.Units;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Processor for Service Electric internet utility transactions.
 *
 * Rules:
 * - Select staging rows where Details='DEBIT' (case-insensitive) and Description starts with "ORIG CO NAME:SERVICE ELECTRIC".
 * - Extract trailing token starting with "TRN:" (from Description last word) => <TRN>.
 * - Expense.Transaction = "SE-" + <year> + "-" + <TRN>
 * - Date = Posting_Date
 * - Year = configured processing year (bank.transactions.year)
 * - Amount = staging.Amount
 * - Details = "Service Eletcric Internet Service"
 * - Payment_Mode = staging.Type
 * - Payment_Account derived from File_Processing.File_Path for staging.FILE_ID: <Bank>-<Account>
 *   where file name (no path) split by '_' => [0]=Bank, [1]=Account
 * - Property:
 *   if Amount > 50 => "104-108 Madison St" else "423 Scott St"
 * - Unit kept null
 * - Category="Utility", Provider="Service Electric", Type="Operation"
 * - Exclude='N', Exclude_Zaky='N'
 */
@Component
public class InternetUtilityProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(InternetUtilityProcessor.class);

    private static final String DESCRIPTION_PREFIX = "ORIG CO NAME:SERVICE ELECTRIC";

    public void loadOnStartup() throws Exception {
        YamlConfig cfg = new YamlConfig();
        int year = cfg.getBankTransactionsYear();
        if (year == 0) {
            throw new IllegalStateException("bank.transactions.year not configured (check application.yml)");
        }

        try (Connection conn = DatabaseManager.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            try {
                ensureStagingEvolvable(conn);

                int processed = process(conn, year);
                conn.commit();
                LOG.info("InternetUtilityProcessor processed {} staging rows for year {}", processed, year);
            } catch (Exception ex) {
                conn.rollback();
                LOG.error("InternetUtilityProcessor failed: {}", ex.getMessage(), ex);
                throw ex;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    /**
     * Execute processor against an existing connection/transaction.
     * @return number of staging rows that were marked processed (Y/N)
     */
    public int process(Connection conn, int year) throws SQLException {
        int utilityCategoryId = resolveExpenseCategoryId(conn, "Utility");
        int providerId = resolveProviderId(conn, "Service Electric");

        String sql = "SELECT ID, FILE_ID, Posting_Date, Description, Amount, Type, Details " +
                "FROM Bank_transaction_staging " +
                "WHERE Year=? AND Description IS NOT NULL AND UPPER(Description) LIKE ?";

        int count = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, year);
            ps.setString(2, DESCRIPTION_PREFIX + "%");

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int stagingId = rs.getInt("ID");
                    int fileId = rs.getInt("FILE_ID");
                    java.sql.Date postingDate = rs.getDate("Posting_Date");
                    String description = rs.getString("Description");
                    BigDecimal amount = rs.getBigDecimal("Amount");
                    String paymentMode = rs.getString("Type");
                    String details = rs.getString("Details");

                    if (!isDebit(details)) {
                        continue;
                    }

                    String trn = extractTrnFromDescription(description);
                    if (trn == null || trn.isBlank()) {
                        LOG.warn("Skipping Service Electric row without TRN token: stagingId={}", stagingId);
                        // Not a match we can process; mark as failed (N) so it is visible.
                        markStagingProcessed(conn, stagingId, "N");
                        count++;
                        continue;
                    }

                    if (postingDate == null) {
                        LOG.warn("Skipping Service Electric row without Posting_Date: stagingId={}", stagingId);
                        markStagingProcessed(conn, stagingId, "N");
                        count++;
                        continue;
                    }

                    if (amount == null) {
                        LOG.warn("Skipping Service Electric row without Amount: stagingId={}", stagingId);
                        markStagingProcessed(conn, stagingId, "N");
                        count++;
                        continue;
                    }

                    String transactionId = "SE-" + year + "-" + trn;
                    String paymentAccount = resolvePaymentAccount(conn, fileId);

                    int propertyId;
                    if (amount.compareTo(new BigDecimal("50")) > 0) {
                        propertyId = Units.resolvePropertyId(conn, "104-108 Madison St");
                    } else {
                        propertyId = Units.resolvePropertyId(conn, "423 Scott St");
                    }

                    // Unit kept NULL.
                    Integer unitId = null;

                    try {
                        upsertExpense(conn,
                                transactionId,
                                postingDate,
                                year,
                                amount,
                                "Service Eletcric Internet Service",
                                paymentMode,
                                paymentAccount,
                                propertyId,
                                unitId,
                                utilityCategoryId,
                                providerId,
                                "Operation",
                                "N",
                                "N");

                        markStagingProcessed(conn, stagingId, "Y");
                    } catch (Exception ex) {
                        LOG.warn("Failed inserting/updating Expense for stagingId={}: {}", stagingId, ex.getMessage());
                        markStagingProcessed(conn, stagingId, "N");
                    }
                    count++;
                }
            }
        }

        return count;
    }

    private void ensureStagingEvolvable(Connection conn) throws SQLException {
        // Ensure column exists for any older DB.
        try (PreparedStatement ps = conn.prepareStatement("ALTER TABLE Bank_transaction_staging ADD COLUMN IF NOT EXISTS IS_PROCESSED VARCHAR(1)")) {
            ps.execute();
        }
    }

    private void upsertExpense(Connection conn,
                              String transactionId,
                              java.sql.Date date,
                              int year,
                              BigDecimal amount,
                              String details,
                              String paymentMode,
                              String paymentAccount,
                              int property,
                              Integer unit,
                              int category,
                              int provider,
                              String type,
                              String exclude,
                              String excludeZaky) throws SQLException {

        String sql = "MERGE INTO Expense (Transaction, Date, Amount, Details, Payment_Mode, Check_num, Payment_Account, Property, Unit, Category, Provider, Type, Exclude, Exclude_Zaky, Year) " +
                "KEY(Transaction) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, transactionId);
            ps.setDate(2, date);
            ps.setBigDecimal(3, amount);
            ps.setString(4, details);
            ps.setString(5, paymentMode);
            ps.setNull(6, java.sql.Types.VARCHAR);
            ps.setString(7, paymentAccount);
            ps.setInt(8, property);
            if (unit == null) {
                ps.setNull(9, java.sql.Types.INTEGER);
            } else {
                ps.setInt(9, unit);
            }
            ps.setInt(10, category);
            ps.setInt(11, provider);
            ps.setString(12, type);
            ps.setString(13, exclude);
            ps.setString(14, excludeZaky);
            ps.setInt(15, year);
            ps.executeUpdate();
        }
    }

    private void markStagingProcessed(Connection conn, int stagingId, String flag) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("UPDATE Bank_transaction_staging SET IS_PROCESSED=? WHERE ID=?")) {
            ps.setString(1, flag);
            ps.setInt(2, stagingId);
            ps.executeUpdate();
        }
    }

    private static boolean isDebit(String details) {
        return details != null && details.trim().equalsIgnoreCase("DEBIT");
    }

    static String extractTrnFromDescription(String description) {
        if (description == null) return null;
        String trimmed = description.trim();
        if (trimmed.isEmpty()) return null;

        // Requirement: extract the word that starts with "TRN:" (not necessarily the last word).
        String[] tokens = trimmed.split("\\s+");
        for (int i=0;i<tokens.length;i++) {
        	String tok = tokens[i];
        	if (tok == null) continue;
            String t = tok.trim();
            if (t.isEmpty()) continue;
            if (t.toUpperCase(Locale.ROOT).startsWith("TRN:")) {
            	if(t.length() > 4) {
            		return t.substring(4).trim();
            	}else {
            		// Check bounds before accessing next token
            		if (i + 1 < tokens.length) {
            			String trn = tokens[i+1].trim();
            			return trn.isEmpty() ? null : trn;
            		}
            		return null;  // No next token available
            	}
            }
        }
        return null;
    }

    private String resolvePaymentAccount(Connection conn, int fileId) throws SQLException {
        String filePath = null;
        try (PreparedStatement ps = conn.prepareStatement("SELECT File_Path FROM File_Processing WHERE ID=?")) {
            ps.setInt(1, fileId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) filePath = rs.getString(1);
            }
        }
        if (filePath == null || filePath.isBlank()) {
            return null;
        }

        String fileName;
        try {
            fileName = Paths.get(filePath).getFileName().toString();
        } catch (Exception ex) {
            fileName = filePath;
        }

        String[] parts = fileName.split("_");
        if (parts.length < 2) {
            return fileName;
        }
        String bankName = parts[0].trim();
        String accountNumber = parts[1].trim();
        return bankName + "-" + accountNumber;
    }

    private int resolveExpenseCategoryId(Connection conn, String categoryName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT Category_Id FROM Expense_Category WHERE LOWER(Category_Name)=LOWER(?)")) {
            ps.setString(1, categoryName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        throw new SQLException("Expense category not found: " + categoryName);
    }

    private int resolveProviderId(Connection conn, String providerName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT Provider_Id FROM Provider WHERE LOWER(Provider_Name)=LOWER(?)")) {
            ps.setString(1, providerName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        throw new SQLException("Provider not found: " + providerName);
    }
}
