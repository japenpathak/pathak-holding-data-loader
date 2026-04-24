package com.ph.dl.gmail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;

/**
 * Service for reading Gmail messages using IMAP.
 * 
 * To enable:
 * 1. Set email.gmail.enabled=true in application.yml
 * 2. Add your Gmail credentials (use App Password, not regular password)
 * 3. Call this service from Application.main() if desired
 */
@Component
public class GmailReaderService {

    private static final Logger LOG = LoggerFactory.getLogger(GmailReaderService.class);

    @Value("${email.gmail.enabled:false}")
    private boolean gmailEnabled;

    @Value("${email.gmail.username:your-email@gmail.com}")
    private String gmailUsername;

    @Value("${email.gmail.password:your-app-password}")
    private String gmailPassword;

    /**
     * Example: Read Gmail messages from the past 7 days.
     */
    public void readRecentEmails() throws Exception {
        // Check if Gmail integration is enabled
        if (!gmailEnabled) {
            LOG.info("Gmail integration is disabled. Set email.gmail.enabled=true to enable.");
            return;
        }

        if (gmailUsername == null || gmailPassword == null || 
            gmailUsername.equals("your-email@gmail.com") || 
            gmailPassword.equals("your-app-password")) {
            LOG.warn("Gmail credentials not configured. Please update application.yml with valid credentials.");
            return;
        }

        LOG.info("Reading Gmail messages for: {}", gmailUsername);

        try (GmailClient client = new GmailClient(gmailUsername, gmailPassword)) {
            client.connect();

            // Read emails from the past 7 days
            LocalDate endDate = LocalDate.now();
            LocalDate startDate = endDate.minusDays(7);

            List<EmailMessage> messages = client.readMessages(startDate, endDate);
            
            LOG.info("Found {} emails in the past 7 days", messages.size());

            // Process messages (example: log subject lines)
            for (EmailMessage msg : messages) {
                LOG.info("Email: [{}] {} (from: {}, date: {})", 
                    msg.isRead() ? "READ" : "UNREAD",
                    msg.getSubject(),
                    msg.getFrom(),
                    msg.getReceivedDate());
                
                // Check for attachments
                if (msg.getAttachments() != null && !msg.getAttachments().isEmpty()) {
                    LOG.info("  Attachments: {}", msg.getAttachments());
                }
            }
        } catch (Exception ex) {
            LOG.error("Failed to read Gmail messages: {}", ex.getMessage(), ex);
            throw ex;
        }
    }

    /**
     * Example: Read only unread emails within a date range.
     */
    public void readUnreadEmails(LocalDate startDate, LocalDate endDate) throws Exception {
        if (!gmailEnabled) {
            LOG.info("Gmail integration is disabled.");
            return;
        }

        try (GmailClient client = new GmailClient(gmailUsername, gmailPassword)) {
            client.connect();

            List<EmailMessage> messages = client.readUnreadMessages(startDate, endDate);
            
            LOG.info("Found {} unread emails from {} to {}", messages.size(), startDate, endDate);

            for (EmailMessage msg : messages) {
                LOG.info("Unread: {} (from: {})", msg.getSubject(), msg.getFrom());
            }
        }
    }
}
