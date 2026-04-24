package com.ph.dl.gmail;

import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.MimeMultipart;
import javax.mail.search.AndTerm;
import javax.mail.search.BodyTerm;
import javax.mail.search.ComparisonTerm;
import javax.mail.search.FlagTerm;
import javax.mail.search.FromStringTerm;
import javax.mail.search.ReceivedDateTerm;
import javax.mail.search.SearchTerm;
import javax.mail.search.SubjectTerm;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

/**
 * Gmail client for reading emails via IMAP protocol.
 * 
 * Usage:
 * <pre>
 * GmailClient client = new GmailClient("user@gmail.com", "app-password");
 * List<EmailMessage> messages = client.readMessages(
 *     LocalDate.of(2025, 1, 1), 
 *     LocalDate.of(2025, 1, 31)
 * );
 * client.close();
 * </pre>
 * 
 * Note: For Gmail, you need to use an App Password (not your regular password).
 * Enable 2-factor auth, then generate app password at: https://myaccount.google.com/apppasswords
 */
public class GmailClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GmailClient.class);

    private static final String GMAIL_IMAP_HOST = "imap.gmail.com";
    private static final String GMAIL_IMAP_PORT = "993";

    private final String username;
    private final String password;
    private Store store;
    private Folder inbox;

    /**
     * Create a Gmail client.
     * 
     * @param username Gmail email address (e.g., user@gmail.com)
     * @param password Gmail app password (not regular password - requires 2FA + app password)
     */
    public GmailClient(String username, String password) {
        this.username = username;
        this.password = password;
    }

    /**
     * Connect to Gmail IMAP server and open INBOX.
     */
    public void connect() throws MessagingException {
        if (store != null && store.isConnected()) {
            LOG.info("Already connected to Gmail");
            return;
        }

        Properties props = new Properties();
        props.put("mail.store.protocol", "imaps");
        props.put("mail.imap.host", GMAIL_IMAP_HOST);
        props.put("mail.imap.port", GMAIL_IMAP_PORT);
        props.put("mail.imap.ssl.enable", "true");
        props.put("mail.imap.ssl.trust", GMAIL_IMAP_HOST);
        props.put("mail.imap.connectiontimeout", "10000");
        props.put("mail.imap.timeout", "10000");

        Session session = Session.getInstance(props, null);
        store = session.getStore("imaps");

        LOG.info("Connecting to Gmail IMAP: {}", username);
        store.connect(GMAIL_IMAP_HOST, username, password);

        inbox = store.getFolder("INBOX");
        inbox.open(Folder.READ_ONLY);
        LOG.info("Connected. INBOX message count: {}", inbox.getMessageCount());
    }

    /**
     * Read emails within a date range (inclusive).
     * 
     * @param startDate Start date (inclusive)
     * @param endDate End date (inclusive)
     * @return List of email messages
     */
    public List<EmailMessage> readMessages(LocalDate startDate, LocalDate endDate) throws MessagingException {
        if (store == null || !store.isConnected()) {
            connect();
        }

        Date start = Date.from(startDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
        Date end = Date.from(endDate.plusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant());

        SearchTerm searchTerm = new AndTerm(
            new ReceivedDateTerm(ComparisonTerm.GE, start),
            new ReceivedDateTerm(ComparisonTerm.LT, end)
        );

        LOG.info("Searching emails from {} to {}", startDate, endDate);
        Message[] messages = inbox.search(searchTerm);
        LOG.info("Found {} messages", messages.length);

        List<EmailMessage> result = new ArrayList<>();
        for (Message msg : messages) {
            try {
            	LOG.info("Message Subject: {}", msg.getSubject());
            	LOG.info("Message Subject: {}", msg.getFrom()[0]);
                result.add(parseMessage(msg));
            } catch (Exception ex) {
                LOG.warn("Failed to parse message: {}", ex.getMessage());
            }
        }

        return result;
    }

    /**
     * Read all unread emails within a date range.
     */
    public List<EmailMessage> readUnreadMessages(LocalDate startDate, LocalDate endDate) throws MessagingException {
        if (store == null || !store.isConnected()) {
            connect();
        }

        Date start = Date.from(startDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
        Date end = Date.from(endDate.plusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant());

        SearchTerm searchTerm = new AndTerm(
            new AndTerm(
                new ReceivedDateTerm(ComparisonTerm.GE, start),
                new ReceivedDateTerm(ComparisonTerm.LT, end)
            ),
            new FlagTerm(new Flags(Flags.Flag.SEEN), false)
        );

        LOG.info("Searching unread emails from {} to {}", startDate, endDate);
        Message[] messages = inbox.search(searchTerm);
        LOG.info("Found {} unread messages", messages.length);

        List<EmailMessage> result = new ArrayList<>();
        for (Message msg : messages) {
            try {
                result.add(parseMessage(msg));
            } catch (Exception ex) {
                LOG.warn("Failed to parse message: {}", ex.getMessage());
            }
        }

        return result;
    }

    /**
     * Read the most recent N emails.
     */
    public List<EmailMessage> readRecentMessages(int count) throws MessagingException {
        if (store == null || !store.isConnected()) {
            connect();
        }

        int totalMessages = inbox.getMessageCount();
        int start = Math.max(1, totalMessages - count + 1);

        LOG.info("Reading {} most recent messages (from {} to {})", count, start, totalMessages);
        Message[] messages = inbox.getMessages(start, totalMessages);

        List<EmailMessage> result = new ArrayList<>();
        for (Message msg : messages) {
            try {
                result.add(parseMessage(msg));
            } catch (Exception ex) {
                LOG.warn("Failed to parse message: {}", ex.getMessage());
            }
        }

        return result;
    }

    /**
     * Search emails with custom criteria including from address, date range, subject, and body content.
     * 
     * @param criteria Search criteria
     * @return List of matching email messages
     * 
     * Example usage:
     * <pre>
     * SearchCriteria criteria = new SearchCriteria()
     *     .from("sender@example.com")
     *     .dateRange(LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 31))
     *     .subject("Invoice")
     *     .bodyContains("payment");
     * 
     * List<EmailMessage> results = client.searchEmails(criteria);
     * </pre>
     */
    public List<EmailMessage> searchEmails(SearchCriteria criteria) throws MessagingException {
        if (store == null || !store.isConnected()) {
            connect();
        }

        List<SearchTerm> searchTerms = new ArrayList<>();

        // Add from address search
        if (criteria.getFromAddress() != null && !criteria.getFromAddress().isEmpty()) {
            searchTerms.add(new FromStringTerm(criteria.getFromAddress()));
        }

        // Add date range search
        if (criteria.getStartDate() != null) {
            Date start = Date.from(criteria.getStartDate().atStartOfDay(ZoneId.systemDefault()).toInstant());
            searchTerms.add(new ReceivedDateTerm(ComparisonTerm.GE, start));
        }
        if (criteria.getEndDate() != null) {
            Date end = Date.from(criteria.getEndDate().plusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant());
            searchTerms.add(new ReceivedDateTerm(ComparisonTerm.LT, end));
        }

        // Add subject search
        if (criteria.getSubjectKeyword() != null && !criteria.getSubjectKeyword().isEmpty()) {
            searchTerms.add(new SubjectTerm(criteria.getSubjectKeyword()));
        }

        // Add body content search
        if (criteria.getBodyKeyword() != null && !criteria.getBodyKeyword().isEmpty()) {
            searchTerms.add(new BodyTerm(criteria.getBodyKeyword()));
        }

        // Add unread filter if specified
        if (criteria.isUnreadOnly()) {
            searchTerms.add(new FlagTerm(new Flags(Flags.Flag.SEEN), false));
        }

        // Combine all search terms
        SearchTerm finalSearchTerm = null;
        if (!searchTerms.isEmpty()) {
            if (searchTerms.size() == 1) {
                finalSearchTerm = searchTerms.get(0);
            } else {
                finalSearchTerm = new AndTerm(searchTerms.toArray(new SearchTerm[0]));
            }
        }

        LOG.info("Searching emails with criteria: {}", criteria);
        Message[] messages;
        if (finalSearchTerm != null) {
            messages = inbox.search(finalSearchTerm);
        } else {
            // If no criteria specified, return all messages
            messages = inbox.getMessages();
        }
        LOG.info("Found {} messages matching criteria", messages.length);

        List<EmailMessage> result = new ArrayList<>();
        for (Message msg : messages) {
            try {
                result.add(parseMessage(msg));
            } catch (Exception ex) {
                LOG.warn("Failed to parse message: {}", ex.getMessage());
            }
        }

        return result;
    }

    private EmailMessage parseMessage(Message msg) throws MessagingException, IOException {
        EmailMessage email = new EmailMessage();
        email.setMessageNumber(msg.getMessageNumber());
        email.setSubject(msg.getSubject());
        email.setFrom(msg.getFrom() != null && msg.getFrom().length > 0 ? msg.getFrom()[0].toString() : null);
        email.setReceivedDate(msg.getReceivedDate());
        email.setSentDate(msg.getSentDate());

        if (msg.getRecipients(Message.RecipientType.TO) != null) {
            List<String> to = new ArrayList<>();
            for (Address addr : msg.getRecipients(Message.RecipientType.TO)) {
                to.add(addr.toString());
            }
            email.setTo(to);
        }

        // Extract body content
        String content = extractTextContent(msg);
        email.setBody(content);

        // Check for attachments
        if (msg.isMimeType("multipart/*")) {
            Multipart multipart = (Multipart) msg.getContent();
            List<String> attachmentNames = new ArrayList<>();
            for (int i = 0; i < multipart.getCount(); i++) {
                BodyPart bodyPart = multipart.getBodyPart(i);
                if (Part.ATTACHMENT.equalsIgnoreCase(bodyPart.getDisposition())) {
                    attachmentNames.add(bodyPart.getFileName());
                }
            }
            email.setAttachments(attachmentNames);
        }

       // email.setRead(msg.isSet(Flags.Flag.SEEN));

        return email;
    }

    private String extractTextContent(Part part) throws MessagingException, IOException {
        if (part.isMimeType("text/plain")) {
            return part.getContent().toString();
        }

        if (part.isMimeType("text/html")) {
            // Return HTML as-is (caller can strip tags if needed)
        	return Jsoup.parse(part.getContent().toString()).text();
            //return "";
        }

        if (part.isMimeType("multipart/*")) {
            MimeMultipart multipart = (MimeMultipart) part.getContent();
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < multipart.getCount(); i++) {
                BodyPart bodyPart = multipart.getBodyPart(i);
                String partContent = extractTextContent(bodyPart);
                if (partContent != null && !partContent.isEmpty()) {
                    result.append(partContent);
                }
            }
            return result.toString();
        }

        return "";
    }

    @Override
    public void close() {
        try {
            if (inbox != null && inbox.isOpen()) {
                inbox.close(false);
                LOG.info("Closed INBOX folder");
            }
        } catch (MessagingException ex) {
            LOG.warn("Error closing inbox: {}", ex.getMessage());
        }

        try {
            if (store != null && store.isConnected()) {
                store.close();
                LOG.info("Disconnected from Gmail");
            }
        } catch (MessagingException ex) {
            LOG.warn("Error closing store: {}", ex.getMessage());
        }
    }
}
