# Gmail Client Integration

## Overview
A JavaMail-based IMAP client for reading Gmail messages within a date range.

## Files Created
- `GmailClient.java` - Core email client using IMAP protocol
- `GmailReaderService.java` - Example service showing how to use the client
- `GmailClientTest.java` - Integration tests (@Disabled by default)

## Setup Instructions

### 1. Enable Gmail IMAP Access
1. Go to Gmail Settings → See all settings → Forwarding and POP/IMAP
2. Enable IMAP access

### 2. Create App Password (Required)
Gmail requires App Passwords for programmatic access:
1. Enable 2-Factor Authentication on your Google account
2. Go to https://myaccount.google.com/apppasswords
3. Generate a new App Password for "Mail"
4. Copy the 16-character password (you'll use this, NOT your regular password)

### 3. Configure application.yml
Update the email section in `application.yml`:

```yaml
email:
  gmail:
    username: "your-email@gmail.com"
    password: "your-16-char-app-password"
    enabled: true
```

## Usage Examples

### Basic Usage
```java
try (GmailClient client = new GmailClient("user@gmail.com", "app-password")) {
    client.connect();
    
    LocalDate start = LocalDate.of(2025, 1, 1);
    LocalDate end = LocalDate.of(2025, 1, 31);
    
    List<EmailMessage> messages = client.readMessages(start, end);
    
    for (EmailMessage msg : messages) {
        System.out.println("Subject: " + msg.getSubject());
        System.out.println("From: " + msg.getFrom());
        System.out.println("Date: " + msg.getReceivedDate());
        System.out.println("Body: " + msg.getBody());
        System.out.println("Attachments: " + msg.getAttachments());
    }
}
```

### Read Unread Messages Only
```java
try (GmailClient client = new GmailClient(username, password)) {
    client.connect();
    
    LocalDate start = LocalDate.now().minusDays(7);
    LocalDate end = LocalDate.now();
    
    List<EmailMessage> unread = client.readUnreadMessages(start, end);
    
    for (EmailMessage msg : unread) {
        System.out.println("Unread: " + msg.getSubject());
    }
}
```

### Read Most Recent N Messages
```java
try (GmailClient client = new GmailClient(username, password)) {
    client.connect();
    
    List<EmailMessage> recent = client.readRecentMessages(10);
    // Process last 10 emails
}
```

### Using the Service Class
```java
GmailReaderService service = new GmailReaderService();
service.readRecentEmails(); // Reads last 7 days
```

## API Reference

### GmailClient Methods

**`connect()`**
- Connects to Gmail IMAP server and opens INBOX folder
- Throws `MessagingException` if connection fails

**`readMessages(LocalDate start, LocalDate end)`**
- Reads all emails received between start and end dates (inclusive)
- Returns `List<EmailMessage>`

**`readUnreadMessages(LocalDate start, LocalDate end)`**
- Reads only unread emails within date range
- Returns `List<EmailMessage>`

**`readRecentMessages(int count)`**
- Reads the most recent N messages
- Returns `List<EmailMessage>`

**`close()`**
- Closes connection (automatically called when using try-with-resources)

### EmailMessage Fields
- `messageNumber` - Server message number
- `subject` - Email subject
- `from` - Sender email address
- `to` - List of recipients
- `receivedDate` - When email was received
- `sentDate` - When email was sent
- `body` - Email body (text or HTML)
- `attachments` - List of attachment filenames
- `read` - Boolean flag for read/unread status

## Troubleshooting

### "Authentication failed"
- Ensure you're using an App Password, not your regular password
- Verify 2FA is enabled on your Google account
- Check that IMAP is enabled in Gmail settings

### "Connection timeout"
- Check firewall settings (port 993 must be open)
- Verify internet connectivity
- Try increasing timeout in GmailClient properties

### SSL/TLS Errors
- Ensure Java is up-to-date
- Check that Gmail's SSL certificate is trusted

## Dependencies
Added to `build.gradle`:
```groovy
implementation 'com.sun.mail:javax.mail:1.6.2'
```

## Security Notes
- **Never commit credentials to version control**
- Use environment variables or secure vaults for production
- App Passwords can be revoked at any time from Google Account settings
- The client connects via SSL/TLS (port 993) for security
