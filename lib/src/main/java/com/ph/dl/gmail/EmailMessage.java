package com.ph.dl.gmail;

import java.util.Date;
import java.util.List;

/**
 * POJO representing an email message.
 */
public class EmailMessage {
    private int messageNumber;
    private String subject;
    private String from;
    private List<String> to;
    private Date receivedDate;
    private Date sentDate;
    private String body;
    private List<String> attachments;
    private boolean read;

    public int getMessageNumber() { return messageNumber; }
    public void setMessageNumber(int messageNumber) { this.messageNumber = messageNumber; }

    public String getSubject() { return subject; }
    public void setSubject(String subject) { this.subject = subject; }

    public String getFrom() { return from; }
    public void setFrom(String from) { this.from = from; }

    public List<String> getTo() { return to; }
    public void setTo(List<String> to) { this.to = to; }

    public Date getReceivedDate() { return receivedDate; }
    public void setReceivedDate(Date receivedDate) { this.receivedDate = receivedDate; }

    public Date getSentDate() { return sentDate; }
    public void setSentDate(Date sentDate) { this.sentDate = sentDate; }

    public String getBody() { return body; }
    public void setBody(String body) { this.body = body; }

    public List<String> getAttachments() { return attachments; }
    public void setAttachments(List<String> attachments) { this.attachments = attachments; }

    public boolean isRead() { return read; }
    public void setRead(boolean read) { this.read = read; }

    @Override
    public String toString() {
        return "EmailMessage{" +
                "messageNumber=" + messageNumber +
                ", subject='" + subject + '\'' +
                ", from='" + from + '\'' +
                ", receivedDate=" + receivedDate +
                ", attachments=" + (attachments != null ? attachments.size() : 0) +
                ", read=" + read +
                '}';
    }
}
