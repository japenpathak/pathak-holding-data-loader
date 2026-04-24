package com.ph.dl.gmail;

import java.time.LocalDate;

/**
 * Search criteria builder for flexible email searching.
 */
public class SearchCriteria {
    private String fromAddress;
    private LocalDate startDate;
    private LocalDate endDate;
    private String subjectKeyword;
    private String bodyKeyword;
    private boolean unreadOnly;

    public SearchCriteria() {
    }

    /**
     * Filter by sender email address (partial match supported).
     */
    public SearchCriteria from(String fromAddress) {
        this.fromAddress = fromAddress;
        return this;
    }

    /**
     * Filter by date range (inclusive).
     */
    public SearchCriteria dateRange(LocalDate startDate, LocalDate endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
        return this;
    }

    /**
     * Filter by start date only.
     */
    public SearchCriteria startDate(LocalDate startDate) {
        this.startDate = startDate;
        return this;
    }

    /**
     * Filter by end date only.
     */
    public SearchCriteria endDate(LocalDate endDate) {
        this.endDate = endDate;
        return this;
    }

    /**
     * Filter by subject keyword (case-insensitive partial match).
     */
    public SearchCriteria subject(String subjectKeyword) {
        this.subjectKeyword = subjectKeyword;
        return this;
    }

    /**
     * Filter by body content keyword (case-insensitive partial match).
     */
    public SearchCriteria bodyContains(String bodyKeyword) {
        this.bodyKeyword = bodyKeyword;
        return this;
    }

    /**
     * Filter to only return unread emails.
     */
    public SearchCriteria unreadOnly(boolean unreadOnly) {
        this.unreadOnly = unreadOnly;
        return this;
    }

    public String getFromAddress() { return fromAddress; }
    public LocalDate getStartDate() { return startDate; }
    public LocalDate getEndDate() { return endDate; }
    public String getSubjectKeyword() { return subjectKeyword; }
    public String getBodyKeyword() { return bodyKeyword; }
    public boolean isUnreadOnly() { return unreadOnly; }

    @Override
    public String toString() {
        return "SearchCriteria{" +
                "from='" + fromAddress + '\'' +
                ", startDate=" + startDate +
                ", endDate=" + endDate +
                ", subject='" + subjectKeyword + '\'' +
                ", body='" + bodyKeyword + '\'' +
                ", unreadOnly=" + unreadOnly +
                '}';
    }
}
