package com.ph.dl.gmail;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for GmailClient.
 * 
 * This test is @Disabled by default because it requires real Gmail credentials.
 * To run: remove @Disabled, set your credentials, and run manually.
 */
//@Disabled("Requires real Gmail credentials - enable manually for integration testing")
class GmailClientTest {
	
	 String username = "pathak.holding@gmail.com";
     String password = "gntk erxt aqks qkez";

     public static void main(String[] args) throws Exception {
		 GmailClientTest test = new GmailClientTest();
		 //test.readsMessagesWithinDateRange();
		 //test.readsUnreadMessages();
		 //test.readsRecentMessages();
		 test.searchEmailsWithCriteria();
	 }
     
    @Test
    void readsMessagesWithinDateRange() throws Exception {
        // TODO: Replace with your actual Gmail credentials
       

        try (GmailClient client = new GmailClient(username, password)) {
            client.connect();

            LocalDate start = LocalDate.of(2025, 1, 1);
            LocalDate end = LocalDate.of(2025, 1, 31);

            List<EmailMessage> messages = client.readMessages(start, end);

            assertNotNull(messages);
            System.out.println("Found " + messages.size() + " messages");

            for (EmailMessage msg : messages) {
                System.out.println(msg);
            }
        }
    }

    //@Test
    void readsUnreadMessages() throws Exception {
      

        try (GmailClient client = new GmailClient(username, password)) {
            client.connect();

            LocalDate start = LocalDate.of(2025, 1, 1);
            LocalDate end = LocalDate.now();

            List<EmailMessage> messages = client.readUnreadMessages(start, end);

            assertNotNull(messages);
            System.out.println("Found " + messages.size() + " unread messages");

            for (EmailMessage msg : messages) {
                System.out.println(msg);
                assertFalse(msg.isRead(), "Should be unread");
            }
        }
    }

    @Test
    void readsRecentMessages() throws Exception {
       

        try (GmailClient client = new GmailClient(username, password)) {
            client.connect();

            List<EmailMessage> messages = client.readRecentMessages(250);

            //assertNotNull(messages);
            //assertTrue(messages.size() <= 10);
            System.out.println("Found " + messages.size() + " recent messages");

            for (EmailMessage msg : messages) {
                //System.out.println(msg);
                if(msg.getSubject().toLowerCase().contains("your american water bill is ready")
                		|| msg.getSubject().toLowerCase().contains("ppl electric utilities: your new bill is available online.")
                		|| msg.getSubject().toLowerCase().contains("your ugi utilities bill is available")){
                	System.out.println("Body: " + msg.getBody());
                }
            }
        }
    }

    @Test
    void searchEmailsWithCriteria() throws Exception {
        try (GmailClient client = new GmailClient(username, password)) {
            client.connect();

            System.out.println("\n=== Test 1: Search by from address ===");
            SearchCriteria criteria1 = new SearchCriteria()
                .from("Customer_Service@amwater.com")
                .subject("Your American Water bill is ready")
                .dateRange(LocalDate.of(2025, 1, 1), LocalDate.of(2025, 12, 31))
                ;
            
            List<EmailMessage> results1 = client.searchEmails(criteria1);
            System.out.println("Found " + results1.size() + " emails from 'Customer_Service@amwater.com'");
            for (EmailMessage msg : results1) {
                System.out.println("  - " + msg.getSubject() + " (from: " + msg.getFrom() + ")");
                System.out.println("    Mail: " + msg.getBody());
            }

            System.out.println("\n=== Test 2: Search ppl ===");
            SearchCriteria criteria2 = new SearchCriteria()
                .subject("PPL Electric Utilities: Your new bill is available online")
            	.from("CustomerService@pplweb.com")
            	.dateRange(LocalDate.of(2025, 1, 1), LocalDate.of(2025, 12, 31));
            
            List<EmailMessage> results2 = client.searchEmails(criteria2);
            System.out.println("Found " + results2.size() + " emails with 'bill' in subject");
            for (EmailMessage msg : results2) {
                System.out.println("  - " + msg.getSubject());
                System.out.println("Body: " + msg.getBody());
            }

            System.out.println("\n=== Test 3: Search ugi ===");
            SearchCriteria criteria3 = new SearchCriteria()
                .dateRange(LocalDate.of(2025, 1, 1), LocalDate.of(2025, 12, 31))
                .subject("Your UGI Utilities Bill is Due Today")
            	.from("ugi@customerservice.ugi.com");
            
            List<EmailMessage> results3 = client.searchEmails(criteria3);
            System.out.println("Found " + results3.size() + " emails from UGI");
            for (EmailMessage msg : results3) {
                System.out.println("  - " + msg.getSubject() + " (" + msg.getReceivedDate() + ")");
                System.out.println("Body: " + msg.getBody());
            }

			/*
			 * System.out.
			 * println("\n=== Test 4: Search by multiple criteria (from + subject + date range) ==="
			 * ); SearchCriteria criteria4 = new SearchCriteria() .from("noreply")
			 * .subject("bill") .dateRange(LocalDate.of(2025, 1, 1), LocalDate.now());
			 * 
			 * List<EmailMessage> results4 = client.searchEmails(criteria4);
			 * System.out.println("Found " + results4.size() +
			 * " emails matching all criteria"); for (EmailMessage msg : results4) {
			 * System.out.println("  - " + msg.getSubject());
			 * System.out.println("    From: " + msg.getFrom());
			 * System.out.println("    Date: " + msg.getReceivedDate()); }
			 * 
			 * System.out.println("\n=== Test 5: Search by body content ===");
			 * SearchCriteria criteria5 = new SearchCriteria() .bodyContains("payment")
			 * .dateRange(LocalDate.of(2025, 1, 1), LocalDate.now());
			 * 
			 * List<EmailMessage> results5 = client.searchEmails(criteria5);
			 * System.out.println("Found " + results5.size() +
			 * " emails with 'payment' in body"); for (EmailMessage msg : results5) {
			 * System.out.println("  - " + msg.getSubject()); if (msg.getBody() != null &&
			 * msg.getBody().length() > 100) { System.out.println("    Body preview: " +
			 * msg.getBody().substring(0, 100) + "..."); } }
			 * 
			 * System.out.println("\n=== Test 6: Search for unread emails only ===");
			 * SearchCriteria criteria6 = new SearchCriteria() .unreadOnly(true)
			 * .startDate(LocalDate.now().minusDays(30));
			 * 
			 * List<EmailMessage> results6 = client.searchEmails(criteria6);
			 * System.out.println("Found " + results6.size() +
			 * " unread emails in the last 30 days"); for (EmailMessage msg : results6) {
			 * System.out.println("  - " + msg.getSubject() + " [UNREAD]"); }
			 * 
			 * System.out.println("\n=== Test 7: Complex search - utility bills ===");
			 * SearchCriteria criteria7 = new SearchCriteria() .subject("bill")
			 * .dateRange(LocalDate.of(2025, 1, 1), LocalDate.now());
			 * 
			 * List<EmailMessage> results7 = client.searchEmails(criteria7);
			 * System.out.println("Found " + results7.size() + " utility bill emails"); for
			 * (EmailMessage msg : results7) { if
			 * (msg.getSubject().toLowerCase().contains("water") ||
			 * msg.getSubject().toLowerCase().contains("electric") ||
			 * msg.getSubject().toLowerCase().contains("utilities")) {
			 * System.out.println("  - " + msg.getSubject());
			 * System.out.println("    From: " + msg.getFrom());
			 * System.out.println("    Date: " + msg.getReceivedDate()); if (msg.getBody()
			 * != null) { System.out.println("    Body: " + (msg.getBody().length() > 200 ?
			 * msg.getBody().substring(0, 200) + "..." : msg.getBody())); }
			 * System.out.println(); } }
			 * 
			 * // Assertions assertNotNull(results1); assertNotNull(results2);
			 * assertNotNull(results3); assertNotNull(results4); assertNotNull(results5);
			 * assertNotNull(results6); assertNotNull(results7);
			 */            
            System.out.println("\n=== All search tests completed successfully ===");
        }
    }
}
