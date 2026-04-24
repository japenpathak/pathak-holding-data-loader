package com.ph.dl.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class IncomeLoaderTransformTest {
    @Test
    void normalizeType() throws Exception {
        IncomeLoader loader = new IncomeLoader();
        // Access private via reflection
        var m = IncomeLoader.class.getDeclaredMethod("normalizeType", String.class);
        m.setAccessible(true);
        assertEquals("Recurring Monthly", m.invoke(loader, "Income / Recurring Monthly"));
        assertEquals("One Time", m.invoke(loader, "Income / One Time"));
        assertEquals("Other", m.invoke(loader, "Other"));
    }

    @Test
    void cleanupMethodCollapsesDuplicates() throws Exception {
        IncomeLoader loader = new IncomeLoader();
        var m = IncomeLoader.class.getDeclaredMethod("cleanupMethod", String.class);
        m.setAccessible(true);
        assertEquals("ACH", m.invoke(loader, "ACH, ACH"));
        assertEquals("Other payment method", m.invoke(loader, "Other payment method, Other payment method"));
        assertEquals("ACH, CASH", m.invoke(loader, "ACH, CASH"));
    }

    @Test
    void categoryAdjustments() throws Exception {
        IncomeLoader loader = new IncomeLoader();
        var m = IncomeLoader.class.getDeclaredMethod("normalizeCategoryAndAdjustType", String.class, String.class);
        m.setAccessible(true);
        String res = (String) m.invoke(loader, "Tenant charges & fees / Water fee", "One Time");
        assertEquals("Water fee\u0000Reimbursement", res);
        res = (String) m.invoke(loader, "Deposit / Pet charge", "One Time");
        assertEquals("Pet charge\u0000Deposit", res);
        res = (String) m.invoke(loader, "Rent / January", "Recurring Monthly");
        assertEquals("January\u0000Recurring Monthly", res);
        res = (String) m.invoke(loader, "Tenant charges & fees / Pet charge", "Recurring Monthly");
        assertEquals("Pet Rent\u0000Recurring Monthly", res);
    }
}
