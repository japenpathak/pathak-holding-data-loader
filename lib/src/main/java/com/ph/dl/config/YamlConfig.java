package com.ph.dl.config;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.LoaderOptions;

import java.io.InputStream;
import java.util.Map;

public class YamlConfig {
    private final Map<String, Object> root;

    public YamlConfig() {
        Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("application.yml");
        if (is == null) {
            throw new RuntimeException("application.yml not found on classpath");
        }
        root = yaml.load(is);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getIncomeConfig() {
        return (Map<String, Object>) root.get("income");
    }

    public String getIncomeCsvPath() {
        Map<String, Object> income = getIncomeConfig();
        Map<String, Object> invoice = (Map<String, Object>) income.get("invoice");
        return (String) invoice.get("input");
    }

    public boolean isForceOverwrite() {
        Map<String, Object> income = getIncomeConfig();
        Map<String, Object> force = (Map<String, Object>) income.get("force");
        if (force == null) return false;
        Object val = force.get("overwrite");
        return val instanceof Boolean ? (Boolean) val : Boolean.parseBoolean(String.valueOf(val));
    }

    public int getYear() {
        Map<String, Object> income = getIncomeConfig();
        Object year = income.get("year");
        if (year == null) throw new RuntimeException("income.year not specified in application.yml");
        return Integer.parseInt(String.valueOf(year));
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getIncomeColumnMapping() {
        Map<String, Object> income = getIncomeConfig();
        Map<String, Object> datamapping = (Map<String, Object>) income.get("datamapping");
        if (datamapping == null) throw new RuntimeException("income.datamapping not specified");
        Map<String, Object> columns = (Map<String, Object>) datamapping.get("columns");
        if (columns == null) throw new RuntimeException("income.datamapping.columns not specified");
        // Convert values to String
        java.util.HashMap<String, String> result = new java.util.HashMap<>();
        for (Map.Entry<String, Object> e : columns.entrySet()) {
            result.put(e.getKey(), String.valueOf(e.getValue()));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getBankConfig() {
        return (Map<String, Object>) root.get("bank");
    }

    public String getBankTransactionsCsvPath() {
        Map<String, Object> bank = getBankConfig();
        if (bank == null) throw new RuntimeException("bank not specified in application.yml");
        Map<String, Object> tx = (Map<String, Object>) bank.get("transactions");
        if (tx == null) throw new RuntimeException("bank.transactions not specified in application.yml");
        return (String) tx.get("input");
    }

    public int getBankTransactionsYear() {
        Map<String, Object> bank = getBankConfig();
        if (bank == null) throw new RuntimeException("bank not specified in application.yml");
        Map<String, Object> tx = (Map<String, Object>) bank.get("transactions");
        if (tx == null) throw new RuntimeException("bank.transactions not specified in application.yml");
        Object year = tx.get("year");
        if (year == null) throw new RuntimeException("bank.transactions.year not specified in application.yml");
        return Integer.parseInt(String.valueOf(year));
    }
}