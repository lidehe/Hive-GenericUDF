package com.zxftech.rrms.utils.impl;

import com.zxftech.rrms.utils.CurrencyRates;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CurrencyRatesFromFile implements CurrencyRates {

    public Map<String, String> getRates() {
        Map<String, String> rates = new HashMap<String, String>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File("cucrrency.csv")));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] items = line.split(",");
                rates.put(items[0] + "-" + items[1], items[2]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rates;
    }
}
