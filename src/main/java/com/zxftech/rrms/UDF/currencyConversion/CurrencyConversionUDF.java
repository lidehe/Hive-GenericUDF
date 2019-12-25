package com.zxftech.rrms.UDF.currencyConversion;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lidehe
 * NOV 5,2019
 * hive sql 和 spark sql用法一样
 * add jar unix_path
 * create function func_name as 'class';
 */
public class CurrencyConversionUDF extends UDF {
    static Map<String, String> rates = new HashMap<String, String>();
    static MathContext mathContext = new MathContext(40, RoundingMode.HALF_DOWN);

    static {
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
    }
    public Text evaluate(final Text s) {
        if (s == null) { return null; }
        String []params=s.toString().split(",");

        String fromCountry = (String) (params[0]);
        String toCountry = (String) (params[1]);
        BigDecimal value = new BigDecimal((String) (params[2]), mathContext);
        String rateStr;
        if ((rateStr = rates.get(fromCountry + "-" + toCountry)) != null) {
            BigDecimal rate = new BigDecimal(rateStr, mathContext);
            return new Text(value.multiply(rate, mathContext).toString());
        } else if ((rateStr = rates.get(toCountry + "-" + fromCountry)) != null) {
            BigDecimal rate = new BigDecimal(rateStr, mathContext);
            return new Text(value.divide(rate, mathContext).toString());
        } else {
            return null;
        }
    }
}
