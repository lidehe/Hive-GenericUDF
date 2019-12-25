package com.zxftech.rrms.UDF.orgnizationConversion;

import com.zxftech.rrms.utils.impl.OrgnizationCodeMapFromFile;
import com.zxftech.rrms.utils.OrgnizationCodeMap;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lidehe
 * NOV 5,2019
 * hive sql 和 spark sql用法一样
 * add jar unix_path
 * create function func_name as 'class';
 */
public class orgnizationConversionUDF extends UDF {
    static Map<String, String> codeNameMap = new HashMap<String, String>();

    static {
        OrgnizationCodeMap orgnizationCodeMap = new OrgnizationCodeMapFromFile();
        codeNameMap = orgnizationCodeMap.getMap();
    }

    public Text evaluate(final Text s) {
        if (s == null) return null;
        return new Text(codeNameMap.get(s.toString()));

    }
}
