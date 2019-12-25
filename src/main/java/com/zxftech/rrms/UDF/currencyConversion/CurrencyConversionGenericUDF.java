package com.zxftech.rrms.UDF.currencyConversion;


import com.zxftech.rrms.utils.CurrencyRates;
import com.zxftech.rrms.utils.impl.CurrencyRatesFromFile;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

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
 * create function func_name as 'class' using jar 'hdfs://master:9000/xxx/xxx.jar';
 * 用于复杂数据类型的处理
 */
public class CurrencyConversionGenericUDF extends GenericUDF {
    static Map<String, String> rates = new HashMap<String, String>();
    static MathContext mathContext = new MathContext(40, RoundingMode.HALF_DOWN);
    private ObjectInspectorConverters.Converter converter;

    static {
        CurrencyRates currencyRates=new CurrencyRatesFromFile();
        rates=currencyRates.getRates();
    }

    /**
     * 参数类型检查，返回类型定义
     * 从表结构中获取字段类型，检查的工作需要我们自己完成
     * 正因如此，必须有from table，否则拿不到字段类型就会报空指针异常
     * @param objectInspectors
     * @return
     * @throws UDFArgumentTypeException
     */
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentTypeException {

        if (objectInspectors.length != 3) {
            throw new UDFArgumentTypeException(1, "3 argument was expected.");
        }

        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory0 = ((PrimitiveObjectInspector) objectInspectors[0]).getPrimitiveCategory();
        if (primitiveCategory0 != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(2, "A string argument was expected.");
        }

        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory1 = ((PrimitiveObjectInspector) objectInspectors[1]).getPrimitiveCategory();
        if (primitiveCategory1 != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(3, "A string argument was expected.");
        }

        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory2 = ((PrimitiveObjectInspector) objectInspectors[2]).getPrimitiveCategory();
        if (primitiveCategory2 != PrimitiveObjectInspector.PrimitiveCategory.DECIMAL) {
            throw new UDFArgumentTypeException(4, "A decimal argument was expected.");
        }
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) {
        try {
            String fromCountry= deferredObjects[0].get().toString();
            String toCountry= deferredObjects[1].get().toString();
            BigDecimal value = new BigDecimal(deferredObjects[2].get().toString(), mathContext);
            String rateStr;
            if ((rateStr = rates.get(fromCountry + "-" + toCountry)) != null) {
                BigDecimal rate = new BigDecimal(rateStr, mathContext);
                return value.multiply(rate, mathContext).toString();
            } else if ((rateStr = rates.get(toCountry + "-" + fromCountry)) != null) {
                BigDecimal rate = new BigDecimal(rateStr, mathContext);
                return value.divide(rate, mathContext).toString();
            } else {
                return null;
            }

        } catch (HiveException e) {
            e.printStackTrace();
            throw new RuntimeException("com.zfxtech.rrms "+e);
        }
    }

    public String getDisplayString(String[] strings) {
        return "something error happened !";

    }
}
