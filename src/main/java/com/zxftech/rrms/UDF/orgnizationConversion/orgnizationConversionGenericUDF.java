package com.zxftech.rrms.UDF.orgnizationConversion;


import com.zxftech.rrms.utils.OrgnizationCodeMap;
import com.zxftech.rrms.utils.impl.OrgnizationCodeMapFromFile;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

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
public class orgnizationConversionGenericUDF extends GenericUDF {
    static Map<String, String> codeNameMap = new HashMap<String, String>();

    static {
        OrgnizationCodeMap orgnizationCodeMap=new OrgnizationCodeMapFromFile();
        codeNameMap = orgnizationCodeMap.getMap();
    }

    /**
     * 参数类型检查，返回类型定义
     * 从表结构中获取字段类型，检查的工作需要我们自己完成
     * 正因如此，必须有from table，否则拿不到字段类型就会报空指针异常
     *
     * @param objectInspectors
     * @return
     * @throws UDFArgumentTypeException
     */
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentTypeException {

        if (objectInspectors.length != 1) {
            throw new UDFArgumentTypeException(1, "1 argument was expected.");
        }

        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory0 = ((PrimitiveObjectInspector) objectInspectors[0]).getPrimitiveCategory();
        if (primitiveCategory0 != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(2, "A string argument was expected.");
        }

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) {
        try {
            String orgCode = deferredObjects[0].get().toString();
            return codeNameMap.get(orgCode);
        } catch (HiveException e) {
            e.printStackTrace();
            throw new RuntimeException("com.zfxtech.rrms.orgnizationConversionGenericUDF evaluate error " + e);
        }
    }

    public String getDisplayString(String[] strings) {
        return "something error happened !";
    }
}
