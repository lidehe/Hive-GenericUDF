import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

public class Test {
    static Map<String, String> rates = new HashMap<String, String>();
    static MathContext mathContext=new MathContext(40, RoundingMode.HALF_DOWN);
    static {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File("d://cucrrency.csv")));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] items = line.split(",");
                rates.put(items[0] + "-" + items[1], items[2]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
            String []param={"cn","us","100"};
            trans(param);
    }

    static void trans(String[] deferredObjects) {

        String fromCountry = deferredObjects[0];
        String toCountry = deferredObjects[1];
        System.out.println(fromCountry+" --->  "+toCountry);
        BigDecimal value = new BigDecimal(deferredObjects[2],mathContext);
        String rateStr;
        BigDecimal result;
        if ((rateStr = rates.get(fromCountry + "-" + toCountry)) != null) {
            BigDecimal rate = new BigDecimal(rateStr,mathContext);
            result = value.multiply(rate,mathContext);
            System.out.println(result);
            System.out.println(result.toString());
        } else if ((rateStr = rates.get(toCountry + "-" + fromCountry)) != null) {
            BigDecimal rate = new BigDecimal(rateStr,mathContext);
            result = value.divide(rate,mathContext);
            System.out.println(result);
            System.out.println(result.toString());
        }
    }
}
