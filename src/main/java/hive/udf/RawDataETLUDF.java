package hive.udf;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author yanbit
 * @date Oct 14, 2015 1:56:48 PM
 * @todo proccess data demo
 * 
 *       ########type1######## // String line = //
 *       "cccccd9ce28327708b3526091d315b40,\"[1410610181,\"\"Poland\"\",\"\"4.9.54\"\",\"\"1\"\",0]\",\"[17897.0,8.0,98.0,1203.0,2203.0,9680.0,88.0,19.0,104.0,18.0,21.0,"
 *       // + //
 *       "492.0,288.0,1047.0,346.0,167.0,451.0,26.0,283.0,613.0,449.0,744.0,1442.0,1487.0,823.0,3072.0,191.0,1174.0,661.0,1792.0,245.0,684.0,891.0,1.0,174"
 *       // + //
 *       "3.0,325.0,100.0,1563.0,143.0,548.0,201.0,1.0,244.0,306.0,18728.0,79.0,326.0,7222.0,935.0,254.0,1.0,206.0,33033.0,1.0,2230.0,613.0,4.0,1277.0,172"
 *       // + //
 *       ".0,1.0,1.0,1.0,315.0,3803.0,206.0,777.0,183.0,88.0,4719.0,298.0,26267.0,501.0,581.0,4999.0,306.0,4508.0,201.0,492.0,1.0,4792.0,1014.0,971.0,1.0,"
 *       // + //
 *       "89.0,33.0,85.0,445.0,1.0,1212.0,4221.0,1.0,1.0,1.0,1001.0,1.0,1.0,123.0,1236.0,484.0,3111.0,9051.0,1643.0]\"";
 * 
 *       ########type2######## // String line = //
 *       "99999a47769a1ecd3c71e73292cb63bb,\"{\"\"0\"\":1440430148,\"\"1\"\":\"\"Colombia\"\",\"\"2\"\":\"\"6.3.99\"\",\"\"3\"\":\"\"2\"\",\"\"4\"\":0}\","
 *       // + "\"[2.059944e+06,221996.0,3.03398e+06," // + //
 *       "4.0,265.0,17.0,4797.0,4.0,34.0,28.0,27.0,32.0,31.0,29.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,25.0,38.0,24.0,17.0,89.0,6730.0,670.0,2023.0,47.0,84.0,55.0,579.0]\"";
 * 
 * 
 */
@Description(name = "raw_etl", value = "_FUNC_(string) - Returns format data.", extended = "Example:\n"
    + "  > SELECT _FUNC_(1,2,3) FROM src LIMIT 1;")
public class RawDataETLUDF extends UDF {

  Text line = new Text();

  // UDF main
  public Text evaluate(Text s) {
    if (s == null) {
      return null;
    }
    line.clear();
    String formatline = proccessRawData(s.toString());
    line.set(formatline);
    return line;
  }

  // proccess data
  public String proccessRawData(String line) {
    StringBuffer sb = null;
    try {
      sb = new StringBuffer();
      if (line.contains(":")) {

        String[] firstlast = line.replace("\"[", "#").split("#");
        String first = firstlast[0].replace("\"", "").replace("}", "");
        String[] uidattrs = first.split(",\\{");
        sb.append(uidattrs[0]).append(",");

        Pattern p = Pattern.compile("(?<=:)([^,]+)(?=,)");
        Matcher m = p.matcher(uidattrs[1]);
        boolean firstFind = true;
        while (m.find()) {
          if (firstFind) {
            firstFind = false;
            sb.append(
                new BigDecimal(m.group()).stripTrailingZeros().toPlainString())
                .append(",");
          } else {
            sb.append(m.group()).append(",");
          }

        }
        sb.deleteCharAt(sb.length() - 1).append(",");
        String[] last = firstlast[1].replace("]\"", "").split(",");
        getSoftId(sb, last, "#");
      } else {
        String[] firstlast =
            line.replace("\"", "").replace("]", "").split(",\\[");
        sb.append(firstlast[0]).append(",");
        String[] atrrs = firstlast[1].split(",");
        for (int i = 0; i < atrrs.length; i++) {
          if (i != 0) {
            sb.append(atrrs[i]).append(",");
          } else {
            sb.append(
                new BigDecimal(atrrs[i]).stripTrailingZeros().toPlainString())
                .append(",");
          }

        }
        sb.deleteCharAt(sb.length() - 1).append(",");
        getSoftId(sb, firstlast[2].split(","), "#");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return sb.toString();
  }

  // process soft uid
  private static StringBuffer getSoftId(StringBuffer sb, String[] last,
      String flat) {
    for (int i = 0; i < last.length; i++) {
      if (last[i] == null || "".equals(last[i])) {
        // return sb;
      }
      try {
        sb.append(new BigDecimal(last[i]).stripTrailingZeros().toPlainString())
            .append(flat);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb;
  }

  // test
  public static void main(String[] args) {
    String line =
        "cccccd9ce28327708b3526091d315b40,\"[1410610181,\"\"Poland\"\",\"\"4.9.54\"\",\"\"1\"\",0]\",\"[17897.0,8.0,98.0,1203.0,2203.0,9680.0,88.0,19.0,104.0,18.0,21.0,"
            + "492.0,288.0,1047.0,346.0,167.0,451.0,26.0,283.0,613.0,449.0,744.0,1442.0,1487.0,823.0,3072.0,191.0,1174.0,661.0,1792.0,245.0,684.0,891.0,1.0,174"
            + "3.0,325.0,100.0,1563.0,143.0,548.0,201.0,1.0,244.0,306.0,18728.0,79.0,326.0,7222.0,935.0,254.0,1.0,206.0,33033.0,1.0,2230.0,613.0,4.0,1277.0,172"
            + ".0,1.0,1.0,1.0,315.0,3803.0,206.0,777.0,183.0,88.0,4719.0,298.0,26267.0,501.0,581.0,4999.0,306.0,4508.0,201.0,492.0,1.0,4792.0,1014.0,971.0,1.0,"
            + "89.0,33.0,85.0,445.0,1.0,1212.0,4221.0,1.0,1.0,1.0,1001.0,1.0,1.0,123.0,1236.0,484.0,3111.0,9051.0,1643.0]\"";
    RawDataETLUDF raw = new RawDataETLUDF();
    Text check = raw.evaluate(new Text(line));
    System.out.println(check.toString());
  }
}
