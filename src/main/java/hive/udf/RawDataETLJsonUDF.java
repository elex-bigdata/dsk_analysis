package hive.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

/**
 * @author yanbit
 * @date Nov 2, 2015 3:36:48 PM
 * @todo format json data
 * 
 *       // data1
 *       {"_id":"19999ad75f4e14d2e0d30c4e8eec5263","attr":[1.409664977e+09,
 *       "Spain","4.4.51","2",0],"items":[973.0,65.0,66.0,745.0,282.0,383.0,9326
 *       .0,10286.0,270.0,215.0,6220.0,154.0,20347.0,78.0,524.0,43.0,3401.0,1784
 *       .0,1572.0,1903.0,966.0,897.0
 *       ,853.0,14929.0,10478.0,7088.0,14368.0,1841.0,111.0,3066.0,72.0,4.0,
 *       12325.0]}
 * 
 *       // data2
 *       {"_id":"333333ce5939e4851cfb8bbb527f4c21","attr":{"0":1433427443,"1":
 *       "Brazil","2":"6.3.81","3":"2","4":0},"items":[8.0,8.0,2.884139e+06,2848
 *       .0,1231.0,20.0,566.0,221996.0,13807.0,2.97646e+06,486.0,23436.0,2005.0,
 *       226.0,3.388554e+06,603.0,
 *       2.976461e+06,6.0,54.0,57.0,347.0,1039.0,329.0,23.0,208.0,73.0,63.0,48.0
 *       ,23.0,40.0,733.0,339.0,51.0,211.0,855.0,1979.0,90.0,2525.0,6.0,337.0,13
 *       .0,374.0,617.0,640809.0,854.0,251.0,2966.0,17.0,502.0,14.0,255.0,79.0,
 *       62.0,17306.0,363.0,340.0
 *       ,162.0,171.0,1.0,292.0,1.0,61.0,1102.0,22.0,17860.0,152.0,1.281246e+06,
 *       1238.0,161.0,212.0,212.0,12.0,1186.0,345.0,1.0,791.0,1.0,369.0,369.0,11
 *       .0,11.0,2875.0,12.0,8825.0,8831.0,5.0,17.0,9.0,5.0,2972.0,1.281246e+06,
 *       9.0,5.0,375.0,7.0,672.0
 *       ,60.0,166.0,45.0,559.0,14.0,10.0,59.0,401.0,1388.0,7.0,3094.0,250.0,30.
 *       0,1.0,89.0,22.0,402.0,1.0,7051.0,91840.0,1873.0,521909.0,3374.0,228.0,
 *       177.0,188.0,500.0,159.0,143.0,8141.0,4.0,232.0,85.0,3.077326e+06,174.0,
 *       1049.0,74.0,1.0,2077.0,1
 *       .0,1049.0,1.0,1.281246e+06,1.0,1327.0,35.0,486.0,212.0,1.281246e+06,143
 *       .0,41.0,143.0,15.0,1.0,1776.0,1880.0,16.0,163.0,94.0,2407.0,146.0,145.0
 *       ,579.0]}
 */
@Description(name = "raw_json_etl", value = "_FUNC_(string) - Returns format data.", extended = "Example:\n"
    + "  > SELECT _FUNC_(1,2,3) FROM src LIMIT 1;")
public class RawDataETLJsonUDF extends UDF {

  private static Log LOG = LogFactory.getLog(RawDataETLJsonUDF.class);

  public Text evaluate(Text s) {
    Text nline = null;
    try {
      nline = new Text();
      String line = s.toString().replace(",{\"$undefined\":true}", "")
          .replace("{\"$undefined\":true},", "")
          .replace("{\"$undefined\":true}", "");
      StringBuffer sb = new StringBuffer();
      JsonParser parser = new JsonParser();
      JsonElement jparse = parser.parse(line);
      JsonObject jobject = jparse.getAsJsonObject();
      sb.append(jobject.get("_id").getAsString()).append(",");
      if (line.contains("\"attr\":[")) {
        try {
          JsonElement attr = jobject.get("attr");
          if (attr.isJsonArray()) {
            JsonArray jsonArray = attr.getAsJsonArray();
            for (int i = 0; i < jsonArray.size(); i++) {
              if (i == 0) {
                sb.append(jsonArray.get(i).getAsBigDecimal()
                    .stripTrailingZeros().toPlainString()).append(",");
              } else {
                JsonElement json = jsonArray.get(i);
                if (json.isJsonNull()) {
                  sb.append(",");
                } else {
                  sb.append(jsonArray.get(i).getAsString()).append(",");
                }

              }
            }
          }
          JsonElement items = jobject.get("items");
          if (items.isJsonArray()) {
            JsonArray jsonArray = items.getAsJsonArray();
            if (jsonArray.size() != 0) {
              for (int i = 0; i < jsonArray.size(); i++) {
                sb.append(new Float(jsonArray.get(i).getAsFloat()).intValue())
                    .append("#");
              }
            } else {
              sb.append(",");
            }

          }
        } catch (JsonSyntaxException e) {
          LOG.error("process attr:[]" + s);
          e.printStackTrace();
        }

      } else {
        try {
          for (int i = 0; i < 5; i++) {
            sb.append(jobject.get("attr").getAsJsonObject()
                .get(String.valueOf(i)).toString().replace("\"", ""))
                .append(",");
          }
          JsonElement items = jobject.get("items");
          if (items.isJsonArray()) {
            JsonArray jsonArray = items.getAsJsonArray();
            if (jsonArray.size() != 0) {
              for (int i = 0; i < jsonArray.size(); i++) {
                sb.append(jsonArray.get(i).getAsBigDecimal()
                    .stripTrailingZeros().toPlainString()).append("#");
              }
            } else {
              sb.append(",");
            }
          }
        } catch (Exception e) {
          LOG.error("process attr:{}" + s);
          e.printStackTrace();
        }
      }
      nline.clear();
      nline.set(sb.substring(0, sb.length() - 1).toString());
    } catch (JsonSyntaxException e) {
      e.printStackTrace();
    }
    return nline;
  }

  public static void main(String[] args) {
    RawDataETLJsonUDF j = new RawDataETLJsonUDF();
    Text result1 = j.evaluate(new Text(
        "{\"_id\":\"19999ad75f4e14d2e0d30c4e8eec5263\",\"attr\":[1.409664977e+09,null,null,null,0],\"items\":[{\"$undefined\":true}]}"));
    Text result2 = j.evaluate(new Text(
        "{\"_id\":\"cccccd3dc26f029459b78c8885b794b6\",\"attr\":{\"0\":1441555507,\"1\":\"Russia\",\"2\":\"6.3.81\",\"3\":\"2\",\"4\":0},\"items\":[281246e+06,1.747288e+06,8.0,8.0,418.0,457.0,249.0,1994.0,1165.0,127351.0,5308.0,3.040267e+06,3.040267e+06,3.040267e+06,3.040267e+06,3.040267e+06,3.040267e+06,243.0,5089.0,1.0,21351.0,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,1.747288e+06,179362.0,20.0,1.710202e+06,10450.0,1.697998e+06,330.0,487.0,221996.0,1.110922e+06,3.212131e+06,1.834961e+06,15592.0,245.0,243.0,2684.0,4.490459e+06,4.587957e+06,1774.0,20798.0,549.0,38148.0,2.235279e+06,54.0,1491.0,1491.0,1036.0,1.0,1.0,1.0,1112.0,1.0,1.0,44.0,1.0,1.0,1.0,39.0,1.0,95277.0,1342.0,5886.0,17.0,40.0,1.0,162.0,1.0,1.0,2599.0,1.0,1.0,172.0,5886.0,319.0,28.0,27.0,31.0,29.0,3.0,3.0,3.0,3.0,25.0,24.0,1.0,104.0,1.0,1.0,1.0,1.0,3.239752e+06,1.0,1.0,8791.0,1.0,1.0,1.0,1.0,362.0,1.0,344111.0,146395.0,252.0,118.0,1913.0,1972.0,71.0,1036.0,4724.0,44.0,1.0,1.0,55198.0,1.0,1.0,1.0,1.281246e+06,1.0,104.0,84.0,55.0,1.281246e+06,7867.0,8034.0,9315.0,164.0,24795.0,63130.0]}"));
    System.out.println(result1.toString());
    System.out.println(result2.toString());
  }
}
