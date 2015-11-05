package kudu.jdbc;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kudu.jdbc.hanlder.CombRegexHandler;
import kudu.jdbc.hanlder.CountHanlder;

/**
 * @author yanbit
 * @date Nov 5, 2015 10:29:28 AM
 * @todo TODO
 */
public class OverlapRateQuery {
  private static Log LOG = LogFactory.getLog(OverlapRateQuery.class);
  private static KuduImpalaJDBC jdbc = new KuduImpalaJDBC();
  private double ALL_COUNT = 0;

  /**
   * count all record
   * 
   * @return
   */
  public Double getAllCount() {
    ALL_COUNT = (Double) jdbc.query("select count(1) from user_software_kudu",
        new CountHanlder());
    return ALL_COUNT;
  }

  /**
   * count softeware record
   * 
   * @param softeware
   * @return
   */
  public String getSoftwareOverlap(String softeware) {
    if (ALL_COUNT == 0) {
      getAllCount();
    }
    if (!StringUtils.isNotEmpty(softeware)) {
      LOG.error("=======param software: is null or \"\" ,please check param!");
      return null;
    }
    String regex = (String) jdbc
        .query("select sid from user_software_name_kudu where sname RLIKE '(?i)"
            + softeware + "'", new CombRegexHandler());
    System.out.println(regex);

    String result = null;
    if (StringUtils.isNotEmpty(regex)) {
      double baiduCount = (Double) jdbc
          .query("select count(1) from user_software_kudu where items RLIKE '"
              + regex + "'", new CountHanlder());
      result =
          Utils.formatPercent(((double) baiduCount / (double) ALL_COUNT), 2);
    }

    return result;
  }

  public static void main(String[] args) {
    OverlapRateQuery query = new OverlapRateQuery();
    String softs =
        "AVG,(Symantec)|(Norton),winzipper,(Piriform)|(Ccleaner),picexa,McAfee,Avast,"
            + "Baidu,(ESET)|(NOD32,Avira,Kaspersky,Malwarebytes,Iobit,Trend Micro,Bitdefender,"
            + "ZoneAlarm,Panda Security,(Lavasoft)|(Ad-Aware),360 Total Security,SmadAv,Quick Heal,"
            + "SpyHunter,(King Soft)|(Kingsoft),Sophos,Kaspersky Lab,(drweb)|(dr.web),Emsisoft,Glarysoft,"
            + "(Xplode)|(AdwCleaner),Webroot Software,Fortinet,Auslogics Software,Doctor Web,SurfRight,AnviSoft";
    String[] softts = softs.split(",");
    StringBuffer sb = new StringBuffer();
    long start = System.currentTimeMillis();
    for (int i = 0; i < softts.length - 1; i++) {
      System.out
          .println("++++++++++++++++++" + softts[i] + "+++++++++++++++++++");
      String rate = softts[i] + ":" + query.getSoftwareOverlap(softts[i]);
      sb.append(rate).append("\n");
    }
    long end = System.currentTimeMillis();

    System.out.println(sb);
    System.out.println((end - start) / 1000);
  }
}
