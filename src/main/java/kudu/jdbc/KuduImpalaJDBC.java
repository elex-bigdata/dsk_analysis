package kudu.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kudu.jdbc.hanlder.SingleResultHanlder;

/**
 * @author yanbit
 * @date Nov 3, 2015 3:28:10 PM
 * @todo TODO
 */
public class KuduImpalaJDBC {

  private static Log LOG = LogFactory.getLog(KuduImpalaJDBC.class);

  /**
   * query return result
   * 
   * @param query
   * @param hanlder
   * @return object
   * @throws IOException
   */
  public static Object query(String query, Hanlder hanlder) throws IOException {
    LOG.info("\n=============================================");
    LOG.info("ELex Impala JDBC Query");
    LOG.info("Using Connection URL: " + LoadConf.connectionUrl);
    LOG.info("Running Query: " + query);

    Connection con = null;
    Object oline = null;
    try {
      Class.forName(LoadConf.jdbcDriverName);
      con = DriverManager.getConnection(LoadConf.connectionUrl);
      Statement stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      LOG.info("\n== Begin Query Results ======================");
      oline = hanlder.process(rs);
      LOG.info("== End Query Results =======================\n\n");
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        con.close();
      } catch (Exception e) {
      }
    }
    return oline;
  }

  /**
   * query result
   * 
   * @param query
   * @param hanlder
   * @return object
   * @throws IOException
   */
  public static void query2(String query, Hanlder hanlder) throws IOException {
    query(query, hanlder);
  }

  public static String formatPercent(double number, int newValue) {
    java.text.NumberFormat nf = java.text.NumberFormat.getPercentInstance();
    nf.setMinimumFractionDigits(newValue);
    return nf.format(number);
  }

  public static void main(String[] args) {
    try {
      Object o = query(
          "select sid from user_software_name_kudu where sname RLIKE '(?i)winzipper'",
          new SoftWareHandler());
      System.out.println(o);
      if (o != null) {
        Object allCount = query("select count(1) from user_software_kudu",
            new SingleResultHanlder());
        Object baiduCount =
            query("select count(1) from user_software_kudu where items RLIKE '"
                + o + "'", new SingleResultHanlder());
        Double all = Double.valueOf((String) allCount);
        Double baidu = Double.valueOf((String) baiduCount);
        System.out.println("=============result===============");
        String result = formatPercent(((double)baidu/(double)all),2);
        System.out.println(result);
      }
      // Object o2 = query(
      // "select \"baidu\",count(1) from user_software_kudu where items RLIKE
      // '"+o+"'",
      // new PrintHanlder());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
