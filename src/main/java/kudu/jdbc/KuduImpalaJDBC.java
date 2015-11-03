package kudu.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author yanbit
 * @date Nov 3, 2015 3:28:10 PM
 * @todo TODO
 */
public class KuduImpalaJDBC {

  private static Log LOG = LogFactory.getLog(KuduImpalaJDBC.class);

  public static void query(String query, Hanlder hanlder) throws IOException {
    LOG.info("\n=============================================");
    LOG.info("ELex Impala JDBC Query");
    LOG.info("Using Connection URL: " + LoadConf.connectionUrl);
    LOG.info("Running Query: " + query);

    Connection con = null;
    try {
      Class.forName(LoadConf.jdbcDriverName);
      con = DriverManager.getConnection(LoadConf.connectionUrl);
      Statement stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      LOG.info("\n== Begin Query Results ======================");
      hanlder.process(rs);
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
  }
  
  public static void main(String[] args) {
    try {
      query("select sid from user_software_name_kudu where sname RLIKE '(?i)baidu'",new SoftWareHandler());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
