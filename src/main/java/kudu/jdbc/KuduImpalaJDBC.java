package kudu.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kudu.jdbc.hanlder.CombRegexHandler;
import kudu.jdbc.hanlder.CountHanlder;
import kudu.jdbc.hanlder.Hanlder;

/**
 * @author yanbit
 * @date Nov 3, 2015 3:28:10 PM
 * @todo TODO
 */
public class KuduImpalaJDBC implements Query {

  private static Log LOG = LogFactory.getLog(KuduImpalaJDBC.class);

  /**
   * query return result
   * 
   * @param query
   * @param hanlder
   * @return object
   * @throws IOException
   */
  public Object query(String query, Hanlder hanlder) {
    LOG.info("\n=============================================");
    LOG.info("ELex Impala JDBC Query");
    LOG.info("Using Connection URL: " + Utils.connectionUrl);
    LOG.info("Running Query: " + query);

    Connection con = null;
    Object oline = null;
    try {
      Class.forName(Utils.jdbcDriverName);
      con = DriverManager.getConnection(Utils.connectionUrl);
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
}
