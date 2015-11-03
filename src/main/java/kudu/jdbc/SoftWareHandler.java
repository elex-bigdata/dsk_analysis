package kudu.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author yanbit
 * @date Nov 3, 2015 3:56:35 PM
 * @todo TODO
 */
public class SoftWareHandler implements Hanlder {

  StringBuffer sb = new StringBuffer();

  public void process(ResultSet rs) {
    try {
      while (rs.next()) {
        sb.append("(\\\\b");
        sb.append(rs.getString(1));
        sb.append("\\\\b)|");
      }
      sb.substring(0, sb.length() - 1);
      System.out.println(sb.toString());
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
