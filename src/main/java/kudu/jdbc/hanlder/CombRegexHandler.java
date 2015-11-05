package kudu.jdbc.hanlder;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author yanbit
 * @date Nov 3, 2015 3:56:35 PM
 * @todo TODO
 */
public class CombRegexHandler implements Hanlder {

  public String process(ResultSet rs) {
    StringBuffer sb = new StringBuffer();
    try {
      while (rs.next()) {
        sb.append("(\\\\b");
        sb.append(rs.getString(1));
        sb.append("\\\\b)|");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    if (sb.length() != 0) {
      return sb.substring(0, sb.length() - 1);
    }
    return sb.toString();
  }
}
