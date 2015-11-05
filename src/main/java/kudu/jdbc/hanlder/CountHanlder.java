package kudu.jdbc.hanlder;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author yanbit
 * @date Nov 3, 2015 5:37:07 PM
 * @todo TODO
 */
public class CountHanlder implements Hanlder {
  public Double process(ResultSet rs) {
    Double count =null;
    try {
      while (rs.next()) {
        count = Double.valueOf(rs.getString(1));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return count;
  }
}
